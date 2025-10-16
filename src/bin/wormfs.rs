//! WormFS CLI - Mount and manage WormFS filesystems.
//!
//! This binary provides a command-line interface for mounting WormFS
//! filesystems via FUSE.

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "wormfs")]
#[command(about = "WormFS - Distributed erasure-coded filesystem", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Enable debug logging
    #[arg(short, long, global = true)]
    debug: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Mount a WormFS filesystem
    Mount {
        /// Path to configuration file
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Mount point directory
        #[arg(short, long)]
        mount_point: PathBuf,

        /// Run in foreground (don't daemonize)
        #[arg(short, long)]
        foreground: bool,

        /// Allow root to access filesystem
        #[arg(long)]
        allow_root: bool,

        /// Allow other users to access filesystem
        #[arg(long)]
        allow_other: bool,

        /// Enable FUSE debug logging
        #[arg(long)]
        fuse_debug: bool,

        /// Metadata database path (overrides config)
        #[arg(long)]
        metadata_db: Option<PathBuf>,

        /// Chunk storage directory (overrides config)
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },

    /// Unmount a WormFS filesystem
    Unmount {
        /// Mount point directory
        mount_point: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Setup logging
    setup_logging(cli.verbose, cli.debug)?;

    match cli.command {
        Commands::Mount {
            config,
            mount_point,
            foreground,
            allow_root,
            allow_other,
            fuse_debug,
            metadata_db,
            data_dir,
        } => {
            mount_command(
                config,
                mount_point,
                foreground,
                allow_root,
                allow_other,
                fuse_debug,
                metadata_db,
                data_dir,
            )
            .await?;
        }
        Commands::Unmount { mount_point } => {
            unmount_command(mount_point)?;
        }
    }

    Ok(())
}

/// Setup logging based on verbosity flags.
fn setup_logging(verbose: bool, debug: bool) -> Result<(), Box<dyn std::error::Error>> {
    let log_level = if debug {
        tracing::Level::DEBUG
    } else if verbose {
        tracing::Level::INFO
    } else {
        tracing::Level::WARN
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false)
                .with_level(true),
        )
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            log_level,
        ))
        .init();

    Ok(())
}

/// Handle mount command.
#[cfg(feature = "fuser")]
async fn mount_command(
    config_path: Option<PathBuf>,
    mount_point: PathBuf,
    foreground: bool,
    allow_root: bool,
    allow_other: bool,
    fuse_debug: bool,
    metadata_db_override: Option<PathBuf>,
    data_dir_override: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    use wormfs::filesystem_service::mount::{MountConfig, MountOptions};

    tracing::info!("Starting WormFS mount...");

    // Load or create default configuration
    let mount_config = if let Some(config_path) = config_path {
        load_config_from_file(&config_path)?
    } else {
        create_default_config(mount_point.clone(), metadata_db_override, data_dir_override)?
    };

    // Override mount options from CLI
    let mount_config = MountConfig {
        mount_options: MountOptions {
            allow_root,
            allow_other,
            foreground,
            debug: fuse_debug,
            ..mount_config.mount_options
        },
        ..mount_config
    };

    // Setup signal handling for graceful shutdown
    let mount_point_for_signal = mount_config.mount_point.clone();
    tokio::spawn(async move {
        setup_signal_handler(mount_point_for_signal).await;
    });

    // Mount filesystem (blocks until unmount)
    tracing::info!("Mounting at {:?}", mount_config.mount_point);
    wormfs::filesystem_service::mount::mount_filesystem(mount_config).await?;

    tracing::info!("Filesystem unmounted");
    Ok(())
}

/// Stub for when fuser feature is disabled.
#[cfg(not(feature = "fuser"))]
async fn mount_command(
    _config_path: Option<PathBuf>,
    _mount_point: PathBuf,
    _foreground: bool,
    _allow_root: bool,
    _allow_other: bool,
    _fuse_debug: bool,
    _metadata_db_override: Option<PathBuf>,
    _data_dir_override: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Error: FUSE support not compiled. Rebuild with --features fuser");
    std::process::exit(1);
}

/// Handle unmount command.
#[cfg(feature = "fuser")]
fn unmount_command(mount_point: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Unmounting {:?}", mount_point);
    wormfs::filesystem_service::mount::unmount_filesystem(&mount_point)?;
    tracing::info!("Successfully unmounted");
    Ok(())
}

/// Stub for when fuser feature is disabled.
#[cfg(not(feature = "fuser"))]
fn unmount_command(_mount_point: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Error: FUSE support not compiled. Rebuild with --features fuser");
    std::process::exit(1);
}

/// Load configuration from TOML file.
#[cfg(feature = "fuser")]
fn load_config_from_file(
    _path: &PathBuf,
) -> Result<wormfs::filesystem_service::mount::MountConfig, Box<dyn std::error::Error>> {
    // TODO: Implement TOML parsing in Phase 1.1
    Err("Config file loading not yet implemented. Use CLI flags for now.".into())
}

/// Create default configuration.
#[cfg(feature = "fuser")]
fn create_default_config(
    mount_point: PathBuf,
    metadata_db: Option<PathBuf>,
    data_dir: Option<PathBuf>,
) -> Result<wormfs::filesystem_service::mount::MountConfig, Box<dyn std::error::Error>> {
    use wormfs::filesystem_service::mount::{MountConfig, MountOptions};
    use wormfs::filesystem_service::types::Config as FsConfig;

    // Determine paths
    let metadata_db_path = metadata_db.unwrap_or_else(|| {
        let mut path = mount_point.clone();
        path.push(".wormfs");
        path.push("metadata.db");
        path
    });

    let data_dir_path = data_dir.unwrap_or_else(|| {
        let mut path = mount_point.clone();
        path.push(".wormfs");
        path.push("chunks");
        path
    });

    // Create directories if they don't exist
    if let Some(parent) = metadata_db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir_all(&data_dir_path)?;

    let metadata_config = wormfs::metadata_store::Config {
        database_path: metadata_db_path,
        ..Default::default()
    };

    let file_store_config = wormfs::file_store::types::Config {
        disk_paths: vec![data_dir_path],
        max_chunk_size: 1024 * 1024, // 1MB
        default_data_shards: 2,
        default_parity_shards: 1,
        max_concurrent_operations: 100,
        verification_interval: std::time::Duration::from_secs(3600),
        orphan_cleanup_age: std::time::Duration::from_secs(3600),
    };

    let filesystem_config = FsConfig {
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        ..Default::default()
    };

    Ok(MountConfig {
        filesystem_config,
        metadata_config,
        file_store_config,
        mount_point,
        mount_options: MountOptions::default(),
    })
}

/// Setup signal handler for graceful shutdown.
#[cfg(feature = "fuser")]
async fn setup_signal_handler(mount_point: PathBuf) {
    use futures::StreamExt;
    use signal_hook::consts::signal::*;
    use signal_hook_tokio::Signals;

    let mut signals = match Signals::new(&[SIGINT, SIGTERM]) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to create signal handler: {}", e);
            return;
        }
    };

    if let Some(signal) = signals.next().await {
        tracing::info!("Received signal {:?}, unmounting...", signal);

        // Attempt to unmount
        if let Err(e) = wormfs::filesystem_service::mount::unmount_filesystem(&mount_point) {
            tracing::error!("Failed to unmount during shutdown: {}", e);
        }
    }
}
