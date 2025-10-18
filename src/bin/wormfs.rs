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

        /// Mount point directory (overrides config)
        #[arg(short, long)]
        mount_point: Option<PathBuf>,

        /// Run in foreground (don't daemonize)
        #[arg(short, long)]
        foreground: bool,

        /// Allow root to access filesystem
        #[arg(long)]
        allow_root: bool,

        /// Allow other users to access filesystem
        #[arg(long)]
        allow_other: bool,

        /// Enable auto-unmount on process exit (requires user_allow_other in /etc/fuse.conf)
        #[arg(long)]
        auto_unmount: bool,

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
            auto_unmount,
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
                auto_unmount,
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
    mount_point: Option<PathBuf>,
    foreground: bool,
    allow_root: bool,
    allow_other: bool,
    auto_unmount: bool,
    fuse_debug: bool,
    metadata_db_override: Option<PathBuf>,
    data_dir_override: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    use wormfs::filesystem_service::mount::MountOptions;

    tracing::info!("Starting WormFS mount...");

    // Load or create default configuration
    let mut mount_config = if let Some(config_path) = config_path {
        load_config_from_file(&config_path)?
    } else {
        // When no config file, mount_point must be provided via CLI
        let mp = mount_point.clone().ok_or_else(|| {
            "mount_point must be specified via --mount-point when not using a config file"
                .to_string()
        })?;
        create_default_config(mp, metadata_db_override.clone(), data_dir_override.clone())?
    };

    // Override mount_point from CLI if provided
    if let Some(mp) = mount_point {
        mount_config.mount_point = mp;
    }

    // Override metadata_db_path if provided via CLI
    if let Some(metadata_db) = metadata_db_override {
        mount_config.metadata_config.database_path = metadata_db;
    }

    // Override disk_paths if provided via CLI
    if let Some(data_dir) = data_dir_override {
        mount_config.file_store_config.disk_paths = vec![data_dir];
    }

    // Override mount options from CLI
    mount_config.mount_options = MountOptions {
        allow_root,
        allow_other,
        auto_unmount,
        foreground,
        debug: fuse_debug,
        ..mount_config.mount_options
    };

    // Setup signal handling for graceful shutdown
    // Keep the JoinHandle to ensure signal handler completes before exit
    let mount_point_for_signal = mount_config.mount_point.clone();
    let signal_handle = tokio::spawn(async move {
        setup_signal_handler(mount_point_for_signal).await;
    });

    // Mount filesystem (blocks until unmount)
    tracing::info!("Mounting at {:?}", mount_config.mount_point);
    wormfs::filesystem_service::mount::mount_filesystem(mount_config).await?;

    tracing::info!("Filesystem unmounted");

    // Wait for signal handler to complete its cleanup/logging
    // This is a no-op if the signal handler already completed
    let _ = signal_handle.await;

    Ok(())
}

/// Stub for when fuser feature is disabled.
#[cfg(not(feature = "fuser"))]
async fn mount_command(
    _config_path: Option<PathBuf>,
    _mount_point: Option<PathBuf>,
    _foreground: bool,
    _allow_root: bool,
    _allow_other: bool,
    _auto_unmount: bool,
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

/// Top-level WormFS configuration structure matching TOML file format.
#[cfg(feature = "fuser")]
#[derive(Debug, serde::Deserialize)]
struct WormFsConfig {
    /// Mount point for the filesystem
    mount_point: Option<PathBuf>,

    /// Metadata store configuration
    #[serde(default)]
    metadata: wormfs::metadata_store::Config,

    /// File store configuration
    #[serde(default)]
    file_store: wormfs::file_store::types::Config,

    /// Filesystem service configuration
    #[serde(default)]
    filesystem: wormfs::filesystem_service::types::Config,

    /// Metrics service configuration (optional)
    #[serde(default)]
    metrics: Option<wormfs::metric_service::Config>,

    /// Admin server configuration (optional)
    #[serde(default)]
    admin: Option<wormfs::admin::Config>,
}

/// Load configuration from TOML file.
#[cfg(feature = "fuser")]
fn load_config_from_file(
    path: &PathBuf,
) -> Result<wormfs::filesystem_service::mount::MountConfig, Box<dyn std::error::Error>> {
    use wormfs::filesystem_service::mount::{MountConfig as FinalMountConfig, MountOptions};

    // Read file
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read config file {:?}: {}", path, e))?;

    // Parse TOML
    let config: WormFsConfig =
        toml::from_str(&contents).map_err(|e| format!("Failed to parse config file: {}", e))?;

    // Validate configuration
    validate_config(&config)?;

    // Get mount point from config
    let mount_point = config.mount_point.ok_or_else(|| {
        "mount_point must be specified in config file or via --mount-point CLI flag".to_string()
    })?;

    Ok(FinalMountConfig {
        filesystem_config: config.filesystem,
        metadata_config: config.metadata,
        file_store_config: config.file_store,
        metric_config: config.metrics,
        admin_config: config.admin,
        mount_point,
        mount_options: MountOptions::default(),
    })
}

/// Validate configuration values.
#[cfg(feature = "fuser")]
fn validate_config(config: &WormFsConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Validate metadata config
    if config.metadata.cache_size_mb > 2047 {
        return Err(format!(
            "Metadata cache_size_mb ({}) exceeds maximum (2047 MB)",
            config.metadata.cache_size_mb
        )
        .into());
    }

    // Validate file store config
    if config.file_store.disk_paths.is_empty() {
        return Err("At least one disk path must be configured in [file_store] section".into());
    }

    if config.file_store.default_data_shards == 0 {
        return Err("default_data_shards must be greater than 0".into());
    }

    if config.file_store.default_parity_shards == 0 {
        return Err("default_parity_shards must be greater than 0".into());
    }

    let total_shards = config.file_store.default_data_shards as usize
        + config.file_store.default_parity_shards as usize;
    if total_shards < 2 {
        return Err("Total erasure coding shards (data + parity) must be at least 2".into());
    }

    if config.file_store.max_chunk_size == 0 {
        return Err("max_chunk_size must be greater than 0".into());
    }

    // Validate filesystem config
    if config.filesystem.max_file_handles == 0 {
        return Err("max_file_handles must be greater than 0".into());
    }

    tracing::info!("Configuration validated successfully");
    Ok(())
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
        metric_config: None, // Metrics disabled by default
        admin_config: None,  // Admin server disabled by default
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
