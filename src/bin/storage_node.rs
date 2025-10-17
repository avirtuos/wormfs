//! # StorageNode Binary
//!
//! Main entry point for the WormFS storage node daemon.

use clap::Parser;
use std::path::PathBuf;
use std::process;
use wormfs::storage_node::{Config, StorageNode, StorageNodeFactory};

/// WormFS Storage Node - Distributed filesystem storage daemon
#[derive(Parser, Debug)]
#[command(name = "wormfs-storage-node")]
#[command(about = "WormFS distributed storage node daemon", long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Node ID (unique identifier for this node)
    #[arg(short, long)]
    node_id: Option<String>,

    /// Bind address for gRPC endpoint
    #[arg(short, long)]
    bind: Option<String>,

    /// Data directory for storage
    #[arg(short, long)]
    data_dir: Option<PathBuf>,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Enable debug logging
    #[arg(long)]
    debug: bool,

    /// Bootstrap mode (initialize a new cluster - Phase 2+)
    #[arg(long)]
    bootstrap: bool,
}

#[tokio::main]
async fn main() {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.debug {
        tracing::Level::DEBUG
    } else if args.verbose {
        tracing::Level::INFO
    } else {
        tracing::Level::WARN
    };

    tracing_subscriber::fmt()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(args.debug)
        .init();

    tracing::info!("WormFS Storage Node starting...");

    // Load configuration
    let config = match load_config(&args) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            process::exit(1);
        }
    };

    tracing::info!("Node ID: {}", config.node_id);
    tracing::info!("Data directory: {}", config.data_dir.display());
    tracing::info!("Bind address: {}", config.listen_address);

    // Initialize StorageNode
    tracing::info!("Initializing storage node...");
    let mut node = match StorageNodeFactory::create_concrete(config).await {
        Ok(node) => node,
        Err(e) => {
            eprintln!("Failed to initialize storage node: {}", e);
            process::exit(1);
        }
    };

    // Start the node
    tracing::info!("Starting storage node...");
    if let Err(e) = node.start().await {
        eprintln!("Failed to start storage node: {}", e);
        process::exit(1);
    }

    tracing::info!("Storage node started successfully");
    tracing::info!("Status: {:?}", node.get_status());

    // Print filesystem service availability
    if let Some(_fs_service) = node.filesystem_service() {
        tracing::info!("FileSystemService is available for mounting");
        tracing::info!("Use 'wormfs mount' command to mount the filesystem");
    }

    // Setup signal handlers for graceful shutdown
    setup_signal_handlers();

    // Wait for shutdown signal
    tracing::info!("Press Ctrl+C to shutdown...");
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            tracing::info!("Shutdown signal received");
        }
        Err(e) => {
            eprintln!("Error setting up signal handler: {}", e);
        }
    }

    // Graceful shutdown
    tracing::info!("Shutting down storage node...");
    if let Err(e) = node.shutdown().await {
        eprintln!("Error during shutdown: {}", e);
        process::exit(1);
    }

    tracing::info!("Storage node shutdown complete");
}

/// Load configuration from file and apply overrides from CLI arguments and environment.
fn load_config(args: &Args) -> Result<Config, String> {
    // Start with config from file, or default if no file specified
    let mut config = if let Some(ref config_path) = args.config {
        tracing::info!("Loading configuration from: {}", config_path.display());
        Config::from_file(config_path).map_err(|e| format!("Failed to load config file: {}", e))?
    } else {
        tracing::info!("No config file specified, using defaults");
        Config::default()
    };

    // Apply environment variable overrides
    config = config.with_env_overrides();

    // Apply CLI argument overrides (highest priority)
    if let Some(ref node_id) = args.node_id {
        config.node_id = node_id.clone();
    }

    if let Some(ref bind_addr) = args.bind {
        config.listen_address = bind_addr
            .parse()
            .map_err(|e| format!("Invalid bind address: {}", e))?;
    }

    if let Some(ref data_dir) = args.data_dir {
        config.data_dir = data_dir.clone();
        // Update derived paths
        config.metadata_db_path = data_dir.join("metadata.db");
    }

    // Validate final configuration
    config
        .validate()
        .map_err(|e| format!("Configuration validation failed: {}", e))?;

    Ok(config)
}

/// Setup signal handlers for graceful shutdown.
fn setup_signal_handlers() {
    // Additional signal handlers can be added here for SIGTERM, SIGHUP, etc.
    // For now, Ctrl+C (SIGINT) is handled in main via tokio::signal::ctrl_c()
    tracing::debug!("Signal handlers configured");
}
