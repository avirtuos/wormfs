//! # StorageNode Binary
//!
//! Main entry point for the WormFS storage node daemon.

use clap::Parser;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use wormfs::admin::AdminServer;
use wormfs::filesystem_service::mount::MountConfig;
use wormfs::metric_service::{MetricService, MetricServiceImpl};
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

    /// Admin UI port (default: 8080)
    #[arg(long, default_value = "8080")]
    admin_port: u16,
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

    // Dial configured peers if network is available
    if let Some(network) = node.storage_network() {
        tracing::info!("Dialing configured peers...");
        if let Err(e) = network.dial_configured_peers().await {
            tracing::warn!("Failed to dial some peers: {}", e);
        } else {
            tracing::info!("Peer dialing initiated");
        }
    }

    // Start Admin UI
    tracing::info!("Starting Admin UI on port {}...", args.admin_port);
    let admin_config = wormfs::admin::types::Config {
        enabled: true,
        bind_address: "127.0.0.1".to_string(),
        port: args.admin_port,
    };

    // Create minimal mount config for Admin UI
    let mount_config = Arc::new(MountConfig {
        filesystem_config: wormfs::filesystem_service::types::Config::default(),
        metadata_config: wormfs::metadata_store::Config::default(),
        file_store_config: wormfs::file_store::types::Config::default(),
        metric_config: None,
        admin_config: None,
        network_config: None, // StorageNode handles networking separately
        raft_config: None,    // StorageNode handles Raft separately
        mount_point: std::path::PathBuf::from("/tmp/wormfs-not-mounted"),
        mount_options: wormfs::filesystem_service::mount::MountOptions::default(),
    });

    // Initialize metrics service
    let metric_config = wormfs::metric_service::Config::default();
    let metrics = match MetricServiceImpl::new(metric_config) {
        Ok(m) => Arc::new(m),
        Err(e) => {
            tracing::warn!("Failed to initialize metrics service: {}", e);
            tracing::warn!("Admin UI will have limited functionality");
            // Can't continue without metrics for Admin UI
            eprintln!("Failed to initialize metrics service: {}", e);
            process::exit(1);
        }
    };

    // Get network handle for Admin UI
    let network_handle = node.storage_network().map(|n| Arc::new(n.clone()));

    // Get Raft member for Admin UI
    let raft_member = node.storage_raft_member().map(|r| Arc::clone(r));

    let admin_server = AdminServer::new(
        admin_config,
        mount_config,
        metrics,
        network_handle,
        raft_member,
    );
    let _admin_handle = match admin_server.start() {
        Ok(handle) => {
            tracing::info!("Admin UI started at http://127.0.0.1:{}", args.admin_port);
            Some(handle)
        }
        Err(e) => {
            tracing::warn!("Failed to start Admin UI: {}", e);
            tracing::warn!("Continuing without Admin UI...");
            None
        }
    };

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
