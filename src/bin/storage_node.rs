//! # StorageNode Binary
//!
//! Main entry point for the WormFS storage node daemon.

use clap::Parser;
use std::path::PathBuf;
use std::process;

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
    #[arg(short, long, default_value = "127.0.0.1:7000")]
    bind: String,

    /// Data directory for storage
    #[arg(short, long, default_value = "/var/lib/wormfs")]
    data_dir: PathBuf,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Bootstrap mode (initialize a new cluster)
    #[arg(long)]
    bootstrap: bool,
}

#[tokio::main]
async fn main() {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    tracing_subscriber::fmt()
        .with_max_level(log_level)
        .with_target(false)
        .init();

    tracing::info!("WormFS Storage Node starting...");
    tracing::info!("Bind address: {}", args.bind);
    tracing::info!("Data directory: {}", args.data_dir.display());

    // TODO: Load configuration from file if provided
    // TODO: Build StorageNode::Config from args
    // TODO: Initialize and start StorageNode
    // TODO: Wait for shutdown signal
    // TODO: Perform graceful shutdown

    tracing::warn!("StorageNode implementation is not yet complete");
    eprintln!("Error: StorageNode implementation is pending");
    process::exit(1);
}
