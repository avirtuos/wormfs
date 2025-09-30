use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{error, info, warn};
use uuid::Uuid;
use wormfs::{StorageNode, StorageNodeConfig};

#[derive(Parser)]
#[command(name = "wormfs")]
#[command(about = "A distributed, erasure-coded filesystem with configurable redundancy")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a storage node
    StorageNode {
        /// Configuration file path
        #[arg(short, long, default_value = "config/storage_node.yaml")]
        config: String,
    },
    /// Store a file in the storage system
    Store {
        /// Path to the file to store
        #[arg(short, long)]
        file: PathBuf,
        /// Virtual path in the storage system
        #[arg(short, long)]
        path: PathBuf,
        /// Configuration file path
        #[arg(short, long, default_value = "config/storage_node.yaml")]
        config: String,
    },
    /// Retrieve a file from the storage system
    Retrieve {
        /// File ID to retrieve
        #[arg(short, long)]
        file_id: String,
        /// Output path for the retrieved file
        #[arg(short, long)]
        output: PathBuf,
        /// Configuration file path
        #[arg(short, long, default_value = "config/storage_node.yaml")]
        config: String,
    },
    /// List all stored files
    List {
        /// Configuration file path
        #[arg(short, long, default_value = "config/storage_node.yaml")]
        config: String,
    },
    /// Delete a file from the storage system
    Delete {
        /// File ID to delete
        #[arg(short, long)]
        file_id: String,
        /// Configuration file path
        #[arg(short, long, default_value = "config/storage_node.yaml")]
        config: String,
    },
    /// Get storage node statistics
    Stats {
        /// Configuration file path
        #[arg(short, long, default_value = "config/storage_node.yaml")]
        config: String,
    },
    /// Run a FUSE client
    Client {
        /// Mount point for the filesystem
        #[arg(short, long)]
        mount: String,
        /// Storage node addresses
        #[arg(short, long)]
        nodes: Vec<String>,
    },
    /// Health check for the service
    HealthCheck,
    /// Show version information
    Version,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::StorageNode { config } => {
            info!("Starting WormFS storage node with config: {}", config);

            // Load or create configuration
            let storage_config = if std::path::Path::new(&config).exists() {
                StorageNodeConfig::from_file(&config)?
            } else {
                warn!("Configuration file not found, creating default configuration");
                let default_config = StorageNodeConfig::new()?;

                // Create config directory if it doesn't exist
                if let Some(parent) = std::path::Path::new(&config).parent() {
                    std::fs::create_dir_all(parent)?;
                }

                // Save default config
                default_config.save_to_file(&config)?;
                info!("Default configuration saved to: {}", config);
                default_config
            };

            // Initialize and start storage node
            let storage_node = StorageNode::new(storage_config)?;

            info!("Storage node started successfully");
            info!("Node ID: {}", storage_node.config().node_id);
            info!("Storage root: {:?}", storage_node.config().storage_root);
            info!("Metadata DB: {:?}", storage_node.config().metadata_db_path);

            // Print stats
            let stats = storage_node.get_stats()?;
            info!(
                "Current stats: {} files, {} chunks, {} bytes",
                stats.total_files, stats.total_chunks, stats.total_size
            );

            // Keep running (in a real implementation, this would be a server loop)
            info!("Storage node running. Press Ctrl+C to stop.");
            tokio::signal::ctrl_c().await?;
            info!("Shutting down storage node");

            Ok(())
        }
        Commands::Store { file, path, config } => {
            info!("Storing file: {:?} -> {:?}", file, path);

            let storage_config = StorageNodeConfig::from_file(&config)?;
            let storage_node = StorageNode::new(storage_config)?;

            let file_id = storage_node.store_file(&file, &path)?;

            println!("File stored successfully!");
            println!("File ID: {}", file_id);
            println!("Virtual path: {:?}", path);

            Ok(())
        }
        Commands::Retrieve {
            file_id,
            output,
            config,
        } => {
            info!("Retrieving file: {} -> {:?}", file_id, output);

            let storage_config = StorageNodeConfig::from_file(&config)?;
            let storage_node = StorageNode::new(storage_config)?;

            let file_uuid = Uuid::parse_str(&file_id)?;
            storage_node.retrieve_file(file_uuid, &output)?;

            println!("File retrieved successfully!");
            println!("Output file: {:?}", output);

            Ok(())
        }
        Commands::List { config } => {
            let storage_config = StorageNodeConfig::from_file(&config)?;
            let storage_node = StorageNode::new(storage_config)?;

            let files = storage_node.list_files()?;

            if files.is_empty() {
                println!("No files stored.");
            } else {
                println!("Stored files:");
                println!(
                    "{:<36} {:<50} {:<12} {:<8} {:<10}",
                    "File ID", "Path", "Size", "Stripes", "Chunks"
                );
                println!("{}", "-".repeat(120));

                for file in files {
                    println!(
                        "{:<36} {:<50} {:<12} {:<8} {:<10}",
                        file.file_id,
                        file.path.display(),
                        format_bytes(file.size),
                        file.stripe_count,
                        file.chunk_count
                    );
                }
            }

            Ok(())
        }
        Commands::Delete { file_id, config } => {
            info!("Deleting file: {}", file_id);

            let storage_config = StorageNodeConfig::from_file(&config)?;
            let storage_node = StorageNode::new(storage_config)?;

            let file_uuid = Uuid::parse_str(&file_id)?;
            storage_node.delete_file(file_uuid)?;

            println!("File deleted successfully!");

            Ok(())
        }
        Commands::Stats { config } => {
            let storage_config = StorageNodeConfig::from_file(&config)?;
            let storage_node = StorageNode::new(storage_config)?;

            let stats = storage_node.get_stats()?;

            println!("Storage Node Statistics:");
            println!("  Node ID: {}", storage_node.config().node_id);
            println!("  Total files: {}", stats.total_files);
            println!("  Total chunks: {}", stats.total_chunks);
            println!("  Total size: {}", format_bytes(stats.total_size));
            println!("  Available space: {}", format_bytes(stats.available_space));
            println!(
                "  Erasure coding: {}+{} (data+parity shards)",
                stats.erasure_config.data_shards, stats.erasure_config.parity_shards
            );
            println!(
                "  Stripe size: {}",
                format_bytes(stats.erasure_config.stripe_size as u64)
            );

            Ok(())
        }
        Commands::Client { mount, nodes } => {
            info!("Starting WormFS client, mounting at: {}", mount);
            info!("Connecting to storage nodes: {:?}", nodes);
            warn!("FUSE client implementation not yet available - this is a placeholder");
            // TODO: Implement FUSE client in Phase 3B
            Ok(())
        }
        Commands::HealthCheck => {
            info!("WormFS health check");
            println!("OK");
            Ok(())
        }
        Commands::Version => {
            println!("WormFS v{}", env!("CARGO_PKG_VERSION"));
            println!("A distributed, erasure-coded filesystem");
            Ok(())
        }
    }
}

/// Format bytes in a human-readable format
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        // Test that CLI parsing works
        let cli = Cli::try_parse_from(["wormfs", "health-check"]);
        assert!(cli.is_ok());
    }

    #[test]
    fn test_version_command() {
        let cli = Cli::try_parse_from(["wormfs", "version"]);
        assert!(cli.is_ok());

        if let Ok(cli) = cli {
            match cli.command {
                Commands::Version => {} // Success case
                _ => panic!("Expected Version command"),
            }
        }
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
    }
}
