use clap::{Parser, Subcommand};
use tracing::{info, warn};

mod chunk_format;

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
            warn!("Storage node implementation not yet available - this is a placeholder");
            // TODO: Implement storage node in Phase 1A
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
                Commands::Version => {}, // Success case
                _ => panic!("Expected Version command"),
            }
        }
    }
}
