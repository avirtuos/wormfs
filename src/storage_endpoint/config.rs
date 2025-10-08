//! Configuration for StorageEndpoint gRPC server

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::{Result, StorageEndpointError};

/// Configuration for the StorageEndpoint gRPC server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageEndpointConfig {
    /// Address to bind the gRPC server to (e.g., "0.0.0.0" or "127.0.0.1")
    #[serde(default = "default_bind_address")]
    pub bind_address: String,

    /// Port for the gRPC server
    #[serde(default = "default_port")]
    pub port: u16,

    /// Directory where snapshots are stored
    pub snapshot_dir: PathBuf,

    /// Node ID (for authentication and logging)
    pub node_id: u64,
}

fn default_bind_address() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8082
}

impl StorageEndpointConfig {
    /// Create a new storage endpoint configuration
    pub fn new(node_id: u64, port: u16, snapshot_dir: PathBuf) -> Self {
        Self {
            bind_address: default_bind_address(),
            port,
            snapshot_dir,
            node_id,
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.bind_address.is_empty() {
            return Err(StorageEndpointError::Config(
                "bind_address cannot be empty".to_string(),
            ));
        }

        if self.port == 0 {
            return Err(StorageEndpointError::Config("port must be > 0".to_string()));
        }

        if self.snapshot_dir.as_os_str().is_empty() {
            return Err(StorageEndpointError::Config(
                "snapshot_dir cannot be empty".to_string(),
            ));
        }

        Ok(())
    }

    /// Get the full server address (bind_address:port)
    pub fn server_address(&self) -> String {
        format!("{}:{}", self.bind_address, self.port)
    }

    /// Get the public endpoint address (for advertising to peers)
    /// This replaces 0.0.0.0 with the actual hostname/IP if needed
    pub fn public_address(&self, hostname: Option<&str>) -> String {
        let addr = if self.bind_address == "0.0.0.0" {
            hostname.unwrap_or("localhost")
        } else {
            &self.bind_address
        };
        format!("http://{}:{}", addr, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = StorageEndpointConfig::new(1, 8082, PathBuf::from("/data/snapshots"));
        assert_eq!(config.node_id, 1);
        assert_eq!(config.port, 8082);
        assert_eq!(config.bind_address, "0.0.0.0");
    }

    #[test]
    fn test_config_validation() {
        let config = StorageEndpointConfig::new(1, 8082, PathBuf::from("/data/snapshots"));
        assert!(config.validate().is_ok());

        let invalid_config = StorageEndpointConfig {
            bind_address: "".to_string(),
            port: 8082,
            snapshot_dir: PathBuf::from("/data"),
            node_id: 1,
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_server_address() {
        let config = StorageEndpointConfig::new(1, 8082, PathBuf::from("/data/snapshots"));
        assert_eq!(config.server_address(), "0.0.0.0:8082");
    }

    #[test]
    fn test_public_address() {
        let config = StorageEndpointConfig::new(1, 8082, PathBuf::from("/data/snapshots"));
        assert_eq!(
            config.public_address(Some("node1.example.com")),
            "http://node1.example.com:8082"
        );
        assert_eq!(config.public_address(None), "http://localhost:8082");

        let config2 = StorageEndpointConfig {
            bind_address: "192.168.1.100".to_string(),
            port: 8082,
            snapshot_dir: PathBuf::from("/data"),
            node_id: 1,
        };
        assert_eq!(config2.public_address(None), "http://192.168.1.100:8082");
    }
}
