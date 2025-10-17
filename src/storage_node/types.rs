//! Common types for the StorageNode component.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Configuration for a StorageNode instance.
///
/// This structure contains all the settings needed to initialize and configure
/// a WormFS storage node, including network settings, storage paths, and
/// operational parameters.
///
/// Can be loaded from TOML files, environment variables, or constructed programmatically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Unique identifier for this node
    pub node_id: String,

    /// Address to bind for client connections
    #[serde(default = "default_listen_address")]
    pub listen_address: SocketAddr,

    /// Directory for all data storage
    pub data_dir: PathBuf,

    /// Path to metadata database
    pub metadata_db_path: PathBuf,

    /// Path to transaction log (Phase 2+)
    #[serde(default)]
    pub transaction_log_path: PathBuf,

    /// Directory for metadata snapshots (Phase 2+)
    #[serde(default)]
    pub snapshot_dir: PathBuf,

    /// Peer node addresses for cluster membership (Phase 2+)
    #[serde(default)]
    pub peer_addresses: Vec<SocketAddr>,

    /// libp2p listening port (Phase 2+)
    #[serde(default = "default_libp2p_port")]
    pub libp2p_listen_port: u16,

    /// Default stripe size in bytes
    #[serde(default = "default_stripe_size")]
    pub default_stripe_size: u64,

    /// Default number of data shards for erasure coding
    #[serde(default = "default_data_shards")]
    pub default_data_shards: u8,

    /// Default number of parity shards for erasure coding
    #[serde(default = "default_parity_shards")]
    pub default_parity_shards: u8,

    /// Default UID for filesystem operations
    #[serde(default = "default_uid")]
    pub default_uid: u32,

    /// Default GID for filesystem operations
    #[serde(default = "default_gid")]
    pub default_gid: u32,

    /// Enable read lock optimization
    #[serde(default = "default_true")]
    pub enable_read_locks: bool,

    /// Lock timeout duration in seconds
    #[serde(default = "default_lock_timeout_secs", with = "duration_serde")]
    pub lock_timeout: Duration,

    /// Watchdog shallow check interval in seconds (Phase 4+)
    #[serde(default = "default_shallow_check_secs", with = "duration_serde")]
    pub shallow_check_interval: Duration,

    /// Watchdog deep check interval in seconds (Phase 4+)
    #[serde(default = "default_deep_check_secs", with = "duration_serde")]
    pub deep_check_interval: Duration,

    /// Snapshot creation interval in seconds (Phase 2+)
    #[serde(default = "default_snapshot_secs", with = "duration_serde")]
    pub snapshot_interval: Duration,
}

// ===== Default Value Functions =====

fn default_listen_address() -> SocketAddr {
    "127.0.0.1:7000".parse().unwrap()
}

fn default_libp2p_port() -> u16 {
    7001
}

fn default_stripe_size() -> u64 {
    1024 * 1024 // 1MB
}

fn default_data_shards() -> u8 {
    2
}

fn default_parity_shards() -> u8 {
    1
}

fn default_uid() -> u32 {
    1000
}

fn default_gid() -> u32 {
    1000
}

fn default_true() -> bool {
    true
}

fn default_lock_timeout_secs() -> Duration {
    Duration::from_secs(30)
}

fn default_shallow_check_secs() -> Duration {
    Duration::from_secs(300) // 5 minutes
}

fn default_deep_check_secs() -> Duration {
    Duration::from_secs(3600) // 1 hour
}

fn default_snapshot_secs() -> Duration {
    Duration::from_secs(1800) // 30 minutes
}

// ===== Custom Serialization for Duration =====

mod duration_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: "wormfs-node-001".to_string(),
            listen_address: default_listen_address(),
            data_dir: PathBuf::from("/var/lib/wormfs"),
            metadata_db_path: PathBuf::from("/var/lib/wormfs/metadata.db"),
            transaction_log_path: PathBuf::from("/var/lib/wormfs/transaction_log"),
            snapshot_dir: PathBuf::from("/var/lib/wormfs/snapshots"),
            peer_addresses: Vec::new(),
            libp2p_listen_port: default_libp2p_port(),
            default_stripe_size: default_stripe_size(),
            default_data_shards: default_data_shards(),
            default_parity_shards: default_parity_shards(),
            default_uid: default_uid(),
            default_gid: default_gid(),
            enable_read_locks: default_true(),
            lock_timeout: default_lock_timeout_secs(),
            shallow_check_interval: default_shallow_check_secs(),
            deep_check_interval: default_deep_check_secs(),
            snapshot_interval: default_snapshot_secs(),
        }
    }
}

impl Config {
    /// Load configuration from a TOML file.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TOML configuration file
    ///
    /// # Returns
    ///
    /// Parsed configuration with defaults applied for missing fields.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - File cannot be read
    /// - TOML parsing fails
    /// - Configuration validation fails
    pub fn from_file(path: &std::path::Path) -> Result<Self, Error> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| Error::InvalidConfig(format!("Failed to read config file: {}", e)))?;

        let config: Config = toml::from_str(&contents)
            .map_err(|e| Error::InvalidConfig(format!("Failed to parse TOML config: {}", e)))?;

        config.validate()?;

        Ok(config)
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration values are invalid.
    pub fn validate(&self) -> Result<(), Error> {
        if self.node_id.is_empty() {
            return Err(Error::InvalidConfig("node_id cannot be empty".to_string()));
        }

        if self.default_data_shards == 0 {
            return Err(Error::InvalidConfig(
                "default_data_shards must be > 0".to_string(),
            ));
        }

        if self.default_stripe_size == 0 {
            return Err(Error::InvalidConfig(
                "default_stripe_size must be > 0".to_string(),
            ));
        }

        Ok(())
    }

    /// Merge configuration with environment variables.
    ///
    /// Environment variables override TOML values:
    /// - `WORMFS_NODE_ID` - Node identifier
    /// - `WORMFS_DATA_DIR` - Data directory path
    /// - `WORMFS_LISTEN_ADDRESS` - Bind address
    ///
    /// # Example
    ///
    /// ```ignore
    /// std::env::set_var("WORMFS_NODE_ID", "custom-node");
    /// let config = Config::default().with_env_overrides();
    /// assert_eq!(config.node_id, "custom-node");
    /// ```
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(node_id) = std::env::var("WORMFS_NODE_ID") {
            self.node_id = node_id;
        }

        if let Ok(data_dir) = std::env::var("WORMFS_DATA_DIR") {
            self.data_dir = PathBuf::from(data_dir);
        }

        if let Ok(listen_addr) = std::env::var("WORMFS_LISTEN_ADDRESS") {
            if let Ok(addr) = listen_addr.parse() {
                self.listen_address = addr;
            }
        }

        self
    }
}

/// Errors that can occur during StorageNode operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Configuration validation error
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Component initialization failed
    #[error("Failed to initialize component {component}: {reason}")]
    ComponentInitFailed { component: String, reason: String },

    /// Network binding error
    #[error("Failed to bind to address {address}: {reason}")]
    NetworkBindFailed { address: String, reason: String },

    /// Storage I/O error
    #[error("Storage I/O error: {0}")]
    StorageIo(#[from] std::io::Error),

    /// Component not initialized
    #[error("Component {0} is not initialized")]
    ComponentNotInitialized(String),

    /// Shutdown error
    #[error("Failed to shutdown cleanly: {0}")]
    ShutdownFailed(String),

    /// Node is not the leader
    #[error("Operation requires leader node")]
    NotLeader,

    /// Cluster communication error
    #[error("Cluster communication error: {0}")]
    ClusterError(String),
}
