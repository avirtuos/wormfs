//! Common types for the StorageNode component.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Configuration for a StorageNode instance.
///
/// This structure contains all the settings needed to initialize and configure
/// a WormFS storage node, including network settings, storage paths, and
/// operational parameters.
#[derive(Debug, Clone)]
pub struct Config {
    /// Unique identifier for this node
    pub node_id: String,

    /// Address to bind for client connections
    pub listen_address: SocketAddr,

    /// Directory for all data storage
    pub data_dir: PathBuf,

    /// Path to metadata database
    pub metadata_db_path: PathBuf,

    /// Path to transaction log
    pub transaction_log_path: PathBuf,

    /// Directory for metadata snapshots
    pub snapshot_dir: PathBuf,

    /// Peer node addresses for cluster membership
    pub peer_addresses: Vec<SocketAddr>,

    /// libp2p listening port
    pub libp2p_listen_port: u16,

    /// Default stripe size in bytes
    pub default_stripe_size: u64,

    /// Default number of data shards for erasure coding
    pub default_data_shards: u8,

    /// Default number of parity shards for erasure coding
    pub default_parity_shards: u8,

    /// Enable read lock optimization
    pub enable_read_locks: bool,

    /// Lock timeout duration
    pub lock_timeout: Duration,

    /// Watchdog shallow check interval
    pub shallow_check_interval: Duration,

    /// Watchdog deep check interval
    pub deep_check_interval: Duration,

    /// Snapshot creation interval
    pub snapshot_interval: Duration,
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
