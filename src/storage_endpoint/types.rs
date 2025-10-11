//! Common types for the StorageEndpoint component.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Configuration for StorageEndpoint.
#[derive(Debug, Clone)]
pub struct Config {
    /// Address to bind the gRPC server to
    pub listen_address: SocketAddr,

    /// Maximum concurrent connections
    pub max_connections: usize,

    /// Maximum message size (bytes)
    pub max_message_size: usize,

    /// Request timeout
    pub request_timeout: Duration,

    /// Enable TLS
    pub enable_tls: bool,

    /// TLS certificate path (if TLS enabled)
    pub tls_cert_path: Option<PathBuf>,

    /// TLS key path (if TLS enabled)
    pub tls_key_path: Option<PathBuf>,

    /// Enable request logging
    pub enable_logging: bool,

    /// Enable metrics
    pub enable_metrics: bool,
}

/// Errors that can occur during StorageEndpoint operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Server bind failed
    #[error("Failed to bind to address {address}: {reason}")]
    BindFailed { address: String, reason: String },

    /// Invalid TLS configuration
    #[error("Invalid TLS configuration: {0}")]
    InvalidTlsConfig(String),

    /// Server operation failed
    #[error("Server operation failed: {0}")]
    ServerFailed(String),

    /// Shutdown timeout
    #[error("Shutdown timed out after {0:?}")]
    ShutdownTimeout(Duration),

    /// Not serving
    #[error("Server is not currently serving")]
    NotServing,

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
