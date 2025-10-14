//! Common types for the StorageEndpoint component.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Configuration for StorageEndpoint.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    /// Address to bind the gRPC server to
    pub listen_address: SocketAddr,

    /// Maximum concurrent requests
    pub max_concurrent_requests: usize,

    /// Maximum message size (bytes)
    pub max_message_size: usize,

    /// Request timeout
    pub request_timeout: Duration,

    /// Enable TLS
    pub enable_tls: bool,

    /// Enable authentication
    pub enable_auth: bool,

    /// Directory containing PSK identity files
    /// Each file represents a client or node identity with the filename as the identity name
    pub identities_dir: Option<PathBuf>,

    /// Which PSK file in identities_dir to use for this node's identity
    pub node_identity: Option<String>,

    /// Per-client rate limit (requests per second per identity)
    pub rate_limit_per_client: Option<usize>,

    /// Overall rate limit (total requests per second for the node)
    pub rate_limit_overall: Option<usize>,

    /// Rate limit burst size
    pub rate_limit_burst_size: usize,

    /// Enable request logging
    pub enable_logging: bool,

    /// Enable metrics
    pub enable_metrics: bool,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            listen_address: "0.0.0.0:7000".parse().unwrap(),
            max_concurrent_requests: 1000,
            max_message_size: 4 * 1024 * 1024, // 4MB
            request_timeout: Duration::from_secs(30),
            enable_tls: true,
            enable_auth: true,
            identities_dir: Some(PathBuf::from("/etc/wormfs/identities")),
            node_identity: Some("storage_node".to_string()),
            rate_limit_per_client: Some(100),
            rate_limit_overall: Some(1000),
            rate_limit_burst_size: 100,
            enable_logging: true,
            enable_metrics: true,
        }
    }
}

/// Errors that can occur during StorageEndpoint operations.
#[derive(Error, Debug)]
pub enum EndpointError {
    /// Server bind failed
    #[error("Failed to bind to address {address}: {reason}")]
    BindFailed { address: String, reason: String },

    /// gRPC transport error
    #[error("gRPC error: {0}")]
    GrpcError(String),

    /// Invalid request
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// Authentication failed
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Rate limit exceeded
    #[error("Rate limit exceeded for client {client}")]
    RateLimitExceeded { client: String },

    /// Invalid TLS configuration
    #[error("Invalid TLS configuration: {0}")]
    InvalidTlsConfig(String),

    /// Invalid PSK configuration
    #[error("Invalid PSK configuration: {0}")]
    InvalidPskConfig(String),

    /// PSK not found
    #[error("PSK file not found for identity: {0}")]
    PskNotFound(String),

    /// Server operation failed
    #[error("Server operation failed: {0}")]
    ServerFailed(String),

    /// Shutdown timeout
    #[error("Shutdown timed out after {0:?}")]
    ShutdownTimeout(Duration),

    /// Not serving
    #[error("Server is not currently serving")]
    NotServing,

    /// Not the leader (redirect required)
    #[error("Not the leader, redirect to: {leader}")]
    NotLeader { leader: String },

    /// Internal error
    #[error("Internal error: {0}")]
    InternalError(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
