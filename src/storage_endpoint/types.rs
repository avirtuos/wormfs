//! Common types for the StorageEndpoint component.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Configuration for StorageEndpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EndpointConfig {
    /// Address to bind the gRPC server to
    pub listen_address: SocketAddr,

    /// Maximum concurrent requests
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,

    /// Maximum message size (bytes)
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    /// Request timeout in seconds
    #[serde(default = "default_request_timeout", with = "duration_serde")]
    pub request_timeout: Duration,

    /// Enable TLS
    #[serde(default)]
    pub enable_tls: bool,

    /// Enable authentication
    #[serde(default)]
    pub enable_auth: bool,

    /// Directory containing PSK identity files
    /// Each file represents a client or node identity with the filename as the identity name
    #[serde(default)]
    pub identities_dir: Option<PathBuf>,

    /// Which PSK file in identities_dir to use for this node's identity
    #[serde(default)]
    pub node_identity: Option<String>,

    /// TLS certificate file path (PEM format)
    /// Required when enable_tls is true
    #[serde(default)]
    pub tls_cert_path: Option<PathBuf>,

    /// TLS private key file path (PEM format)
    /// Required when enable_tls is true
    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,

    /// Per-client rate limit (requests per second per identity)
    #[serde(default)]
    pub rate_limit_per_client: Option<usize>,

    /// Overall rate limit (total requests per second for the node)
    #[serde(default)]
    pub rate_limit_overall: Option<usize>,

    /// Rate limit burst size
    #[serde(default = "default_rate_limit_burst_size")]
    pub rate_limit_burst_size: usize,

    /// Enable request logging
    #[serde(default = "default_enable_logging")]
    pub enable_logging: bool,

    /// Enable metrics
    #[serde(default = "default_enable_metrics")]
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
            tls_cert_path: None,
            tls_key_path: None,
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

// Default value functions for serde
fn default_max_concurrent_requests() -> usize {
    1000
}

fn default_max_message_size() -> usize {
    4 * 1024 * 1024 // 4MB
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_rate_limit_burst_size() -> usize {
    100
}

fn default_enable_logging() -> bool {
    false
}

fn default_enable_metrics() -> bool {
    true
}

// Duration serialization/deserialization for serde
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
