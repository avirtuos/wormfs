//! Admin module types and configuration.

use serde::{Deserialize, Serialize};

/// Configuration for the admin HTTP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Enable the admin server
    pub enabled: bool,

    /// Port to bind the admin server
    pub port: u16,

    /// Bind address (default: 127.0.0.1 for localhost only)
    pub bind_address: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9090,
            bind_address: "127.0.0.1".to_string(),
        }
    }
}

/// Error types for admin module.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to start admin server: {0}")]
    ServerStartup(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
