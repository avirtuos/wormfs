//! Request handlers for admin endpoints.

pub mod config;
pub mod health;
pub mod logs;
pub mod metrics;
pub mod network;
pub mod raft;

pub use config::config_handler;
pub use health::status_handler;
pub use logs::{logs_handler, LogBuffer, LogEntry};
pub use metrics::{component_metrics_handler, components_handler, health_handler, metrics_handler};
pub use network::{network_status_handler, peers_handler};
pub use raft::{raft_metrics_handler, raft_status_handler};
