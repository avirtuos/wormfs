//! Request handlers for admin endpoints.

pub mod config;
pub mod health;
pub mod logs;
pub mod metrics;

pub use config::config_handler;
pub use health::status_handler;
pub use logs::{logs_handler, LogBuffer, LogEntry};
pub use metrics::{health_handler, metrics_handler};
