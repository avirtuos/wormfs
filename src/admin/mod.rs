//! Admin module for WormFS management and monitoring.
//!
//! This module provides a comprehensive admin interface with:
//! - REST API endpoints for metrics, configuration, health, and logs
//! - WebSocket streaming for real-time metrics updates
//! - Web-based UI for monitoring and managing the filesystem
//!
//! # Architecture
//!
//! The admin module is organized as follows:
//! - `types`: Configuration and error types
//! - `handlers`: Request handlers for API endpoints
//! - `ui`: HTML/CSS/JS templates for the web interface
//! - `websocket`: WebSocket support for real-time streaming
//! - `server`: Main HTTP server implementation
//!
//! # Usage
//!
//! ```rust,no_run
//! use wormfs::admin::{AdminServer, Config};
//! use wormfs::metric_service::{MetricService, MetricServiceImpl};
//! use wormfs::filesystem_service::mount::MountConfig;
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Config::default();
//! let metrics = Arc::new(MetricServiceImpl::new(Default::default())?);
//!
//! // Create a minimal MountConfig
//! let mount_config = Arc::new(MountConfig {
//!     filesystem_config: Default::default(),
//!     metadata_config: Default::default(),
//!     file_store_config: Default::default(),
//!     metric_config: Some(Default::default()),
//!     admin_config: Some(config.clone()),
//!     network_config: None,
//!     mount_point: std::path::PathBuf::from("/tmp/wormfs"),
//!     mount_options: Default::default(),
//! });
//!
//! let server = AdminServer::new(config, mount_config, metrics, None);
//! let handle = server.start()?;
//!
//! // Server runs in the background
//! // Access UI at http://127.0.0.1:9090/
//! # Ok(())
//! # }
//! ```

pub mod config_provider;
pub mod config_providers;
pub mod handlers;
pub mod server;
pub mod types;
pub mod ui;
pub mod websocket;

pub use config_provider::{ConfigProvider, ConfigRegistry, ConfigWithDescriptions};
pub use server::AdminServer;
pub use types::{Config, Error};
