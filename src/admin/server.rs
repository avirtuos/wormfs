//! Main admin HTTP server implementation.
//!
//! Provides the admin interface with REST API endpoints, WebSocket streaming,
//! and a web-based UI for monitoring and managing WormFS.

use super::{
    handlers::{
        component_metrics_handler, components_handler, config_handler, health_handler,
        logs_handler, metrics_handler, network_status_handler, peers_handler, raft_metrics_handler,
        raft_proposal_details_handler, raft_proposals_handler, raft_status_handler, status_handler,
    },
    types::{Config, Error},
    ui::templates::INDEX_HTML,
    websocket::{ws_handler, WsState},
};
use crate::filesystem_service::mount::MountConfig;
use crate::metric_service::MetricServiceImpl;
use crate::storage_network::StorageNetworkHandle;
use crate::storage_raft_member::StorageRaftMemberImpl;
use axum::{
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use std::sync::Arc;
use tower_http::trace::TraceLayer;

/// Admin server instance
pub struct AdminServer {
    config: Config,
    mount_config: Arc<MountConfig>,
    metrics: Arc<MetricServiceImpl>,
    network: Option<Arc<StorageNetworkHandle>>,
    raft_member: Option<Arc<StorageRaftMemberImpl>>,
}

impl AdminServer {
    /// Create a new admin server instance
    pub fn new(
        config: Config,
        mount_config: Arc<MountConfig>,
        metrics: Arc<MetricServiceImpl>,
        network: Option<Arc<StorageNetworkHandle>>,
        raft_member: Option<Arc<StorageRaftMemberImpl>>,
    ) -> Self {
        Self {
            config,
            mount_config,
            metrics,
            network,
            raft_member,
        }
    }

    /// Start the admin server
    ///
    /// This spawns a background task that runs the HTTP server on the configured port.
    /// The server provides:
    /// - REST API endpoints at `/api/*`
    /// - WebSocket streaming at `/ws/metrics`
    /// - Web UI at `/`
    ///
    /// # Returns
    ///
    /// A `tokio::task::JoinHandle` for the background server task.
    pub fn start(self) -> Result<tokio::task::JoinHandle<()>, Error> {
        if !self.config.enabled {
            tracing::info!("Admin server is disabled");
            return Err(Error::InvalidConfig("Admin server is disabled".to_string()));
        }

        let config = self.config.clone();
        let mount_config = self.mount_config.clone();
        let metrics = self.metrics.clone();
        let network = self.network.clone();
        let raft_member = self.raft_member.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) =
                Self::run_server(config, mount_config, metrics, network, raft_member).await
            {
                tracing::error!("Admin server error: {}", e);
            }
        });

        Ok(handle)
    }

    /// Internal method to run the server
    async fn run_server(
        config: Config,
        mount_config: Arc<MountConfig>,
        metrics: Arc<MetricServiceImpl>,
        network: Option<Arc<StorageNetworkHandle>>,
        raft_member: Option<Arc<StorageRaftMemberImpl>>,
    ) -> Result<(), Error> {
        // Create WebSocket state and start broadcast task
        let ws_state = WsState::new(metrics.clone());
        ws_state.start_broadcast_task();

        // Build the router
        let app = Self::create_router(mount_config, metrics, ws_state, network, raft_member);

        // Create bind address
        let addr = format!("{}:{}", config.bind_address, config.port);
        let socket_addr: std::net::SocketAddr = addr
            .parse()
            .map_err(|e| Error::ServerStartup(format!("Invalid bind address: {}", e)))?;

        tracing::info!("Starting admin server on http://{}", socket_addr);

        // Bind and serve
        let listener = tokio::net::TcpListener::bind(socket_addr)
            .await
            .map_err(|e| Error::ServerStartup(format!("Failed to bind: {}", e)))?;

        axum::serve(listener, app)
            .await
            .map_err(|e| Error::ServerStartup(format!("Server error: {}", e)))?;

        Ok(())
    }

    /// Create the Axum router with all routes
    fn create_router(
        mount_config: Arc<MountConfig>,
        metrics: Arc<MetricServiceImpl>,
        ws_state: WsState,
        network: Option<Arc<StorageNetworkHandle>>,
        raft_member: Option<Arc<StorageRaftMemberImpl>>,
    ) -> Router {
        // Create WebSocket router with WsState
        let ws_router = Router::new()
            .route("/ws/metrics", get(ws_handler))
            .with_state(ws_state);

        // Create config router with MountConfig state
        let config_router = Router::new()
            .route("/api/config", get(config_handler))
            .with_state(mount_config);

        // Create network router with StorageNetworkHandle state
        let network_router = if let Some(net) = network {
            Router::new()
                .route("/api/network/status", get(network_status_handler))
                .route("/api/network/peers", get(peers_handler))
                .with_state(net)
        } else {
            Router::new()
        };

        // Create Raft router with StorageRaftMemberImpl state
        let raft_router = if let Some(raft) = raft_member {
            Router::new()
                .route("/api/raft/metrics", get(raft_metrics_handler))
                .route("/api/raft/status", get(raft_status_handler))
                .route("/api/raft/proposals", get(raft_proposals_handler))
                .route(
                    "/api/raft/proposals/{log_index}",
                    get(raft_proposal_details_handler),
                )
                .with_state(raft)
        } else {
            Router::new()
        };

        // Create main router with metrics state
        let api_router = Router::new()
            // UI routes
            .route("/", get(index_handler))
            // API routes
            .route("/api/metrics", get(metrics_handler))
            .route("/api/metrics/components", get(components_handler))
            .route("/api/metrics/{component}", get(component_metrics_handler))
            .route("/api/health", get(health_handler))
            .route("/api/status", get(status_handler))
            .route("/api/logs", get(logs_handler))
            .with_state(metrics);

        // Merge routers and add tracing layer
        api_router
            .merge(config_router)
            .merge(network_router)
            .merge(raft_router)
            .merge(ws_router)
            .layer(TraceLayer::new_for_http())
    }
}

/// Handler for the index page (main UI)
async fn index_handler() -> impl IntoResponse {
    Html(INDEX_HTML)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric_service::{Config as MetricsConfig, MetricService};

    #[tokio::test]
    async fn test_admin_server_creation() {
        let admin_config = Config {
            enabled: true,
            port: 9090,
            bind_address: "127.0.0.1".to_string(),
        };

        let metrics_config = MetricsConfig {
            enabled: true,
            ..Default::default()
        };

        let metrics =
            Arc::new(MetricServiceImpl::new(metrics_config).expect("Failed to create metrics"));

        // Create a minimal MountConfig for testing
        let mount_config = Arc::new(MountConfig {
            filesystem_config: crate::filesystem_service::types::Config::default(),
            metadata_config: crate::metadata_store::Config::default(),
            file_store_config: crate::file_store::types::Config::default(),
            metric_config: Some(MetricsConfig::default()),
            admin_config: Some(admin_config.clone()),
            network_config: None,
            raft_config: None,
            storage_endpoint_config: None,
            mount_point: std::path::PathBuf::from("/tmp/test"),
            mount_options: crate::filesystem_service::mount::MountOptions::default(),
        });

        let server = AdminServer::new(admin_config.clone(), mount_config, metrics, None, None);

        assert_eq!(server.config.port, 9090);
        assert_eq!(server.config.bind_address, "127.0.0.1");
    }

    #[tokio::test]
    async fn test_disabled_server() {
        let admin_config = Config {
            enabled: false,
            port: 9090,
            bind_address: "127.0.0.1".to_string(),
        };

        let metrics_config = MetricsConfig {
            enabled: true,
            ..Default::default()
        };

        let metrics =
            Arc::new(MetricServiceImpl::new(metrics_config).expect("Failed to create metrics"));

        // Create a minimal MountConfig for testing
        let mount_config = Arc::new(MountConfig {
            filesystem_config: crate::filesystem_service::types::Config::default(),
            metadata_config: crate::metadata_store::Config::default(),
            file_store_config: crate::file_store::types::Config::default(),
            metric_config: Some(MetricsConfig::default()),
            admin_config: Some(admin_config.clone()),
            network_config: None,
            raft_config: None,
            storage_endpoint_config: None,
            mount_point: std::path::PathBuf::from("/tmp/test"),
            mount_options: crate::filesystem_service::mount::MountOptions::default(),
        });

        let server = AdminServer::new(admin_config, mount_config, metrics, None, None);

        let result = server.start();
        assert!(result.is_err());
    }
}
