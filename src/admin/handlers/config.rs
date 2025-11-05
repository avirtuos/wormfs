//! Configuration handler for admin API endpoints.
//!
//! Provides handlers for configuration viewing and management using the
//! ConfigRegistry system for automatic config discovery.

use crate::admin::config_provider::ConfigRegistry;
use crate::admin::config_providers::*;
use crate::filesystem_service::mount::MountConfig;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use std::sync::Arc;

/// Handler for `/api/config` endpoint.
///
/// Returns the current system configuration in JSON format with descriptions for each field.
/// Uses the ConfigRegistry system to automatically include all component configs.
pub async fn config_handler(State(mount_config): State<Arc<MountConfig>>) -> impl IntoResponse {
    // Create a config registry and register all component configs
    let mut registry = ConfigRegistry::new();

    // Register Admin config
    registry.register(AdminConfigProvider {
        config: mount_config.admin_config.clone(),
    });

    // Register Metrics config
    registry.register(MetricsConfigProvider {
        config: mount_config.metric_config.clone(),
    });

    // Register Metadata config
    registry.register(MetadataConfigProvider {
        config: mount_config.metadata_config.clone(),
    });

    // Register FileStore config
    registry.register(FileStoreConfigProvider {
        config: mount_config.file_store_config.clone(),
    });

    // Register Filesystem config
    registry.register(FilesystemConfigProvider {
        config: mount_config.filesystem_config.clone(),
    });

    // Register BufferedFileHandle config
    registry.register(BufferedFileHandleConfigProvider {
        config: mount_config
            .filesystem_config
            .buffered_file_handle_config
            .clone(),
    });

    // Register StorageNetwork config (this is the new one!)
    registry.register(StorageNetworkConfigProvider::new(
        mount_config.network_config.clone(),
    ));

    // Register Mount options
    registry.register(MountConfigProvider {
        mount_point: mount_config.mount_point.clone(),
        options: mount_config.mount_options.clone(),
    });

    // Get all configs as JSON
    let config_json = registry.get_all_configs();

    (StatusCode::OK, Json(config_json))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;

    #[tokio::test]
    async fn test_config_handler() {
        // Create a test MountConfig
        let mount_config = Arc::new(MountConfig {
            filesystem_config: crate::filesystem_service::types::Config::default(),
            metadata_config: crate::metadata_store::Config::default(),
            file_store_config: crate::file_store::types::Config::default(),
            metric_config: Some(crate::metric_service::Config::default()),
            admin_config: Some(crate::admin::Config::default()),
            network_config: None,
            raft_config: None,
            mount_point: std::path::PathBuf::from("/tmp/test"),
            mount_options: crate::filesystem_service::mount::MountOptions::default(),
        });

        let response = config_handler(State(mount_config)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_config_handler_with_network() {
        // Create a test MountConfig with network config
        let mount_config = Arc::new(MountConfig {
            filesystem_config: crate::filesystem_service::types::Config::default(),
            metadata_config: crate::metadata_store::Config::default(),
            file_store_config: crate::file_store::types::Config::default(),
            metric_config: Some(crate::metric_service::Config::default()),
            admin_config: Some(crate::admin::Config::default()),
            network_config: Some(crate::storage_network::Config {
                node_id: "test-node".to_string(),
                listen_addresses: vec!["/ip4/0.0.0.0/tcp/7100".to_string()],
                peers: vec![],
                peer_id_store_path: std::path::PathBuf::from("/tmp/peer_ids.json"),
                max_peers: 100,
                max_connections_per_peer: 3,
                connection_timeout: std::time::Duration::from_secs(30),
                idle_connection_timeout: std::time::Duration::from_secs(600),
                keep_alive_interval: std::time::Duration::from_secs(30),
                admin_url: Some("http://127.0.0.1:9090".to_string()),
            }),
            raft_config: None,
            mount_point: std::path::PathBuf::from("/tmp/test"),
            mount_options: crate::filesystem_service::mount::MountOptions::default(),
        });

        let response = config_handler(State(mount_config)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
