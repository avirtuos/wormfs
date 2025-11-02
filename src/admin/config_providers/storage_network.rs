//! StorageNetwork configuration provider.

use crate::admin::config_provider::{ConfigProvider, ConfigWithDescriptions};
use crate::storage_network::Config;
use serde_json::json;
use std::collections::HashMap;

/// Configuration provider for StorageNetwork component.
pub struct StorageNetworkConfigProvider {
    config: Option<Config>,
}

impl StorageNetworkConfigProvider {
    /// Create a new StorageNetwork config provider
    pub fn new(config: Option<Config>) -> Self {
        Self { config }
    }
}

impl ConfigProvider for StorageNetworkConfigProvider {
    fn name(&self) -> &'static str {
        "network"
    }

    fn get_config_with_descriptions(&self) -> ConfigWithDescriptions {
        let values = if let Some(ref config) = self.config {
            json!({
                "node_id": config.node_id,
                "listen_addresses": config.listen_addresses,
                "peer_id_store_path": config.peer_id_store_path.display().to_string(),
                "max_peers": config.max_peers,
                "max_connections_per_peer": config.max_connections_per_peer,
                "connection_timeout": config.connection_timeout.as_secs(),
                "idle_connection_timeout": config.idle_connection_timeout.as_secs(),
                "keep_alive_interval": config.keep_alive_interval.as_secs(),
                "admin_url": config.admin_url,
                "peers_count": config.peers.len(),
            })
        } else {
            json!({
                "enabled": false,
            })
        };

        let mut descriptions = HashMap::new();
        descriptions.insert(
            "node_id".to_string(),
            "Unique identifier for this node in the distributed network".to_string(),
        );
        descriptions.insert(
            "listen_addresses".to_string(),
            "libp2p listen addresses for incoming peer connections (multiaddr format)".to_string(),
        );
        descriptions.insert(
            "peer_id_store_path".to_string(),
            "Path to store discovered peer IDs (for auto-ID mode)".to_string(),
        );
        descriptions.insert(
            "max_peers".to_string(),
            "Maximum number of peers to maintain connections with".to_string(),
        );
        descriptions.insert(
            "max_connections_per_peer".to_string(),
            "Maximum concurrent connections per peer".to_string(),
        );
        descriptions.insert(
            "connection_timeout".to_string(),
            "Timeout for establishing new peer connections (seconds)".to_string(),
        );
        descriptions.insert(
            "idle_connection_timeout".to_string(),
            "Timeout for idle peer connections before cleanup (seconds)".to_string(),
        );
        descriptions.insert(
            "keep_alive_interval".to_string(),
            "Interval for sending keep-alive heartbeat messages (seconds)".to_string(),
        );
        descriptions.insert(
            "admin_url".to_string(),
            "Admin UI URL advertised to peers via heartbeat messages".to_string(),
        );
        descriptions.insert(
            "peers_count".to_string(),
            "Number of configured peers for initial connections".to_string(),
        );

        ConfigWithDescriptions::new(values, descriptions)
    }
}
