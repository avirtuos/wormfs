//! Integration tests for StorageEndpoint gRPC server.
//!
//! These tests verify the core functionality of the StorageEndpoint server
//! including lifecycle management, configuration, and graceful shutdown.

#![cfg(all(test, feature = "tonic", feature = "test-utils"))]

use wormfs::storage_endpoint::types::EndpointConfig;

// Note: Full integration tests require MockStorageNode, MockStorageRaftMember,
// and MockTransactionLogStore which are not yet exported. These tests will be
// expanded once those mocks are available.

/// Test endpoint configuration validation.
#[test]
fn test_endpoint_config_default() {
    let config = EndpointConfig::default();
    assert_eq!(config.listen_address.port(), 7000);
    assert!(config.enable_auth);
    assert!(config.enable_tls);
}

/// Test endpoint configuration with custom values.
#[test]
fn test_endpoint_config_custom() {
    let config = EndpointConfig {
        listen_address: "127.0.0.1:8080".parse().unwrap(),
        enable_auth: false,
        enable_tls: false,
        rate_limit_per_client: Some(50),
        rate_limit_overall: Some(500),
        rate_limit_burst_size: 75,
        ..Default::default()
    };

    assert_eq!(config.listen_address.port(), 8080);
    assert!(!config.enable_auth);
    assert!(!config.enable_tls);
    assert_eq!(config.rate_limit_per_client, Some(50));
}

// Placeholder for full server lifecycle test
// Will be implemented once all required mocks are exported
/*
#[tokio::test]
async fn test_server_lifecycle() {
    let config = EndpointConfig {
        listen_address: "127.0.0.1:0".parse().unwrap(),
        enable_auth: false,
        ..Default::default()
    };

    let endpoint = StorageEndpointFactory::create(
        config,
        Arc::new(MockFileSystemService::new()),
        Arc::new(MockFileStore::new()),
        Arc::new(MockSnapshotStore::new()),
        Arc::new(MockTransactionLogStore::new()),
        Arc::new(MockStorageRaftMember::new()),
        Arc::new(MockStorageNode::new()),
        MockMetricService::new(),
    )
    .await
    .expect("Failed to create endpoint");

    ...
}
*/
