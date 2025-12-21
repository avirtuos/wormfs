//! Integration tests for StorageEndpoint gRPC server.
//!
//! These tests verify the core functionality of the StorageEndpoint server
//! including lifecycle management, configuration, and graceful shutdown.

#![cfg(all(test, feature = "tonic", feature = "test-utils"))]

use std::sync::Arc;
use std::time::Duration;
use wormfs::storage_endpoint::factory::StorageEndpointFactory;
use wormfs::storage_endpoint::types::EndpointConfig;
use wormfs::storage_endpoint::StorageEndpoint;
use wormfs::test_utils::mocks::*;

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

/// Test complete server lifecycle: startup, serving, and shutdown.
#[tokio::test]
async fn test_server_lifecycle() {
    let config = EndpointConfig {
        listen_address: "127.0.0.1:0".parse().unwrap(),
        enable_auth: false,
        enable_tls: false,
        ..Default::default()
    };

    let endpoint = StorageEndpointFactory::create(
        config,
        Arc::new(MockFileSystemService::default()),
        Arc::new(MockFileStore::default()),
        Arc::new(MockSnapshotStore::default()),
        Arc::new(MockTransactionLogStore::default()),
        Arc::new(MockStorageRaftMember::default()),
        Arc::new(MockStorageNode::default()),
        MockMetricService::default(),
    )
    .await
    .expect("Failed to create endpoint");

    // Test: Server should not be serving initially
    assert!(!endpoint.is_serving());

    // Test: Start the server in background
    let endpoint_clone = Arc::new(endpoint);
    let endpoint_serve = endpoint_clone.clone();
    let server_handle = tokio::spawn(async move { endpoint_serve.serve().await });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test: Server should now be serving
    assert!(endpoint_clone.is_serving());

    // Test: Local address should be set
    // Note: When using :0 for port, tonic doesn't expose the OS-assigned port,
    // so we just verify the address is configured
    let addr = endpoint_clone.local_addr();
    assert_eq!(addr.ip().to_string(), "127.0.0.1");

    // Test: Graceful shutdown
    endpoint_clone
        .shutdown(Duration::from_secs(5))
        .await
        .expect("Shutdown failed");

    // Wait for server to stop
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    // Test: Server should no longer be serving
    assert!(!endpoint_clone.is_serving());
}
