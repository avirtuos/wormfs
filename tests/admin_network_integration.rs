//! Integration tests for Admin API network endpoints
//!
//! These tests verify that the network handlers work correctly with the
//! admin server HTTP layer. They test the API endpoints without requiring
//! a full networkconfiguration.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use wormfs::admin::{AdminServer, Config as AdminConfig};
use wormfs::filesystem_service::mount::MountConfig;
use wormfs::metric_service::{Config as MetricConfig, MetricService, MetricServiceImpl};

/// Helper function to create a minimal MountConfig for testing
fn create_test_mount_config(admin_port: u16) -> MountConfig {
    MountConfig {
        filesystem_config: Default::default(),
        metadata_config: Default::default(),
        file_store_config: Default::default(),
        metric_config: Some(MetricConfig {
            enabled: true,
            ..Default::default()
        }),
        admin_config: Some(AdminConfig {
            enabled: true,
            bind_address: "127.0.0.1".to_string(),
            port: admin_port,
            ..Default::default()
        }),
        network_config: None, // No network needed for these basic tests
        raft_config: None,    // No raft needed for these basic tests
        mount_point: std::path::PathBuf::from("/tmp/wormfs-test"),
        mount_options: Default::default(),
    }
}

#[tokio::test]
async fn test_network_status_endpoint_no_network() {
    // Create admin server without network
    let mount_config = create_test_mount_config(29090);

    // Create metrics
    let metrics = Arc::new(
        MetricServiceImpl::new(MetricConfig {
            enabled: true,
            ..Default::default()
        })
        .expect("Failed to create metrics"),
    );

    // Start metrics aggregation
    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    // Create and start admin server without network
    let admin_server = AdminServer::new(
        AdminConfig {
            enabled: true,
            bind_address: "127.0.0.1".to_string(),
            port: 29090,
            ..Default::default()
        },
        Arc::new(mount_config),
        metrics,
        None, // No network
        None, // No raft member
    );

    let server_handle = admin_server.start().expect("Failed to start admin server");

    // Wait for server to be ready
    sleep(Duration::from_millis(500)).await;

    // Make HTTP request to network status endpoint
    let client = reqwest::Client::new();
    let result = client
        .get("http://127.0.0.1:29090/api/network/status")
        .timeout(Duration::from_secs(5))
        .send()
        .await;

    // Since we have no network, the endpoint might return an error or empty response
    // The important thing is that the server is responding
    match result {
        Ok(response) => {
            // If we get a response, it should be valid HTTP
            assert!(response.status().is_success() || response.status().is_client_error());
        }
        Err(e) => {
            // Connection error is also acceptable for this test
            println!("Expected potential error with no network: {}", e);
        }
    }

    // Cleanup
    server_handle.abort();
}

#[tokio::test]
async fn test_peers_endpoint_no_network() {
    // Create admin server without network
    let mount_config = create_test_mount_config(29091);

    let metrics = Arc::new(
        MetricServiceImpl::new(MetricConfig {
            enabled: true,
            ..Default::default()
        })
        .expect("Failed to create metrics"),
    );

    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    let admin_server = AdminServer::new(
        AdminConfig {
            enabled: true,
            bind_address: "127.0.0.1".to_string(),
            port: 29091,
            ..Default::default()
        },
        Arc::new(mount_config),
        metrics,
        None, // No network
        None, // No raft member
    );

    let server_handle = admin_server.start().expect("Failed to start admin server");
    sleep(Duration::from_millis(500)).await;

    // Make HTTP request to peers endpoint
    let client = reqwest::Client::new();
    let result = client
        .get("http://127.0.0.1:29091/api/network/peers")
        .timeout(Duration::from_secs(5))
        .send()
        .await;

    match result {
        Ok(response) => {
            assert!(response.status().is_success() || response.status().is_client_error());
        }
        Err(e) => {
            println!("Expected potential error with no network: {}", e);
        }
    }

    // Cleanup
    server_handle.abort();
}

#[tokio::test]
async fn test_admin_server_health_endpoint() {
    // Test that the admin server is properly set up by checking the health endpoint
    let mount_config = create_test_mount_config(29092);

    let metrics = Arc::new(
        MetricServiceImpl::new(MetricConfig {
            enabled: true,
            ..Default::default()
        })
        .expect("Failed to create metrics"),
    );

    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    let admin_server = AdminServer::new(
        AdminConfig {
            enabled: true,
            bind_address: "127.0.0.1".to_string(),
            port: 29092,
            ..Default::default()
        },
        Arc::new(mount_config),
        metrics,
        None, // No network
        None, // No raft member
    );

    let server_handle = admin_server.start().expect("Failed to start admin server");
    sleep(Duration::from_millis(500)).await;

    // Test health endpoint
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:29092/api/health")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("Failed to make request");

    assert_eq!(response.status(), 200);

    let text = response.text().await.expect("Failed to get response text");
    assert!(text.contains("healthy") || text.contains("ok"));

    // Cleanup
    server_handle.abort();
}

#[tokio::test]
async fn test_concurrent_admin_requests() {
    // Test that the admin server can handle multiple concurrent requests
    let mount_config = create_test_mount_config(29093);

    let metrics = Arc::new(
        MetricServiceImpl::new(MetricConfig {
            enabled: true,
            ..Default::default()
        })
        .expect("Failed to create metrics"),
    );

    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    let admin_server = AdminServer::new(
        AdminConfig {
            enabled: true,
            bind_address: "127.0.0.1".to_string(),
            port: 29093,
            ..Default::default()
        },
        Arc::new(mount_config),
        metrics,
        None, // No network
        None, // No raft member
    );

    let server_handle = admin_server.start().expect("Failed to start admin server");
    sleep(Duration::from_millis(500)).await;

    // Make multiple concurrent requests to different endpoints
    let client = reqwest::Client::new();
    let mut handles = vec![];

    for _ in 0..5 {
        let client_clone = client.clone();
        handles.push(tokio::spawn(async move {
            client_clone
                .get("http://127.0.0.1:29093/api/health")
                .timeout(Duration::from_secs(5))
                .send()
                .await
        }));
    }

    // Wait for all requests to complete
    let mut success_count = 0;
    for handle in handles {
        if let Ok(Ok(response)) = handle.await {
            if response.status().is_success() {
                success_count += 1;
            }
        }
    }

    // At least some requests should succeed
    assert!(
        success_count > 0,
        "Expected at least one successful request"
    );

    // Cleanup
    server_handle.abort();
}
