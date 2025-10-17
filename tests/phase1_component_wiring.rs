//! Integration tests for Phase 1 Component Wiring (Step 10)
//!
//! These tests verify that all three Phase 1 components can be successfully
//! wired together through the StorageNode orchestrator.

use std::path::PathBuf;
use tempfile::TempDir;
use wormfs::storage_node::{Config, StorageNode, StorageNodeFactory};

/// Test that StorageNode can be initialized with default configuration
#[tokio::test]
async fn test_storage_node_initialization() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = Config {
        node_id: "test-node-init".to_string(),
        data_dir: temp_dir.path().to_path_buf(),
        metadata_db_path: temp_dir.path().join("metadata.db"),
        default_stripe_size: 1024 * 1024, // 1MB
        default_data_shards: 2,
        default_parity_shards: 1,
        default_uid: 1000,
        default_gid: 1000,
        lock_timeout: std::time::Duration::from_secs(30),
        ..Default::default()
    };

    let node = StorageNodeFactory::create_concrete(config)
        .await
        .expect("Failed to create StorageNode");

    // Verify status
    let status = node.get_status();
    assert_eq!(status.node_id, "test-node-init");
    assert!(!status.started);
    assert!(status.components.metadata_store);
    assert!(status.components.file_store);
    assert!(status.components.filesystem_service);
}

/// Test that StorageNode can be started and stopped
#[tokio::test]
async fn test_storage_node_lifecycle() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = Config {
        node_id: "test-node-lifecycle".to_string(),
        data_dir: temp_dir.path().to_path_buf(),
        metadata_db_path: temp_dir.path().join("metadata.db"),
        default_stripe_size: 1024 * 1024,
        default_data_shards: 2,
        default_parity_shards: 1,
        default_uid: 1000,
        default_gid: 1000,
        lock_timeout: std::time::Duration::from_secs(30),
        ..Default::default()
    };

    let mut node = StorageNodeFactory::create_concrete(config)
        .await
        .expect("Failed to create StorageNode");

    // Initial state should be not started
    assert!(!node.get_status().started);

    // Start the node
    node.start().await.expect("Failed to start node");
    assert!(node.get_status().started);

    // Starting again should be idempotent
    node.start().await.expect("Failed to start node again");
    assert!(node.get_status().started);

    // Shutdown the node
    node.shutdown().await.expect("Failed to shutdown node");
    assert!(!node.get_status().started);

    // Shutting down again should be idempotent
    node.shutdown()
        .await
        .expect("Failed to shutdown node again");
    assert!(!node.get_status().started);
}

/// Test that configuration can be loaded from TOML file
#[tokio::test]
async fn test_config_from_toml() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    // Create a test TOML config file
    let config_path = temp_dir.path().join("test_config.toml");
    let toml_content = format!(
        r#"
node_id = "test-node-toml"
listen_address = "127.0.0.1:9000"
data_dir = "{}"
metadata_db_path = "{}/metadata.db"
default_stripe_size = 2097152
default_data_shards = 3
default_parity_shards = 2
default_uid = 2000
default_gid = 2000
lock_timeout = 60
"#,
        temp_dir.path().display(),
        temp_dir.path().display()
    );

    std::fs::write(&config_path, toml_content).expect("Failed to write config file");

    // Load configuration from file
    let config = Config::from_file(&config_path).expect("Failed to load config");

    assert_eq!(config.node_id, "test-node-toml");
    assert_eq!(config.listen_address.port(), 9000);
    assert_eq!(config.default_stripe_size, 2097152);
    assert_eq!(config.default_data_shards, 3);
    assert_eq!(config.default_parity_shards, 2);
    assert_eq!(config.default_uid, 2000);
    assert_eq!(config.default_gid, 2000);
    assert_eq!(config.lock_timeout, std::time::Duration::from_secs(60));
}

/// Test that environment variables override TOML configuration
#[tokio::test]
async fn test_env_overrides() {
    // Set environment variables
    std::env::set_var("WORMFS_NODE_ID", "env-override-node");
    std::env::set_var("WORMFS_DATA_DIR", "/custom/data/dir");
    std::env::set_var("WORMFS_LISTEN_ADDRESS", "192.168.1.100:8000");

    // Start with default config and apply env overrides
    let config = Config::default().with_env_overrides();

    assert_eq!(config.node_id, "env-override-node");
    assert_eq!(config.data_dir, PathBuf::from("/custom/data/dir"));
    assert_eq!(config.listen_address.ip().to_string(), "192.168.1.100");
    assert_eq!(config.listen_address.port(), 8000);

    // Clean up environment variables
    std::env::remove_var("WORMFS_NODE_ID");
    std::env::remove_var("WORMFS_DATA_DIR");
    std::env::remove_var("WORMFS_LISTEN_ADDRESS");
}

/// Test that invalid configuration is rejected
#[tokio::test]
async fn test_invalid_config_rejection() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    // Test 1: Empty node_id
    let config = Config {
        node_id: "".to_string(),
        data_dir: temp_dir.path().to_path_buf(),
        metadata_db_path: temp_dir.path().join("metadata.db"),
        ..Default::default()
    };

    assert!(config.validate().is_err(), "Should reject empty node_id");

    // Test 2: Zero data shards
    let config = Config {
        node_id: "test-node".to_string(),
        data_dir: temp_dir.path().to_path_buf(),
        metadata_db_path: temp_dir.path().join("metadata.db"),
        default_data_shards: 0,
        ..Default::default()
    };

    assert!(config.validate().is_err(), "Should reject zero data shards");

    // Test 3: Zero stripe size
    let config = Config {
        node_id: "test-node".to_string(),
        data_dir: temp_dir.path().to_path_buf(),
        metadata_db_path: temp_dir.path().join("metadata.db"),
        default_stripe_size: 0,
        ..Default::default()
    };

    assert!(config.validate().is_err(), "Should reject zero stripe size");
}

/// Test that cluster info returns correct information for Phase 1 (single node)
#[tokio::test]
async fn test_cluster_info_single_node() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = Config {
        node_id: "test-node-cluster".to_string(),
        data_dir: temp_dir.path().to_path_buf(),
        metadata_db_path: temp_dir.path().join("metadata.db"),
        ..Default::default()
    };

    let node = StorageNodeFactory::create_concrete(config)
        .await
        .expect("Failed to create StorageNode");

    let cluster_info = node
        .get_cluster_info()
        .await
        .expect("Failed to get cluster info");

    assert_eq!(cluster_info.node_count, 1);
    assert!(cluster_info.leader_node.is_none()); // Phase 1: No Raft, so no leader
    assert_eq!(cluster_info.nodes.len(), 1);
    assert_eq!(cluster_info.nodes[0], "test-node-cluster");
}

/// Test that is_leader returns false in Phase 1 (no Raft)
#[tokio::test]
async fn test_is_leader_phase1() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = Config {
        node_id: "test-node-leader".to_string(),
        data_dir: temp_dir.path().to_path_buf(),
        metadata_db_path: temp_dir.path().join("metadata.db"),
        ..Default::default()
    };

    let node = StorageNodeFactory::create_concrete(config)
        .await
        .expect("Failed to create StorageNode");

    // In Phase 1, there's no Raft, so is_leader should always return false
    assert!(!node.is_leader());
}

/// Test that filesystem service is available after initialization
#[tokio::test]
async fn test_filesystem_service_availability() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = Config {
        node_id: "test-node-fs".to_string(),
        data_dir: temp_dir.path().to_path_buf(),
        metadata_db_path: temp_dir.path().join("metadata.db"),
        ..Default::default()
    };

    let node = StorageNodeFactory::create_concrete(config)
        .await
        .expect("Failed to create StorageNode");

    // FileSystemService should be available
    assert!(
        node.filesystem_service().is_some(),
        "FileSystemService should be available"
    );
}
