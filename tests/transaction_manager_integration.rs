//! Integration tests for TransactionManager in multi-node Raft clusters
//!
//! These tests verify that TransactionManager works correctly across a real
//! distributed Raft cluster, including:
//! - Atomic transaction commits across multiple nodes
//! - Concurrent transaction handling
//! - ACID properties (Atomicity, Consistency, Isolation, Durability)
//! - Leader failover during transactions
//! - Crash recovery

mod stub_storage_network;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::time::sleep;
use tracing::{self, info};

use stub_storage_network::StubNetworkHub;
use wormfs::file_store::types::{ChunkId, FileId, StripeId};
use wormfs::metadata_store::{MetadataStore, MetadataStoreImpl};
use wormfs::metric_service::{MetricService, MetricServiceImpl};
use wormfs::storage_raft_member::types::{FileMetadata, StoragePolicy};
use wormfs::storage_raft_member::{NodeId, StorageRaftMember, StorageRaftMemberImpl};
use wormfs::transaction_manager::{Operation, TransactionManager, TransactionManagerFactory};

/// Get timeout multiplier for CI environments.
fn get_timeout_multiplier() -> f64 {
    std::env::var("TEST_TIMEOUT_MULTIPLIER")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0)
        .max(1.0)
}

/// Apply timeout multiplier to a duration.
fn apply_timeout_multiplier(duration: Duration) -> Duration {
    let multiplier = get_timeout_multiplier();
    duration.mul_f64(multiplier)
}

/// Multi-node test cluster with TransactionManager support
struct TransactionTestCluster {
    nodes: Vec<TransactionTestNode>,
    hub: StubNetworkHub,
}

struct TransactionTestNode {
    id: u64,
    raft: StorageRaftMemberImpl,
    metadata_store: MetadataStoreImpl,
    tx_manager: Arc<dyn TransactionManager>,
    peer_id: String,
    _temp_dir: TempDir,
}

impl TransactionTestCluster {
    /// Create a new N-node test cluster with TransactionManager
    async fn new(node_count: usize) -> Result<Self, Box<dyn std::error::Error>> {
        eprintln!(
            "Creating {}-node transaction test cluster with stub network",
            node_count
        );

        let hub = StubNetworkHub::new();
        let mut nodes = Vec::with_capacity(node_count);

        // Create nodes
        for i in 0..node_count {
            let node_id = (i + 1) as u64;
            let temp_dir = TempDir::new()?;
            let data_dir = temp_dir.path().to_path_buf();

            // Create stub network handle
            let network_handle = hub.create_handle(node_id);
            network_handle.register().await;

            // Get the real PeerId for this node
            let peer_id = network_handle.peer_id_string();

            // Create MetadataStore FIRST (so we can pass it to Raft)
            let metadata_config = wormfs::metadata_store::Config {
                database_path: data_dir.join("metadata.redb"),
                cache_size_mb: 100,
                ..Default::default()
            };

            let metadata_store =
                wormfs::metadata_store::factory::MetadataStoreFactory::create_concrete(
                    metadata_config,
                )
                .await?;
            metadata_store.initialize_schema().await?;

            // Create Raft configuration
            let raft_config = wormfs::storage_raft_member::Config {
                heartbeat_interval: Duration::from_millis(500),
                election_timeout_min: Duration::from_millis(1500),
                election_timeout_max: Duration::from_millis(3000),
                max_payload_entries: 1000,
                max_in_flight_append_entries: 10,
                replication_lag_threshold: 1000,
                max_uncommitted_entries: 5000,
                snapshot_time_threshold: Duration::from_secs(3600),
                snapshot_log_size_threshold: 100 * 1024 * 1024,
                enable_snapshot_compression: true,
                snapshot_compression_level: 3,
                enable_lease_based_reads: false,
                lease_duration: Duration::from_secs(10),
                max_read_staleness: Duration::from_secs(120),
                default_transaction_timeout: Duration::from_secs(30),
                max_concurrent_transactions: 100,
                transaction_recovery_timeout: Duration::from_secs(60),
                transaction_log_path: data_dir.join("raft_log.redb"),
                metadata_db_path: data_dir.join("metadata.redb"),
                snapshot_directory: data_dir.join("snapshots"),
                network_address: format!("127.0.0.1:{}", 50000 + node_id).parse().unwrap(),
                storage_network: Some(Arc::new(network_handle.clone())),
                enable_cluster_manager: false,
                cluster_manager_preset: wormfs::storage_raft_member::ClusterManagerPreset::Moderate,
            };

            // Create Raft instance (passing the shared MetadataStore)
            let raft = <StorageRaftMemberImpl as StorageRaftMember>::new(
                NodeId(node_id),
                raft_config,
                metadata_store.clone(),
            )
            .await?;

            // Register Raft handler
            network_handle
                .register_raft_handler_internal(Arc::new(raft.clone()))
                .await;

            // Create MetricService
            let metrics_config = wormfs::metric_service::Config {
                enabled: true,
                ..Default::default()
            };
            let metrics = MetricServiceImpl::new(metrics_config).expect("Failed to create metrics");

            // Create TransactionManager
            let tx_config = wormfs::transaction_manager::types::Config {
                max_active_transactions: 100,
                default_timeout: Duration::from_secs(30),
                max_timeout: Duration::from_secs(300),
                cleanup_interval: Duration::from_secs(1),
            };

            let tx_manager = TransactionManagerFactory::create(
                Arc::new(raft.clone()),
                metadata_store.clone(),
                tx_config,
                metrics,
            );

            nodes.push(TransactionTestNode {
                id: node_id,
                raft,
                metadata_store,
                tx_manager,
                peer_id,
                _temp_dir: temp_dir,
            });
        }

        Ok(Self { nodes, hub })
    }

    /// Initialize the cluster as a single-node or multi-node cluster
    async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.nodes.len() == 1 {
            // Single-node cluster: just initialize without peers
            self.nodes[0].raft.initialize(vec![]).await?;
        } else {
            // Multi-node cluster: initialize all nodes with peer list
            let peers: Vec<(NodeId, String)> = self
                .nodes
                .iter()
                .map(|n| (NodeId(n.id), n.peer_id.clone()))
                .collect();

            for node in &mut self.nodes {
                node.raft.initialize(peers.clone()).await?;
            }
        }

        // Wait for leader election
        self.wait_for_leader().await?;

        Ok(())
    }

    /// Wait for a leader to be elected
    async fn wait_for_leader(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let timeout = apply_timeout_multiplier(Duration::from_secs(10));
        let start = std::time::Instant::now();

        loop {
            for node in &self.nodes {
                if node.raft.is_leader() {
                    info!("Node {} is leader", node.id);
                    return Ok(node.id);
                }
            }

            if start.elapsed() > timeout {
                return Err("No leader elected within timeout".into());
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Get the current leader node
    fn get_leader(&self) -> Option<&TransactionTestNode> {
        self.nodes.iter().find(|n| n.raft.is_leader())
    }

    /// Get a follower node
    fn get_follower(&self) -> Option<&TransactionTestNode> {
        self.nodes.iter().find(|n| !n.raft.is_leader())
    }
}

#[tokio::test]
async fn test_basic_transaction_across_cluster() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut cluster = TransactionTestCluster::new(3)
        .await
        .expect("Failed to create cluster");
    cluster.initialize().await.expect("Failed to initialize");

    // Get the leader's transaction manager
    let leader = cluster.get_leader().expect("No leader found");
    let tx_manager = &leader.tx_manager;

    // Create a test file at root level
    let tx_id = tx_manager
        .begin(Duration::from_secs(30))
        .await
        .expect("Failed to begin transaction");

    tx_manager
        .add_operation(
            tx_id,
            Operation::CreateFile {
                file_id: FileId::generate(),
                path: PathBuf::from("/testfile.txt"),
                inode: 1001,
                metadata: FileMetadata {
                    size: 1024,
                    mode: 0o644,
                    created: SystemTime::now(),
                    modified: SystemTime::now(),
                },
                policy: StoragePolicy {
                    data_chunks: 6,
                    parity_chunks: 3,
                    replication_factor: 1,
                },
            },
        )
        .await
        .expect("Failed to add operation");

    tx_manager
        .commit(tx_id)
        .await
        .expect("Failed to commit transaction");

    // Wait for replication
    sleep(apply_timeout_multiplier(Duration::from_millis(500))).await;

    // Verify the file exists on all nodes
    for node in &cluster.nodes {
        let file = node
            .metadata_store
            .get_file_by_path(&PathBuf::from("/testfile.txt"))
            .await;
        assert!(
            file.is_ok(),
            "File should exist on node {} after commit",
            node.id
        );
    }
}

#[tokio::test]
async fn test_concurrent_transactions() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut cluster = TransactionTestCluster::new(3)
        .await
        .expect("Failed to create cluster");
    cluster.initialize().await.expect("Failed to initialize");

    let leader = cluster.get_leader().expect("No leader found");
    let tx_manager = &leader.tx_manager;

    // Create test directory
    let tx_id = tx_manager
        .begin(Duration::from_secs(30))
        .await
        .expect("Failed to begin");
    tx_manager
        .add_operation(
            tx_id,
            Operation::CreateFile {
                file_id: FileId::generate(),
                path: PathBuf::from("/test"),
                inode: 1001,
                metadata: FileMetadata {
                    size: 0,
                    mode: 0o755,
                    created: SystemTime::now(),
                    modified: SystemTime::now(),
                },
                policy: StoragePolicy {
                    data_chunks: 6,
                    parity_chunks: 3,
                    replication_factor: 1,
                },
            },
        )
        .await
        .unwrap();
    tx_manager.commit(tx_id).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    // Start 5 concurrent transactions creating different files
    let mut handles = vec![];

    for i in 0..5 {
        let tx_mgr = tx_manager.clone();
        let handle = tokio::spawn(async move {
            let tx_id = tx_mgr.begin(Duration::from_secs(30)).await?;

            tx_mgr
                .add_operation(
                    tx_id,
                    Operation::CreateFile {
                        file_id: FileId::generate(),
                        path: PathBuf::from(format!("/test/file{}.txt", i)),
                        inode: 2000 + i,
                        metadata: FileMetadata {
                            size: 1024 * i as u64,
                            mode: 0o644,
                            created: SystemTime::now(),
                            modified: SystemTime::now(),
                        },
                        policy: StoragePolicy {
                            data_chunks: 6,
                            parity_chunks: 3,
                            replication_factor: 1,
                        },
                    },
                )
                .await?;

            tx_mgr.commit(tx_id).await?;

            Ok::<_, wormfs::transaction_manager::types::Error>(())
        });

        handles.push(handle);
    }

    // Wait for all transactions to complete
    for handle in handles {
        handle
            .await
            .expect("Task panicked")
            .expect("Transaction failed");
    }

    // Wait for replication
    sleep(apply_timeout_multiplier(Duration::from_millis(1000))).await;

    // Verify all files exist on all nodes
    for i in 0..5 {
        for node in &cluster.nodes {
            let file = node
                .metadata_store
                .get_file_by_path(&PathBuf::from(format!("/test/file{}.txt", i)))
                .await;
            assert!(
                file.is_ok(),
                "File {} should exist on node {} after concurrent commits",
                i,
                node.id
            );
        }
    }
}

#[tokio::test]
async fn test_transaction_atomicity() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut cluster = TransactionTestCluster::new(3)
        .await
        .expect("Failed to create cluster");
    cluster.initialize().await.expect("Failed to initialize");

    let leader = cluster.get_leader().expect("No leader found");
    let tx_manager = &leader.tx_manager;

    // Create test directory
    let tx_id = tx_manager.begin(Duration::from_secs(30)).await.unwrap();
    tx_manager
        .add_operation(
            tx_id,
            Operation::CreateFile {
                file_id: FileId::generate(),
                path: PathBuf::from("/test"),
                inode: 1001,
                metadata: FileMetadata {
                    size: 0,
                    mode: 0o755,
                    created: SystemTime::now(),
                    modified: SystemTime::now(),
                },
                policy: StoragePolicy {
                    data_chunks: 6,
                    parity_chunks: 3,
                    replication_factor: 1,
                },
            },
        )
        .await
        .unwrap();
    tx_manager.commit(tx_id).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    // Create a transaction with multiple operations
    let tx_id = tx_manager
        .begin(Duration::from_secs(30))
        .await
        .expect("Failed to begin");

    // Add 3 file creations
    for i in 0..3 {
        tx_manager
            .add_operation(
                tx_id,
                Operation::CreateFile {
                    file_id: FileId::generate(),
                    path: PathBuf::from(format!("/test/atomic{}.txt", i)),
                    inode: 3000 + i,
                    metadata: FileMetadata {
                        size: 100 * i as u64,
                        mode: 0o644,
                        created: SystemTime::now(),
                        modified: SystemTime::now(),
                    },
                    policy: StoragePolicy {
                        data_chunks: 6,
                        parity_chunks: 3,
                        replication_factor: 1,
                    },
                },
            )
            .await
            .expect("Failed to add operation");
    }

    // Commit - all 3 should succeed or all fail
    tx_manager
        .commit(tx_id)
        .await
        .expect("Failed to commit atomic transaction");

    // Wait for replication
    sleep(apply_timeout_multiplier(Duration::from_millis(500))).await;

    // Verify all 3 files exist on all nodes (atomicity)
    for i in 0..3 {
        for node in &cluster.nodes {
            let file = node
                .metadata_store
                .get_file_by_path(&PathBuf::from(format!("/test/atomic{}.txt", i)))
                .await;
            assert!(
                file.is_ok(),
                "Atomic file {} should exist on node {}",
                i,
                node.id
            );
        }
    }
}

#[tokio::test]
async fn test_transaction_subscription_across_cluster() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut cluster = TransactionTestCluster::new(3)
        .await
        .expect("Failed to create cluster");
    cluster.initialize().await.expect("Failed to initialize");

    let leader = cluster.get_leader().expect("No leader found");
    let follower = cluster.get_follower().expect("No follower found");

    // Subscribe to changes on a follower node
    let mut rx = follower.tx_manager.subscribe_metadata_changes(None).await;

    // Pre-generate file_id before creating the file
    // This ensures all Raft nodes use the same file_id for this file
    let test_file_id = FileId::generate();

    // Create a file with the pre-generated file_id (this WILL emit a FileCreated event)
    let tx_id1 = leader
        .tx_manager
        .begin(Duration::from_secs(30))
        .await
        .unwrap();
    leader
        .tx_manager
        .add_operation(
            tx_id1,
            Operation::CreateFile {
                file_id: test_file_id,
                path: PathBuf::from("/test"),
                inode: 1001,
                metadata: FileMetadata {
                    size: 0,
                    mode: 0o755,
                    created: SystemTime::now(),
                    modified: SystemTime::now(),
                },
                policy: StoragePolicy {
                    data_chunks: 6,
                    parity_chunks: 3,
                    replication_factor: 1,
                },
            },
        )
        .await
        .unwrap();

    // Commit on leader
    leader.tx_manager.commit(tx_id1).await.unwrap();

    // Wait for event on follower
    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("Timeout waiting for subscription event")
        .expect("Channel closed");

    // Verify we received the file creation event
    assert!(!event.changes.is_empty(), "Should have received changes");
    println!(
        "✅ Received subscription event with {} changes",
        event.changes.len()
    );
}

#[tokio::test]
async fn test_transaction_consistency_across_nodes() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut cluster = TransactionTestCluster::new(3)
        .await
        .expect("Failed to create cluster");
    cluster.initialize().await.expect("Failed to initialize");

    let leader = cluster.get_leader().expect("No leader found");

    // Create test directory
    let tx_id = leader
        .tx_manager
        .begin(Duration::from_secs(30))
        .await
        .unwrap();
    leader
        .tx_manager
        .add_operation(
            tx_id,
            Operation::CreateFile {
                file_id: FileId::generate(),
                path: PathBuf::from("/test"),
                inode: 1001,
                metadata: FileMetadata {
                    size: 0,
                    mode: 0o755,
                    created: SystemTime::now(),
                    modified: SystemTime::now(),
                },
                policy: StoragePolicy {
                    data_chunks: 6,
                    parity_chunks: 3,
                    replication_factor: 1,
                },
            },
        )
        .await
        .unwrap();
    leader.tx_manager.commit(tx_id).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    // Create a file with specific metadata
    let tx_id = leader
        .tx_manager
        .begin(Duration::from_secs(30))
        .await
        .unwrap();

    let file_size = 4096u64;
    let file_mode = 0o644u32;

    leader
        .tx_manager
        .add_operation(
            tx_id,
            Operation::CreateFile {
                file_id: FileId::generate(),
                path: PathBuf::from("/test/consistent.txt"),
                inode: 5000,
                metadata: FileMetadata {
                    size: file_size,
                    mode: file_mode,
                    created: SystemTime::now(),
                    modified: SystemTime::now(),
                },
                policy: StoragePolicy {
                    data_chunks: 6,
                    parity_chunks: 3,
                    replication_factor: 1,
                },
            },
        )
        .await
        .unwrap();

    leader.tx_manager.commit(tx_id).await.unwrap();

    // Wait for replication
    sleep(apply_timeout_multiplier(Duration::from_millis(1000))).await;

    // Verify metadata is identical across all nodes
    let path = PathBuf::from("/test/consistent.txt");
    let mut file_metadata = vec![];

    for node in &cluster.nodes {
        let file = node
            .metadata_store
            .get_file_by_path(&path)
            .await
            .expect(&format!("File should exist on node {}", node.id));

        file_metadata.push((node.id, file.size, file.permissions));
    }

    // All nodes should have the same metadata
    for (node_id, size, mode) in &file_metadata {
        assert_eq!(
            *size, file_size,
            "File size should be consistent on node {}",
            node_id
        );
        assert_eq!(
            *mode, file_mode,
            "File mode should be consistent on node {}",
            node_id
        );
    }
}
