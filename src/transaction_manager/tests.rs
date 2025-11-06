//! Unit tests for TransactionManager.

#[cfg(test)]
mod tests {
    use super::super::implementation::TransactionManagerImpl;
    use super::super::types::{Config, Error, Operation};
    use super::super::TransactionManager;
    use crate::file_store::types::StripeId;
    use crate::metadata_store::factory::MetadataStoreFactory;
    use crate::metadata_store::MetadataStoreImpl;
    use crate::metric_service::{Config as MetricConfig, MetricService, MetricServiceImpl};
    use crate::storage_raft_member::types::{
        FileId, FileMetadata, NodeId, StoragePolicy, TxId, WormFsOperation,
    };
    use crate::storage_raft_member::{Error as RaftError, StorageRaftMember};
    use async_trait::async_trait;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};
    use tokio::sync::{Mutex, RwLock};

    /// Mock Raft member for testing
    struct MockRaftMember {
        proposed_operations: Arc<Mutex<Vec<WormFsOperation>>>,
        should_fail: Arc<RwLock<bool>>,
        operation_count: Arc<AtomicU64>,
        is_leader: Arc<AtomicBool>,
    }

    impl MockRaftMember {
        fn new() -> Self {
            Self {
                proposed_operations: Arc::new(Mutex::new(Vec::new())),
                should_fail: Arc::new(RwLock::new(false)),
                operation_count: Arc::new(AtomicU64::new(0)),
                is_leader: Arc::new(AtomicBool::new(true)),
            }
        }

        async fn get_proposed_operations(&self) -> Vec<WormFsOperation> {
            self.proposed_operations.lock().await.clone()
        }

        async fn set_should_fail(&self, should_fail: bool) {
            *self.should_fail.write().await = should_fail;
        }
    }

    #[async_trait]
    impl StorageRaftMember for MockRaftMember {
        type Operation = WormFsOperation;
        type OperationResult = ();

        async fn new(
            _node_id: NodeId,
            _config: crate::storage_raft_member::Config,
        ) -> Result<Self, RaftError>
        where
            Self: Sized,
        {
            Ok(Self::new())
        }

        async fn initialize(&mut self, _peers: Vec<NodeId>) -> Result<(), RaftError> {
            Ok(())
        }

        async fn propose_operation(
            &self,
            operation: Self::Operation,
        ) -> Result<Self::OperationResult, RaftError> {
            self.operation_count.fetch_add(1, Ordering::SeqCst);

            if *self.should_fail.read().await {
                return Err(RaftError::NotLeader { leader: None });
            }

            self.proposed_operations.lock().await.push(operation);
            Ok(())
        }

        fn is_leader(&self) -> bool {
            self.is_leader.load(Ordering::SeqCst)
        }

        fn get_metrics(&self) -> crate::storage_raft_member::types::RaftMetrics {
            use crate::storage_raft_member::types::{RaftMetrics, RaftRole};
            use std::collections::HashMap;
            use std::time::Instant;

            RaftMetrics {
                current_term: 0,
                role: RaftRole::Follower,
                leader_id: None,
                commit_index: 0,
                last_applied: 0,
                last_log_index: 0,
                snapshot_index: 0,
                cluster_size: 1,
                cluster_members: vec![],
                replication_lag: HashMap::new(),
                heartbeat_sent: HashMap::new(),
                heartbeat_acked: HashMap::new(),
            }
        }

        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            Ok(())
        }

        async fn trigger_election(&self) -> Result<(), RaftError> {
            Ok(())
        }

        async fn add_node(
            &self,
            _node_id: NodeId,
            _address: std::net::SocketAddr,
            _peer_id: String,
        ) -> Result<(), RaftError> {
            Ok(())
        }

        async fn remove_node(&self, _node_id: NodeId) -> Result<(), RaftError> {
            Ok(())
        }

        async fn step_down(&self) -> Result<(), RaftError> {
            Ok(())
        }

        async fn subscribe_metadata_changes(
            &self,
            _filter: Option<Vec<crate::storage_raft_member::types::MetadataChangeType>>,
        ) -> tokio::sync::mpsc::UnboundedReceiver<
            crate::storage_raft_member::types::MetadataChangeEvent,
        > {
            let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
            rx
        }

        async fn handle_raft_rpc(&self, _request: Vec<u8>) -> Result<Vec<u8>, RaftError> {
            Ok(vec![])
        }
    }

    /// Helper to create a test metadata store
    async fn create_test_metadata_store() -> MetadataStoreImpl {
        use crate::metadata_store::types::FileMetadata as MdFileMetadata;
        use crate::metadata_store::MetadataStore;
        use std::time::SystemTime;

        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_metadata.db");

        let config = crate::metadata_store::Config {
            database_path: db_path.clone(),
            ..Default::default()
        };

        let store = MetadataStoreFactory::create_concrete(config)
            .await
            .expect("Failed to create metadata store");

        // Initialize the schema (creates root directory)
        store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Create a test directory for our files
        let now = SystemTime::now();
        store
            .create_file(
                FileId::generate(),
                &PathBuf::from("/test"),
                1001, // inode for test directory
                MdFileMetadata {
                    file_type: crate::metadata_store::types::FileType::Directory,
                    size: 0,
                    permissions: 0o755,
                    uid: 1000,
                    gid: 1000,
                    created_at: now,
                    modified_at: now,
                    accessed_at: now,
                    target: None,
                },
            )
            .await
            .expect("Failed to create /test directory");

        store
    }

    /// Helper to create a test transaction manager
    async fn create_test_transaction_manager() -> (
        Arc<TransactionManagerImpl>,
        Arc<MockRaftMember>,
        MetadataStoreImpl,
        MetricServiceImpl,
    ) {
        let raft_member = Arc::new(MockRaftMember::new());
        let metadata_store = create_test_metadata_store().await;
        let metrics = <MetricServiceImpl as MetricService>::new(MetricConfig::default())
            .expect("Failed to create metrics");

        let config = Config {
            max_active_transactions: 100,
            default_timeout: Duration::from_secs(30),
            max_timeout: Duration::from_secs(300),
            cleanup_interval: Duration::from_secs(60),
        };

        let tx_manager = TransactionManagerImpl::new(
            raft_member.clone(),
            metadata_store.clone(),
            config,
            metrics.clone(),
        );

        (tx_manager, raft_member, metadata_store, metrics)
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        let (tx_manager, _, _, _) = create_test_transaction_manager().await;

        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        assert!(tx_id.0 > 0, "Transaction ID should be non-zero");
    }

    #[tokio::test]
    async fn test_begin_multiple_transactions() {
        let (tx_manager, _, _, _) = create_test_transaction_manager().await;

        let tx_id1 = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction 1");

        let tx_id2 = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction 2");

        assert_ne!(tx_id1, tx_id2, "Transaction IDs should be unique");
    }

    #[tokio::test]
    async fn test_add_operation_to_transaction() {
        let (tx_manager, _, _, _) = create_test_transaction_manager().await;

        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        let operation = Operation::CreateFile {
            path: PathBuf::from("/test/file.txt"),
            inode: 12345,
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
        };

        tx_manager
            .add_operation(tx_id, operation)
            .await
            .expect("Failed to add operation");
    }

    #[tokio::test]
    async fn test_add_multiple_operations() {
        let (tx_manager, _, _, _) = create_test_transaction_manager().await;

        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        // Add first operation
        let op1 = Operation::CreateFile {
            path: PathBuf::from("/test/file1.txt"),
            inode: 12345,
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
        };

        tx_manager
            .add_operation(tx_id, op1)
            .await
            .expect("Failed to add operation 1");

        // Add second operation
        let op2 = Operation::CreateFile {
            path: PathBuf::from("/test/file2.txt"),
            inode: 12346,
            metadata: FileMetadata {
                size: 2048,
                mode: 0o644,
                created: SystemTime::now(),
                modified: SystemTime::now(),
            },
            policy: StoragePolicy {
                data_chunks: 6,
                parity_chunks: 3,
                replication_factor: 1,
            },
        };

        tx_manager
            .add_operation(tx_id, op2)
            .await
            .expect("Failed to add operation 2");
    }

    #[tokio::test]
    async fn test_commit_transaction() {
        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        let operation = Operation::CreateFile {
            path: PathBuf::from("/test/file.txt"),
            inode: 12345,
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
        };

        tx_manager
            .add_operation(tx_id, operation)
            .await
            .expect("Failed to add operation");

        tx_manager
            .commit(tx_id)
            .await
            .expect("Failed to commit transaction");

        // Verify operation was proposed to Raft
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(
            proposed_ops.len(),
            1,
            "Should have proposed 1 operation to Raft"
        );

        // Verify it's an AtomicTransaction
        match &proposed_ops[0] {
            WormFsOperation::AtomicTransaction {
                tx_id: op_tx_id,
                operations,
                ..
            } => {
                assert_eq!(*op_tx_id, tx_id, "Transaction ID should match");
                assert_eq!(operations.len(), 1, "Should have 1 operation");
            }
            _ => panic!("Expected AtomicTransaction operation"),
        }
    }

    #[tokio::test]
    async fn test_abort_transaction() {
        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        let operation = Operation::CreateFile {
            path: PathBuf::from("/test/file.txt"),
            inode: 12345,
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
        };

        tx_manager
            .add_operation(tx_id, operation)
            .await
            .expect("Failed to add operation");

        tx_manager
            .abort(tx_id)
            .await
            .expect("Failed to abort transaction");

        // Verify no operations were proposed to Raft
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(
            proposed_ops.len(),
            0,
            "Should not have proposed any operations to Raft"
        );

        // Verify transaction is removed
        assert_eq!(
            tx_manager.active_count().await,
            0,
            "Should have no active transactions"
        );
    }

    #[tokio::test]
    async fn test_add_operation_to_nonexistent_transaction() {
        let (tx_manager, _, _, _) = create_test_transaction_manager().await;

        let fake_tx_id = TxId(99999);
        let operation = Operation::CreateFile {
            path: PathBuf::from("/test/file.txt"),
            inode: 12345,
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
        };

        let result = tx_manager.add_operation(fake_tx_id, operation).await;
        assert!(
            result.is_err(),
            "Should fail to add operation to nonexistent transaction"
        );

        match result.unwrap_err() {
            Error::TransactionNotFound(_) => {}
            e => panic!("Expected TransactionNotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_commit_nonexistent_transaction() {
        let (tx_manager, _, _, _) = create_test_transaction_manager().await;

        let fake_tx_id = TxId(99999);
        let result = tx_manager.commit(fake_tx_id).await;

        assert!(
            result.is_err(),
            "Should fail to commit nonexistent transaction"
        );

        match result.unwrap_err() {
            Error::TransactionNotFound(_) => {}
            e => panic!("Expected TransactionNotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_commit_empty_transaction() {
        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        // Commit without adding any operations
        let result = tx_manager.commit(tx_id).await;

        assert!(result.is_err(), "Should fail to commit empty transaction");

        match result.unwrap_err() {
            Error::EmptyTransaction(_) => {}
            e => panic!("Expected EmptyTransaction error, got: {:?}", e),
        }

        // Verify no operations were proposed
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(
            proposed_ops.len(),
            0,
            "Should not have proposed any operations"
        );
    }

    #[tokio::test]
    async fn test_active_count() {
        let (tx_manager, _, _, _) = create_test_transaction_manager().await;

        assert_eq!(
            tx_manager.active_count().await,
            0,
            "Should start with 0 active transactions"
        );

        let _tx_id1 = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction 1");

        assert_eq!(
            tx_manager.active_count().await,
            1,
            "Should have 1 active transaction"
        );

        let _tx_id2 = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction 2");

        assert_eq!(
            tx_manager.active_count().await,
            2,
            "Should have 2 active transactions"
        );
    }

    #[tokio::test]
    async fn test_raft_failure_during_commit() {
        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        let operation = Operation::CreateFile {
            path: PathBuf::from("/test/file.txt"),
            inode: 12345,
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
        };

        tx_manager
            .add_operation(tx_id, operation)
            .await
            .expect("Failed to add operation");

        // Make Raft fail
        raft_member.set_should_fail(true).await;

        let result = tx_manager.commit(tx_id).await;
        assert!(result.is_err(), "Commit should fail when Raft fails");

        match result.unwrap_err() {
            Error::RaftError(_) => {}
            e => panic!("Expected RaftError, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_concurrent_transactions() {
        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        let tx_manager = Arc::new(tx_manager);
        let mut handles = vec![];

        // Start 10 concurrent transactions
        for i in 0..10 {
            let tx_manager_clone = tx_manager.clone();
            let handle = tokio::spawn(async move {
                let tx_id = tx_manager_clone
                    .begin(Duration::from_secs(30))
                    .await
                    .expect("Failed to begin transaction");

                let operation = Operation::CreateFile {
                    path: PathBuf::from(format!("/test/file{}.txt", i)),
                    inode: 12345 + i as u64,
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
                };

                tx_manager_clone
                    .add_operation(tx_id, operation)
                    .await
                    .expect("Failed to add operation");

                tx_manager_clone
                    .commit(tx_id)
                    .await
                    .expect("Failed to commit transaction");
            });

            handles.push(handle);
        }

        // Wait for all transactions to complete
        for handle in handles {
            handle.await.expect("Transaction task failed");
        }

        // Verify all operations were proposed
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(proposed_ops.len(), 10, "Should have proposed 10 operations");

        // Verify no active transactions remain
        assert_eq!(
            tx_manager.active_count().await,
            0,
            "Should have no active transactions after all commits"
        );
    }

    #[tokio::test]
    async fn test_transaction_with_multiple_creates() {
        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        // Add first CreateFile operation
        tx_manager
            .add_operation(
                tx_id,
                Operation::CreateFile {
                    path: PathBuf::from("/test/file1.txt"),
                    inode: 12345,
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
            .expect("Failed to add CreateFile operation 1");

        // Add second CreateFile operation
        tx_manager
            .add_operation(
                tx_id,
                Operation::CreateFile {
                    path: PathBuf::from("/test/file2.txt"),
                    inode: 12346,
                    metadata: FileMetadata {
                        size: 2048,
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
            .expect("Failed to add CreateFile operation 2");

        // Add third CreateFile operation
        tx_manager
            .add_operation(
                tx_id,
                Operation::CreateFile {
                    path: PathBuf::from("/test/file3.txt"),
                    inode: 12347,
                    metadata: FileMetadata {
                        size: 4096,
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
            .expect("Failed to add CreateFile operation 3");

        tx_manager
            .commit(tx_id)
            .await
            .expect("Failed to commit transaction");

        // Verify all operations were batched into a single AtomicTransaction
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(
            proposed_ops.len(),
            1,
            "Should have proposed 1 AtomicTransaction"
        );

        match &proposed_ops[0] {
            WormFsOperation::AtomicTransaction { operations, .. } => {
                assert_eq!(operations.len(), 3, "Should have batched all 3 operations");
            }
            _ => panic!("Expected AtomicTransaction operation"),
        }
    }
}
