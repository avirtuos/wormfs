//! Unit tests for TransactionManager.

#[cfg(test)]
mod tests {
    use super::super::implementation::TransactionManagerImpl;
    use super::super::types::{Config, Error, Operation};
    use super::super::TransactionManager;
    use crate::file_store::types::StripeId;
    use crate::metadata_store::factory::MetadataStoreFactory;
    use crate::metadata_store::{MetadataStore, MetadataStoreImpl};
    use crate::metric_service::{Config as MetricConfig, MetricService, MetricServiceImpl};
    use crate::storage_raft_member::types::{
        FileId, FileMetadata, MetadataOperation, NodeId, StoragePolicy, TxId, WormFsOperation,
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
        event_sender: Arc<
            Mutex<
                Vec<
                    tokio::sync::mpsc::UnboundedSender<
                        crate::storage_raft_member::types::MetadataChangeEvent,
                    >,
                >,
            >,
        >,
    }

    impl MockRaftMember {
        fn new() -> Self {
            Self {
                proposed_operations: Arc::new(Mutex::new(Vec::new())),
                should_fail: Arc::new(RwLock::new(false)),
                operation_count: Arc::new(AtomicU64::new(0)),
                is_leader: Arc::new(AtomicBool::new(true)),
                event_sender: Arc::new(Mutex::new(Vec::new())),
            }
        }

        async fn get_proposed_operations(&self) -> Vec<WormFsOperation> {
            self.proposed_operations.lock().await.clone()
        }

        async fn set_should_fail(&self, should_fail: bool) {
            *self.should_fail.write().await = should_fail;
        }

        /// Send a test event to all subscribers
        async fn send_test_event(
            &self,
            event: crate::storage_raft_member::types::MetadataChangeEvent,
        ) {
            let senders = self.event_sender.lock().await;
            for sender in senders.iter() {
                let _ = sender.send(event.clone());
            }
        }
    }

    #[async_trait]
    impl StorageRaftMember for MockRaftMember {
        type Operation = WormFsOperation;
        type OperationResult = ();

        async fn new(
            _node_id: NodeId,
            _config: crate::storage_raft_member::Config,
            _metadata_store: crate::metadata_store::MetadataStoreImpl,
        ) -> Result<Self, RaftError>
        where
            Self: Sized,
        {
            Ok(Self::new())
        }

        async fn initialize(&mut self, _peers: Vec<(NodeId, String)>) -> Result<(), RaftError> {
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
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            self.event_sender.lock().await.push(tx);
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
            prepare_timeout_secs: 30,
            lock_timeout_secs: 10,
            deadlock_detection_interval_ms: 100,
            enable_subscriptions: true,
            max_subscribers: 100,
            cleanup_interval_secs: 60,
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
            file_id: FileId::generate(),
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
            file_id: FileId::generate(),
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
            file_id: FileId::generate(),
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
            file_id: FileId::generate(),
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
            file_id: FileId::generate(),
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

        let fake_tx_id = TxId::new(99999);
        let operation = Operation::CreateFile {
            file_id: FileId::generate(),
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

        let fake_tx_id = TxId::new(99999);
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
            file_id: FileId::generate(),
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
                    file_id: FileId::generate(),
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
                    file_id: FileId::generate(),
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
                    file_id: FileId::generate(),
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
                    file_id: FileId::generate(),
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

    #[tokio::test]
    async fn test_acquire_read_lock() {
        let (tx_manager, raft_member, metadata_store, _) = create_test_transaction_manager().await;

        // First create a file to lock
        let file_id = FileId::generate();
        metadata_store
            .create_file(
                file_id,
                &PathBuf::from("/test/lockfile.txt"),
                20001,
                crate::metadata_store::types::FileMetadata {
                    file_type: crate::metadata_store::types::FileType::RegularFile,
                    size: 0,
                    permissions: 0o644,
                    uid: 1000,
                    gid: 1000,
                    created_at: SystemTime::now(),
                    modified_at: SystemTime::now(),
                    accessed_at: SystemTime::now(),
                    target: None,
                },
            )
            .await
            .expect("Failed to create file");

        // Begin transaction
        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        // Add read lock operation
        let expires_at = SystemTime::now() + Duration::from_secs(60);
        let operation = Operation::AcquireReadLock {
            file_id,
            client_id: 1001,
            expires_at,
        };

        tx_manager
            .add_operation(tx_id, operation)
            .await
            .expect("Failed to add read lock operation");

        // Commit transaction
        tx_manager
            .commit(tx_id)
            .await
            .expect("Failed to commit transaction");

        // Verify lock operation was proposed
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(proposed_ops.len(), 1);

        match &proposed_ops[0] {
            WormFsOperation::AtomicTransaction { operations, .. } => {
                assert_eq!(operations.len(), 1);
                match &operations[0] {
                    MetadataOperation::AcquireReadLock {
                        file_id: fid,
                        client_id,
                        ..
                    } => {
                        assert_eq!(*fid, file_id);
                        assert_eq!(*client_id, 1001);
                    }
                    _ => panic!("Expected AcquireReadLock operation"),
                }
            }
            _ => panic!("Expected AtomicTransaction operation"),
        }
    }

    #[tokio::test]
    async fn test_acquire_write_lock() {
        let (tx_manager, raft_member, metadata_store, _) = create_test_transaction_manager().await;

        // First create a file to lock
        let file_id = FileId::generate();
        metadata_store
            .create_file(
                file_id,
                &PathBuf::from("/test/lockfile2.txt"),
                20002,
                crate::metadata_store::types::FileMetadata {
                    file_type: crate::metadata_store::types::FileType::RegularFile,
                    size: 0,
                    permissions: 0o644,
                    uid: 1000,
                    gid: 1000,
                    created_at: SystemTime::now(),
                    modified_at: SystemTime::now(),
                    accessed_at: SystemTime::now(),
                    target: None,
                },
            )
            .await
            .expect("Failed to create file");

        // Begin transaction
        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        // Add write lock operation
        let expires_at = SystemTime::now() + Duration::from_secs(60);
        let operation = Operation::AcquireWriteLock {
            file_id,
            client_id: 1002,
            node_id: 1,
            expires_at,
        };

        tx_manager
            .add_operation(tx_id, operation)
            .await
            .expect("Failed to add write lock operation");

        // Commit transaction
        tx_manager
            .commit(tx_id)
            .await
            .expect("Failed to commit transaction");

        // Verify lock operation was proposed
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(proposed_ops.len(), 1);

        match &proposed_ops[0] {
            WormFsOperation::AtomicTransaction { operations, .. } => {
                assert_eq!(operations.len(), 1);
                match &operations[0] {
                    MetadataOperation::AcquireWriteLock {
                        file_id: fid,
                        client_id,
                        node_id,
                        ..
                    } => {
                        assert_eq!(*fid, file_id);
                        assert_eq!(*client_id, 1002);
                        assert_eq!(*node_id, 1);
                    }
                    _ => panic!("Expected AcquireWriteLock operation"),
                }
            }
            _ => panic!("Expected AtomicTransaction operation"),
        }
    }

    #[tokio::test]
    async fn test_release_lock() {
        let (tx_manager, raft_member, metadata_store, _) = create_test_transaction_manager().await;

        // Create a file
        let file_id = FileId::generate();
        metadata_store
            .create_file(
                file_id,
                &PathBuf::from("/test/lockfile3.txt"),
                20003,
                crate::metadata_store::types::FileMetadata {
                    file_type: crate::metadata_store::types::FileType::RegularFile,
                    size: 0,
                    permissions: 0o644,
                    uid: 1000,
                    gid: 1000,
                    created_at: SystemTime::now(),
                    modified_at: SystemTime::now(),
                    accessed_at: SystemTime::now(),
                    target: None,
                },
            )
            .await
            .expect("Failed to create file");

        // Acquire a lock first
        let expires_at = SystemTime::now() + Duration::from_secs(60);
        metadata_store
            .acquire_read_lock(
                file_id,
                crate::metadata_store::types::ClientId::new(1003),
                expires_at,
            )
            .await
            .expect("Failed to acquire read lock");

        // Begin transaction to release lock
        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        // Add release lock operation
        let operation = Operation::ReleaseLock {
            file_id,
            client_id: 1003,
        };

        tx_manager
            .add_operation(tx_id, operation)
            .await
            .expect("Failed to add release lock operation");

        // Commit transaction
        tx_manager
            .commit(tx_id)
            .await
            .expect("Failed to commit transaction");

        // Verify release operation was proposed
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(proposed_ops.len(), 1);

        match &proposed_ops[0] {
            WormFsOperation::AtomicTransaction { operations, .. } => {
                assert_eq!(operations.len(), 1);
                match &operations[0] {
                    MetadataOperation::ReleaseLock {
                        file_id: fid,
                        client_id,
                    } => {
                        assert_eq!(*fid, file_id);
                        assert_eq!(*client_id, 1003);
                    }
                    _ => panic!("Expected ReleaseLock operation"),
                }
            }
            _ => panic!("Expected AtomicTransaction operation"),
        }
    }

    #[tokio::test]
    async fn test_extend_lock() {
        let (tx_manager, raft_member, metadata_store, _) = create_test_transaction_manager().await;

        // Create a file
        let file_id = FileId::generate();
        metadata_store
            .create_file(
                file_id,
                &PathBuf::from("/test/lockfile4.txt"),
                20004,
                crate::metadata_store::types::FileMetadata {
                    file_type: crate::metadata_store::types::FileType::RegularFile,
                    size: 0,
                    permissions: 0o644,
                    uid: 1000,
                    gid: 1000,
                    created_at: SystemTime::now(),
                    modified_at: SystemTime::now(),
                    accessed_at: SystemTime::now(),
                    target: None,
                },
            )
            .await
            .expect("Failed to create file");

        // Acquire a lock first
        let expires_at = SystemTime::now() + Duration::from_secs(60);
        metadata_store
            .acquire_write_lock(
                file_id,
                crate::metadata_store::types::ClientId::new(1004),
                1,
                expires_at,
            )
            .await
            .expect("Failed to acquire write lock");

        // Begin transaction to extend lock
        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        // Add extend lock operation
        let new_expiry = SystemTime::now() + Duration::from_secs(120);
        let operation = Operation::ExtendLock {
            file_id,
            client_id: 1004,
            new_expiry,
        };

        tx_manager
            .add_operation(tx_id, operation)
            .await
            .expect("Failed to add extend lock operation");

        // Commit transaction
        tx_manager
            .commit(tx_id)
            .await
            .expect("Failed to commit transaction");

        // Verify extend operation was proposed
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(proposed_ops.len(), 1);

        match &proposed_ops[0] {
            WormFsOperation::AtomicTransaction { operations, .. } => {
                assert_eq!(operations.len(), 1);
                match &operations[0] {
                    MetadataOperation::ExtendLock {
                        file_id: fid,
                        client_id,
                        ..
                    } => {
                        assert_eq!(*fid, file_id);
                        assert_eq!(*client_id, 1004);
                    }
                    _ => panic!("Expected ExtendLock operation"),
                }
            }
            _ => panic!("Expected AtomicTransaction operation"),
        }
    }

    #[tokio::test]
    async fn test_lock_on_nonexistent_file() {
        let (tx_manager, _, _, _) = create_test_transaction_manager().await;

        let fake_file_id = FileId::generate();
        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        let expires_at = SystemTime::now() + Duration::from_secs(60);
        let operation = Operation::AcquireReadLock {
            file_id: fake_file_id,
            client_id: 1005,
            expires_at,
        };

        let result = tx_manager.add_operation(tx_id, operation).await;

        assert!(result.is_err(), "Should fail to lock nonexistent file");

        match result.unwrap_err() {
            Error::FileNotFound(_) => {}
            e => panic!("Expected FileNotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_release_lock_without_holding_it() {
        let (tx_manager, _, metadata_store, _) = create_test_transaction_manager().await;

        // Create a file
        let file_id = FileId::generate();
        metadata_store
            .create_file(
                file_id,
                &PathBuf::from("/test/lockfile5.txt"),
                20005,
                crate::metadata_store::types::FileMetadata {
                    file_type: crate::metadata_store::types::FileType::RegularFile,
                    size: 0,
                    permissions: 0o644,
                    uid: 1000,
                    gid: 1000,
                    created_at: SystemTime::now(),
                    modified_at: SystemTime::now(),
                    accessed_at: SystemTime::now(),
                    target: None,
                },
            )
            .await
            .expect("Failed to create file");

        // Try to release a lock we don't have
        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        let operation = Operation::ReleaseLock {
            file_id,
            client_id: 9999,
        };

        let result = tx_manager.add_operation(tx_id, operation).await;

        assert!(
            result.is_err(),
            "Should fail to release lock not held by client"
        );

        match result.unwrap_err() {
            Error::LockNotFound(_, _) => {}
            e => panic!("Expected LockNotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_extend_lock_without_holding_it() {
        let (tx_manager, _, metadata_store, _) = create_test_transaction_manager().await;

        // Create a file
        let file_id = FileId::generate();
        metadata_store
            .create_file(
                file_id,
                &PathBuf::from("/test/lockfile6.txt"),
                20006,
                crate::metadata_store::types::FileMetadata {
                    file_type: crate::metadata_store::types::FileType::RegularFile,
                    size: 0,
                    permissions: 0o644,
                    uid: 1000,
                    gid: 1000,
                    created_at: SystemTime::now(),
                    modified_at: SystemTime::now(),
                    accessed_at: SystemTime::now(),
                    target: None,
                },
            )
            .await
            .expect("Failed to create file");

        // Try to extend a lock we don't have
        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        let new_expiry = SystemTime::now() + Duration::from_secs(120);
        let operation = Operation::ExtendLock {
            file_id,
            client_id: 9999,
            new_expiry,
        };

        let result = tx_manager.add_operation(tx_id, operation).await;

        assert!(
            result.is_err(),
            "Should fail to extend lock not held by client"
        );

        match result.unwrap_err() {
            Error::LockNotFound(_, _) => {}
            e => panic!("Expected LockNotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_multiple_lock_operations_in_transaction() {
        let (tx_manager, raft_member, metadata_store, _) = create_test_transaction_manager().await;

        // Create two files
        let file_id1 = FileId::generate();
        let file_id2 = FileId::generate();

        metadata_store
            .create_file(
                file_id1,
                &PathBuf::from("/test/lockfile7.txt"),
                20007,
                crate::metadata_store::types::FileMetadata {
                    file_type: crate::metadata_store::types::FileType::RegularFile,
                    size: 0,
                    permissions: 0o644,
                    uid: 1000,
                    gid: 1000,
                    created_at: SystemTime::now(),
                    modified_at: SystemTime::now(),
                    accessed_at: SystemTime::now(),
                    target: None,
                },
            )
            .await
            .expect("Failed to create file1");

        metadata_store
            .create_file(
                file_id2,
                &PathBuf::from("/test/lockfile8.txt"),
                20008,
                crate::metadata_store::types::FileMetadata {
                    file_type: crate::metadata_store::types::FileType::RegularFile,
                    size: 0,
                    permissions: 0o644,
                    uid: 1000,
                    gid: 1000,
                    created_at: SystemTime::now(),
                    modified_at: SystemTime::now(),
                    accessed_at: SystemTime::now(),
                    target: None,
                },
            )
            .await
            .expect("Failed to create file2");

        // Begin transaction
        let tx_id = tx_manager
            .begin(Duration::from_secs(30))
            .await
            .expect("Failed to begin transaction");

        // Add multiple lock operations
        let expires_at = SystemTime::now() + Duration::from_secs(60);

        tx_manager
            .add_operation(
                tx_id,
                Operation::AcquireReadLock {
                    file_id: file_id1,
                    client_id: 2001,
                    expires_at,
                },
            )
            .await
            .expect("Failed to add first lock");

        tx_manager
            .add_operation(
                tx_id,
                Operation::AcquireWriteLock {
                    file_id: file_id2,
                    client_id: 2001,
                    node_id: 1,
                    expires_at,
                },
            )
            .await
            .expect("Failed to add second lock");

        // Commit transaction
        tx_manager
            .commit(tx_id)
            .await
            .expect("Failed to commit transaction");

        // Verify both lock operations were batched
        let proposed_ops = raft_member.get_proposed_operations().await;
        assert_eq!(proposed_ops.len(), 1);

        match &proposed_ops[0] {
            WormFsOperation::AtomicTransaction { operations, .. } => {
                assert_eq!(
                    operations.len(),
                    2,
                    "Should have batched both lock operations"
                );
            }
            _ => panic!("Expected AtomicTransaction operation"),
        }
    }

    #[tokio::test]
    async fn test_subscribe_metadata_changes() {
        use crate::storage_raft_member::types::{MetadataChange, MetadataChangeEvent};

        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        // Subscribe to metadata changes
        let mut rx = tx_manager.subscribe_metadata_changes(None).await;

        // Send a test event
        let test_event = MetadataChangeEvent {
            committed_at: SystemTime::now(),
            log_index: 1,
            changes: vec![MetadataChange::FileCreated {
                file_id: FileId::generate(),
                inode: 12345,
                path: PathBuf::from("/test/newfile.txt"),
            }],
        };

        raft_member.send_test_event(test_event.clone()).await;

        // Verify we received the event
        let received_event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("Timeout waiting for event")
            .expect("Channel closed");

        assert_eq!(received_event.log_index, test_event.log_index);
        assert_eq!(received_event.changes.len(), 1);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        use crate::storage_raft_member::types::{MetadataChange, MetadataChangeEvent};

        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        // Create multiple subscribers
        let mut rx1 = tx_manager.subscribe_metadata_changes(None).await;
        let mut rx2 = tx_manager.subscribe_metadata_changes(None).await;
        let mut rx3 = tx_manager.subscribe_metadata_changes(None).await;

        // Send a test event
        let test_event = MetadataChangeEvent {
            committed_at: SystemTime::now(),
            log_index: 42,
            changes: vec![MetadataChange::FileDeleted {
                file_id: FileId::generate(),
                inode: 54321,
            }],
        };

        raft_member.send_test_event(test_event.clone()).await;

        // Verify all subscribers received the event
        let event1 = tokio::time::timeout(Duration::from_secs(1), rx1.recv())
            .await
            .expect("Timeout on rx1")
            .expect("Channel closed");
        let event2 = tokio::time::timeout(Duration::from_secs(1), rx2.recv())
            .await
            .expect("Timeout on rx2")
            .expect("Channel closed");
        let event3 = tokio::time::timeout(Duration::from_secs(1), rx3.recv())
            .await
            .expect("Timeout on rx3")
            .expect("Channel closed");

        assert_eq!(event1.log_index, 42);
        assert_eq!(event2.log_index, 42);
        assert_eq!(event3.log_index, 42);
    }

    #[tokio::test]
    async fn test_subscribe_with_stripe_events() {
        use crate::storage_raft_member::types::{MetadataChange, MetadataChangeEvent};

        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        // Subscribe to metadata changes
        let mut rx = tx_manager.subscribe_metadata_changes(None).await;

        // Send a stripe creation event
        let file_id = FileId::generate();
        let stripe_id = StripeId::generate();
        let test_event = MetadataChangeEvent {
            committed_at: SystemTime::now(),
            log_index: 100,
            changes: vec![MetadataChange::StripeCreated {
                file_id,
                stripe_id,
                offset: 0,
                size: 1024,
            }],
        };

        raft_member.send_test_event(test_event.clone()).await;

        // Verify we received the stripe event
        let received_event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("Timeout waiting for event")
            .expect("Channel closed");

        assert_eq!(received_event.log_index, 100);
        assert_eq!(received_event.changes.len(), 1);

        match &received_event.changes[0] {
            MetadataChange::StripeCreated {
                file_id: fid,
                stripe_id: sid,
                offset,
                size,
            } => {
                assert_eq!(*fid, file_id);
                assert_eq!(*sid, stripe_id);
                assert_eq!(*offset, 0);
                assert_eq!(*size, 1024);
            }
            _ => panic!("Expected StripeCreated event"),
        }
    }

    #[tokio::test]
    async fn test_subscription_channel_independent() {
        use crate::storage_raft_member::types::{MetadataChange, MetadataChangeEvent};

        let (tx_manager, raft_member, _, _) = create_test_transaction_manager().await;

        // Create two subscribers
        let mut rx1 = tx_manager.subscribe_metadata_changes(None).await;
        let mut rx2 = tx_manager.subscribe_metadata_changes(None).await;

        // Send first event
        let event1 = MetadataChangeEvent {
            committed_at: SystemTime::now(),
            log_index: 1,
            changes: vec![MetadataChange::FileCreated {
                file_id: FileId::generate(),
                inode: 111,
                path: PathBuf::from("/test/file1.txt"),
            }],
        };

        raft_member.send_test_event(event1.clone()).await;

        // Read from rx1 only
        let _ = rx1.recv().await.expect("rx1 should receive event");

        // Send second event
        let event2 = MetadataChangeEvent {
            committed_at: SystemTime::now(),
            log_index: 2,
            changes: vec![MetadataChange::FileCreated {
                file_id: FileId::generate(),
                inode: 222,
                path: PathBuf::from("/test/file2.txt"),
            }],
        };

        raft_member.send_test_event(event2.clone()).await;

        // rx2 should have both events
        let rx2_event1 = rx2.recv().await.expect("rx2 should receive first event");
        let rx2_event2 = rx2.recv().await.expect("rx2 should receive second event");

        assert_eq!(rx2_event1.log_index, 1);
        assert_eq!(rx2_event2.log_index, 2);

        // rx1 should only have second event
        let rx1_event2 = rx1.recv().await.expect("rx1 should receive second event");
        assert_eq!(rx1_event2.log_index, 2);
    }
}
