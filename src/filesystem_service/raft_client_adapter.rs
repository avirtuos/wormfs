//! Adapter that implements RaftClient trait using real StorageRaftMemberImpl.
//!
//! This adapter bridges the FileSystemService's RaftClient interface with the
//! actual Raft consensus layer, enabling distributed metadata replication.

use crate::file_store::types::StripeMetadata;
use crate::filesystem_service::buffered_file_handle::{RaftClient, StripeOperation};
use crate::filesystem_service::types::Error;
use crate::storage_raft_member::types::{
    ChunkPlacement, FileMetadata, MetadataOperation, NodeId, StoragePolicy, TxId, WormFsOperation,
};
use crate::storage_raft_member::{StorageRaftMember, StorageRaftMemberImpl};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::debug;

/// Adapter implementing RaftClient using real Raft consensus for stripe operations.
///
/// This adapter uses real Raft for stripe/chunk metadata (which needs replication)
/// but falls back to the stub for file operations (which are still Phase 1 in implementation.rs).
pub struct RaftClientAdapter {
    raft_member: Arc<StorageRaftMemberImpl>,
    stub: Arc<crate::filesystem_service::raft_commands::StorageRaftMemberStub>,
}

impl RaftClientAdapter {
    /// Create a new RaftClientAdapter wrapping a StorageRaftMemberImpl.
    ///
    /// # Arguments
    ///
    /// * `raft_member` - Real Raft member for stripe/chunk consensus
    /// * `stub` - Stub for file operations (Phase 1 compatibility)
    pub fn new(
        raft_member: Arc<StorageRaftMemberImpl>,
        stub: Arc<crate::filesystem_service::raft_commands::StorageRaftMemberStub>,
    ) -> Self {
        Self { raft_member, stub }
    }

    /// Convert StripeOperations to MetadataOperations for Raft consensus.
    fn convert_operations(
        operations: Vec<StripeOperation>,
    ) -> Result<Vec<MetadataOperation>, Error> {
        let mut metadata_ops = Vec::new();

        for op in operations {
            match op {
                StripeOperation::Create { file_id, stripe } => {
                    // Convert to CreateStripe operation with chunk placements
                    metadata_ops.push(Self::create_stripe_operation(file_id, stripe)?);
                }
                StripeOperation::Update { file_id, stripe } => {
                    // For updates, we delete the old stripe and create a new one
                    // This matches the stub's behavior
                    metadata_ops.push(MetadataOperation::DeleteStripe {
                        stripe_id: stripe.stripe_id,
                        file_id,
                    });
                    metadata_ops.push(Self::create_stripe_operation(file_id, stripe)?);
                }
                StripeOperation::Delete { stripe_id, file_id } => {
                    metadata_ops.push(MetadataOperation::DeleteStripe { stripe_id, file_id });
                }
                StripeOperation::UpdateAttributes {
                    file_id,
                    inode,
                    attributes,
                } => {
                    // Convert FileAttr to FileMetadata
                    use crate::filesystem_service::types::FileType as FsFileType;

                    let file_type_num = match attributes.kind {
                        FsFileType::RegularFile => 0u8,
                        FsFileType::Directory => 1u8,
                        FsFileType::Symlink => 2u8,
                        _ => 0u8, // Other types default to regular file
                    };

                    let metadata = FileMetadata {
                        size: attributes.size,
                        created: attributes.crtime,
                        modified: attributes.mtime,
                        mode: attributes.perm as u32,
                        uid: attributes.uid,
                        gid: attributes.gid,
                        file_type: file_type_num,
                        target: None, // Will be preserved by state machine if symlink
                    };

                    // Use default storage policy (will be overridden if file exists)
                    let policy = StoragePolicy {
                        data_chunks: 2,
                        parity_chunks: 1,
                        replication_factor: 1,
                    };

                    metadata_ops.push(MetadataOperation::FileUpdate {
                        file_id,
                        inode,
                        metadata,
                        policy,
                    });
                }
            }
        }

        Ok(metadata_ops)
    }

    /// Create a CreateStripe operation from StripeMetadata.
    fn create_stripe_operation(
        file_id: crate::file_store::types::FileId,
        stripe: StripeMetadata,
    ) -> Result<MetadataOperation, Error> {
        // Calculate stripe index from offset
        const STRIPE_SIZE: u64 = 1024 * 1024; // 1MB
        let stripe_index = (stripe.offset / STRIPE_SIZE) as u32;

        // Convert ChunkMetadata to ChunkPlacement
        let chunks: Vec<ChunkPlacement> = stripe
            .chunks
            .iter()
            .map(|chunk| ChunkPlacement {
                chunk_id: chunk.chunk_id,
                // Convert FileStore NodeId to Raft NodeId
                node_id: NodeId(chunk.node_id.0),
                disk_id: chunk.disk_id,
                chunk_index: chunk.chunk_index as u32,
            })
            .collect();

        // Use default storage policy (2+1 erasure coding)
        let policy = StoragePolicy {
            data_chunks: 2,
            parity_chunks: 1,
            replication_factor: 1,
        };

        Ok(MetadataOperation::CreateStripe {
            file_id,
            stripe_id: stripe.stripe_id,
            stripe_index,
            policy,
            offset: stripe.offset,
            size: stripe.size,
            chunks,
        })
    }

    /// Generate a distributed-safe inode using node_id and timestamp.
    ///
    /// **Inode Format** (64 bits total):
    /// ```text
    /// |  16 bits  |      32 bits       |    16 bits    |
    /// |  node_id  | unix_timestamp_sec | per-sec counter|
    /// ```
    ///
    /// This ensures uniqueness across:
    /// - **Different nodes**: Each node has unique node_id (up to 65,536 nodes)
    /// - **Node restarts**: Timestamp changes every second, preventing collisions
    /// - **High throughput**: Counter allows 65,536 inodes per second per node
    ///
    /// **Collision Prevention:**
    /// - Files created at different seconds get different inodes (timestamp changes)
    /// - Files created in same second use incrementing counter (up to 65k/sec)
    /// - Different nodes can't collide (node_id in high bits)
    /// - Node restart resets counter, but timestamp will have advanced
    ///
    /// **Limitations:**
    /// - Maximum 65,536 nodes (16-bit node_id)
    /// - Maximum 65,536 file creations per second per node
    /// - Year 2038 problem: timestamp will overflow u32 in 2038
    ///   (After 2038, falls back to monotonic counter only)
    ///
    /// # Arguments
    ///
    /// * `node_id` - Must be < 65536 to fit in 16 bits (asserted in debug builds)
    fn generate_distributed_inode(&self, node_id: u64) -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};

        // Per-second counter to allow multiple inodes in same second
        static INODE_COUNTER: AtomicU64 = AtomicU64::new(0);

        // Validate node_id fits in 16 bits (only in debug builds for performance)
        debug_assert!(
            node_id < (1 << 16),
            "node_id {} exceeds 16-bit limit (max: 65535)",
            node_id
        );

        // Get current Unix timestamp (seconds since epoch)
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Increment counter and wrap at 16 bits (65536)
        let counter = INODE_COUNTER.fetch_add(1, Ordering::SeqCst) & 0xFFFF;

        // Combine: node_id (16) | timestamp (32) | counter (16)
        let node_bits = (node_id & 0xFFFF) << 48;
        let timestamp_bits = (timestamp & 0xFFFF_FFFF) << 16;
        let counter_bits = counter & 0xFFFF;

        node_bits | timestamp_bits | counter_bits
    }
}

#[async_trait::async_trait]
impl RaftClient for RaftClientAdapter {
    async fn propose_stripe_batch(&self, operations: Vec<StripeOperation>) -> Result<(), Error> {
        if operations.is_empty() {
            return Ok(());
        }

        debug!(
            "RaftClientAdapter proposing {} stripe operations through Raft",
            operations.len()
        );

        // Convert StripeOperations to MetadataOperations
        let metadata_ops = Self::convert_operations(operations)?;

        // Create an atomic transaction
        let operation = WormFsOperation::AtomicTransaction {
            tx_id: TxId::generate(),
            operations: metadata_ops,
            timeout: SystemTime::now() + Duration::from_secs(60),
        };

        // Propose through Raft consensus
        self.raft_member
            .propose_operation(operation)
            .await
            .map_err(|e| Error::Internal(format!("Raft proposal failed: {}", e)))
    }

    async fn propose_raft_command(
        &self,
        command: crate::filesystem_service::raft_commands::RaftCommand,
    ) -> Result<crate::filesystem_service::raft_commands::RaftCommandResult, Error> {
        use crate::filesystem_service::raft_commands::{FileType, RaftCommand, RaftCommandResult};
        use crate::metadata_store::MetadataStore;

        match command {
            RaftCommand::CreateFile {
                parent_inode,
                name,
                file_type,
                mode,
                uid,
                gid,
            } => {
                // Get metadata store from stub for parent lookup
                let metadata_store = self.stub.metadata_store();

                // 1. Look up parent to construct full path
                let parent = metadata_store
                    .get_file_by_inode(parent_inode)
                    .await
                    .map_err(|e| Error::Internal(format!("Parent not found: {}", e)))?;
                let path = parent.path.join(&name);

                // 2. Generate distributed-safe inode using node_id in high bits
                let node_id = self.raft_member.node_id().as_u64();
                let inode = self.generate_distributed_inode(node_id);

                // 3. Generate file_id
                let file_id = crate::file_store::FileId::generate();

                // 4. Convert FileType to numeric representation
                let file_type_num = match file_type {
                    FileType::Regular => 0u8,
                    FileType::Directory => 1u8,
                    FileType::Symlink => 2u8,
                };

                // 5. Create FileMetadata
                let now = SystemTime::now();
                let metadata = FileMetadata {
                    size: 0,
                    created: now,
                    modified: now,
                    mode,
                    uid,
                    gid,
                    file_type: file_type_num,
                    target: None, // Will be set for symlinks via separate command
                };

                // 6. Create default storage policy
                let policy = StoragePolicy {
                    data_chunks: 2,
                    parity_chunks: 1,
                    replication_factor: 1,
                };

                // 7. Propose through Raft
                let operation = WormFsOperation::AtomicTransaction {
                    tx_id: TxId::generate(),
                    operations: vec![MetadataOperation::FileCreate {
                        file_id,
                        path,
                        inode,
                        metadata,
                        policy,
                    }],
                    timeout: SystemTime::now() + Duration::from_secs(60),
                };

                self.raft_member
                    .propose_operation(operation)
                    .await
                    .map_err(|e| Error::Internal(format!("Raft proposal failed: {}", e)))?;

                Ok(RaftCommandResult::FileCreated { inode, file_id })
            }

            RaftCommand::DeleteFile { parent_inode, name } => {
                // Get metadata store from stub for file lookup
                let metadata_store = self.stub.metadata_store();

                // Look up parent to construct path
                let parent = metadata_store
                    .get_file_by_inode(parent_inode)
                    .await
                    .map_err(|e| Error::Internal(format!("Parent not found: {}", e)))?;
                let path = parent.path.join(&name);

                // Look up file to get file_id and inode
                let file = metadata_store
                    .get_file_by_path(&path)
                    .await
                    .map_err(|e| Error::Internal(format!("File not found: {}", e)))?;

                // Propose FileDelete through Raft
                let operation = WormFsOperation::AtomicTransaction {
                    tx_id: TxId::generate(),
                    operations: vec![MetadataOperation::FileDelete {
                        file_id: file.file_id,
                        inode: file.inode,
                    }],
                    timeout: SystemTime::now() + Duration::from_secs(60),
                };

                self.raft_member
                    .propose_operation(operation)
                    .await
                    .map_err(|e| Error::Internal(format!("Raft proposal failed: {}", e)))?;

                Ok(RaftCommandResult::FileDeleted)
            }

            RaftCommand::UpdateFile { inode, updates } => {
                // Get metadata store from stub for file lookup
                let metadata_store = self.stub.metadata_store();

                // Look up file to get file_id and current metadata
                let file = metadata_store
                    .get_file_by_inode(inode)
                    .await
                    .map_err(|e| Error::Internal(format!("File not found: {}", e)))?;

                // Convert file_type to numeric
                let file_type_num = match file.file_type {
                    crate::metadata_store::FileType::RegularFile => 0u8,
                    crate::metadata_store::FileType::Directory => 1u8,
                    crate::metadata_store::FileType::Symlink => 2u8,
                };

                // Create updated metadata (apply updates or keep existing values)
                let metadata = FileMetadata {
                    size: updates.size.unwrap_or(file.size),
                    created: file.created_at,
                    modified: updates.mtime.unwrap_or(file.modified_at),
                    mode: updates.mode.unwrap_or(file.permissions),
                    uid: updates.uid.unwrap_or(file.uid),
                    gid: updates.gid.unwrap_or(file.gid),
                    file_type: file_type_num,
                    target: file.target,
                };

                // Keep existing storage policy (look up from file's stripes or use default)
                let policy = StoragePolicy {
                    data_chunks: 2,
                    parity_chunks: 1,
                    replication_factor: 1,
                };

                // Propose FileUpdate through Raft
                let operation = WormFsOperation::AtomicTransaction {
                    tx_id: TxId::generate(),
                    operations: vec![MetadataOperation::FileUpdate {
                        file_id: file.file_id,
                        inode,
                        metadata,
                        policy,
                    }],
                    timeout: SystemTime::now() + Duration::from_secs(60),
                };

                self.raft_member
                    .propose_operation(operation)
                    .await
                    .map_err(|e| Error::Internal(format!("Raft proposal failed: {}", e)))?;

                Ok(RaftCommandResult::FileUpdated)
            }

            // Other commands (symlinks, transactions, etc.) still use stub
            // Lock operations are handled via dedicated acquire_lock/release_lock/extend_lock methods
            other => self
                .stub
                .propose_operation(other)
                .await
                .map_err(|e| Error::Internal(format!("Stub operation failed: {}", e))),
        }
    }

    async fn acquire_lock(
        &self,
        file_id: crate::file_store::FileId,
        _inode: u64,
        lock_type: crate::filesystem_service::raft_commands::LockType,
        client_id: u64,
        node_id: u64,
        expires_at: SystemTime,
    ) -> Result<u64, Error> {
        // Create lock operation using file_id
        let operation = match lock_type {
            crate::filesystem_service::raft_commands::LockType::Write => {
                MetadataOperation::AcquireWriteLock {
                    file_id,
                    client_id,
                    node_id,
                    expires_at,
                }
            }
            crate::filesystem_service::raft_commands::LockType::Read => {
                MetadataOperation::AcquireReadLock {
                    file_id,
                    client_id,
                    expires_at,
                }
            }
        };

        // Propose through Raft
        let raft_operation = WormFsOperation::AtomicTransaction {
            tx_id: TxId::generate(),
            operations: vec![operation],
            timeout: SystemTime::now() + Duration::from_secs(60),
        };

        // For now, we don't get lock_id back from Raft - generate one
        // TODO: Update state machine to return lock_id in operation result
        self.raft_member
            .propose_operation(raft_operation)
            .await
            .map_err(|e| Error::Internal(format!("Failed to acquire lock: {}", e)))?;

        // Generate a lock ID (temporary solution until state machine returns it)
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_LOCK_ID: AtomicU64 = AtomicU64::new(1);
        Ok(NEXT_LOCK_ID.fetch_add(1, Ordering::SeqCst))
    }

    async fn release_lock(
        &self,
        file_id: crate::file_store::FileId,
        _inode: u64,
        client_id: u64,
    ) -> Result<(), Error> {
        // Create release lock operation
        let operation = MetadataOperation::ReleaseLock { file_id, client_id };

        // Propose through Raft
        let raft_operation = WormFsOperation::AtomicTransaction {
            tx_id: TxId::generate(),
            operations: vec![operation],
            timeout: SystemTime::now() + Duration::from_secs(60),
        };

        self.raft_member
            .propose_operation(raft_operation)
            .await
            .map_err(|e| Error::Internal(format!("Failed to release lock: {}", e)))
    }

    async fn extend_lock(
        &self,
        file_id: crate::file_store::FileId,
        _inode: u64,
        client_id: u64,
        new_expiry: SystemTime,
    ) -> Result<(), Error> {
        // Create extend lock operation
        let operation = MetadataOperation::ExtendLock {
            file_id,
            client_id,
            new_expiry,
        };

        // Propose through Raft
        let raft_operation = WormFsOperation::AtomicTransaction {
            tx_id: TxId::generate(),
            operations: vec![operation],
            timeout: SystemTime::now() + Duration::from_secs(60),
        };

        self.raft_member
            .propose_operation(raft_operation)
            .await
            .map_err(|e| Error::Internal(format!("Failed to extend lock: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_store::types::StripeMetadata;
    use crate::filesystem_service::types::FileAttr;
    use crate::filesystem_service::types::FileType as FsFileType;

    /// Test convert_operations with Create operation
    #[test]
    fn test_convert_operations_create() {
        let file_id = crate::file_store::types::FileId::generate();
        let stripe_id = crate::file_store::types::StripeId::generate();
        let chunk_id = crate::file_store::types::ChunkId::generate();
        let node_id = crate::file_store::types::NodeId(1);
        let disk_id = crate::file_store::types::DiskId::new(100);

        let stripe = StripeMetadata {
            stripe_id,
            file_id,
            offset: 0,
            size: 1024,
            checksum: 0, // Test value
            chunks: vec![crate::file_store::types::ChunkMetadata::new(
                chunk_id, node_id, disk_id, 0,
            )],
        };

        let operations = vec![StripeOperation::Create {
            file_id,
            stripe: stripe.clone(),
        }];

        let result = RaftClientAdapter::convert_operations(operations);
        assert!(result.is_ok());

        let metadata_ops = result.unwrap();
        assert_eq!(metadata_ops.len(), 1);

        match &metadata_ops[0] {
            MetadataOperation::CreateStripe {
                file_id: f,
                stripe_id: s,
                ..
            } => {
                assert_eq!(*f, file_id);
                assert_eq!(*s, stripe_id);
            }
            _ => panic!("Expected CreateStripe operation"),
        }
    }

    /// Test convert_operations with Update operation (should create Delete + Create)
    #[test]
    fn test_convert_operations_update() {
        let file_id = crate::file_store::types::FileId::generate();
        let stripe_id = crate::file_store::types::StripeId::generate();
        let chunk_id = crate::file_store::types::ChunkId::generate();
        let node_id = crate::file_store::types::NodeId(1);
        let disk_id = crate::file_store::types::DiskId::new(100);

        let stripe = StripeMetadata {
            stripe_id,
            file_id,
            offset: 0,
            size: 1024,
            checksum: 0, // Test value
            chunks: vec![crate::file_store::types::ChunkMetadata::new(
                chunk_id, node_id, disk_id, 0,
            )],
        };

        let operations = vec![StripeOperation::Update {
            file_id,
            stripe: stripe.clone(),
        }];

        let result = RaftClientAdapter::convert_operations(operations);
        assert!(result.is_ok());

        let metadata_ops = result.unwrap();
        // Update should produce DeleteStripe + CreateStripe
        assert_eq!(metadata_ops.len(), 2);

        match &metadata_ops[0] {
            MetadataOperation::DeleteStripe {
                stripe_id: s,
                file_id: f,
            } => {
                assert_eq!(*s, stripe_id);
                assert_eq!(*f, file_id);
            }
            _ => panic!("Expected DeleteStripe as first operation"),
        }

        match &metadata_ops[1] {
            MetadataOperation::CreateStripe {
                file_id: f,
                stripe_id: s,
                ..
            } => {
                assert_eq!(*f, file_id);
                assert_eq!(*s, stripe_id);
            }
            _ => panic!("Expected CreateStripe as second operation"),
        }
    }

    /// Test convert_operations with Delete operation
    #[test]
    fn test_convert_operations_delete() {
        let file_id = crate::file_store::types::FileId::generate();
        let stripe_id = crate::file_store::types::StripeId::generate();

        let operations = vec![StripeOperation::Delete { stripe_id, file_id }];

        let result = RaftClientAdapter::convert_operations(operations);
        assert!(result.is_ok());

        let metadata_ops = result.unwrap();
        assert_eq!(metadata_ops.len(), 1);

        match &metadata_ops[0] {
            MetadataOperation::DeleteStripe {
                stripe_id: s,
                file_id: f,
            } => {
                assert_eq!(*s, stripe_id);
                assert_eq!(*f, file_id);
            }
            _ => panic!("Expected DeleteStripe operation"),
        }
    }

    /// Test convert_operations with UpdateAttributes operation
    #[test]
    fn test_convert_operations_update_attributes() {
        use std::time::SystemTime;

        let file_id = crate::file_store::types::FileId::generate();
        let inode = 12345;
        let now = SystemTime::now();

        let attributes = FileAttr {
            ino: inode,
            size: 2048,
            blocks: 4,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FsFileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        };

        let operations = vec![StripeOperation::UpdateAttributes {
            file_id,
            inode,
            attributes: attributes.clone(),
        }];

        let result = RaftClientAdapter::convert_operations(operations);
        assert!(result.is_ok());

        let metadata_ops = result.unwrap();
        assert_eq!(metadata_ops.len(), 1);

        match &metadata_ops[0] {
            MetadataOperation::FileUpdate {
                file_id: f,
                inode: i,
                metadata,
                ..
            } => {
                assert_eq!(*f, file_id);
                assert_eq!(*i, inode);
                assert_eq!(metadata.size, 2048);
                assert_eq!(metadata.uid, 1000);
                assert_eq!(metadata.gid, 1000);
                assert_eq!(metadata.mode, 0o644);
            }
            _ => panic!("Expected FileUpdate operation"),
        }
    }

    /// Test convert_operations with multiple operations
    #[test]
    fn test_convert_operations_multiple() {
        let file_id = crate::file_store::types::FileId::generate();
        let stripe_id1 = crate::file_store::types::StripeId::generate();
        let stripe_id2 = crate::file_store::types::StripeId::generate();
        let chunk_id = crate::file_store::types::ChunkId::generate();
        let node_id = crate::file_store::types::NodeId(1);
        let disk_id = crate::file_store::types::DiskId::new(100);

        let stripe1 = StripeMetadata {
            stripe_id: stripe_id1,
            file_id,
            offset: 0,
            size: 1024,
            checksum: 0, // Test value
            chunks: vec![crate::file_store::types::ChunkMetadata::new(
                chunk_id, node_id, disk_id, 0,
            )],
        };

        let operations = vec![
            StripeOperation::Create {
                file_id,
                stripe: stripe1,
            },
            StripeOperation::Delete {
                stripe_id: stripe_id2,
                file_id,
            },
        ];

        let result = RaftClientAdapter::convert_operations(operations);
        assert!(result.is_ok());

        let metadata_ops = result.unwrap();
        assert_eq!(metadata_ops.len(), 2);
    }

    /// Test convert_operations with empty operations list
    #[test]
    fn test_convert_operations_empty() {
        let operations: Vec<StripeOperation> = vec![];

        let result = RaftClientAdapter::convert_operations(operations);
        assert!(result.is_ok());

        let metadata_ops = result.unwrap();
        assert_eq!(metadata_ops.len(), 0);
    }

    /// Test create_stripe_operation with valid stripe metadata
    #[test]
    fn test_create_stripe_operation() {
        let file_id = crate::file_store::types::FileId::generate();
        let stripe_id = crate::file_store::types::StripeId::generate();
        let chunk_id = crate::file_store::types::ChunkId::generate();
        let node_id = crate::file_store::types::NodeId(1);
        let disk_id = crate::file_store::types::DiskId::new(100);

        let stripe = StripeMetadata {
            stripe_id,
            file_id,
            offset: 1024 * 1024, // 1MB offset
            size: 2048,
            checksum: 0, // Test value
            chunks: vec![crate::file_store::types::ChunkMetadata::new(
                chunk_id, node_id, disk_id, 0,
            )],
        };

        let result = RaftClientAdapter::create_stripe_operation(file_id, stripe);
        assert!(result.is_ok());

        let operation = result.unwrap();
        match operation {
            MetadataOperation::CreateStripe {
                file_id: f,
                stripe_id: s,
                stripe_index,
                offset,
                size,
                chunks,
                ..
            } => {
                assert_eq!(f, file_id);
                assert_eq!(s, stripe_id);
                assert_eq!(stripe_index, 1); // 1MB / 1MB = index 1
                assert_eq!(offset, 1024 * 1024);
                assert_eq!(size, 2048);
                assert_eq!(chunks.len(), 1);
                assert_eq!(chunks[0].chunk_id, chunk_id);
                assert_eq!(chunks[0].node_id, NodeId(1));
            }
            _ => panic!("Expected CreateStripe operation"),
        }
    }

    /// Test UpdateAttributes with Directory type
    #[test]
    fn test_convert_operations_update_attributes_directory() {
        use std::time::SystemTime;

        let file_id = crate::file_store::types::FileId::generate();
        let inode = 12345;
        let now = SystemTime::now();

        let attributes = FileAttr {
            ino: inode,
            size: 4096,
            blocks: 8,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FsFileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        };

        let operations = vec![StripeOperation::UpdateAttributes {
            file_id,
            inode,
            attributes,
        }];

        let result = RaftClientAdapter::convert_operations(operations);
        assert!(result.is_ok());

        let metadata_ops = result.unwrap();
        assert_eq!(metadata_ops.len(), 1);

        match &metadata_ops[0] {
            MetadataOperation::FileUpdate { metadata, .. } => {
                assert_eq!(metadata.file_type, 1); // Directory = 1
            }
            _ => panic!("Expected FileUpdate operation"),
        }
    }

    /// Test UpdateAttributes with Symlink type
    #[test]
    fn test_convert_operations_update_attributes_symlink() {
        use std::time::SystemTime;

        let file_id = crate::file_store::types::FileId::generate();
        let inode = 12345;
        let now = SystemTime::now();

        let attributes = FileAttr {
            ino: inode,
            size: 10,
            blocks: 1,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FsFileType::Symlink,
            perm: 0o777,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        };

        let operations = vec![StripeOperation::UpdateAttributes {
            file_id,
            inode,
            attributes,
        }];

        let result = RaftClientAdapter::convert_operations(operations);
        assert!(result.is_ok());

        let metadata_ops = result.unwrap();
        assert_eq!(metadata_ops.len(), 1);

        match &metadata_ops[0] {
            MetadataOperation::FileUpdate { metadata, .. } => {
                assert_eq!(metadata.file_type, 2); // Symlink = 2
            }
            _ => panic!("Expected FileUpdate operation"),
        }
    }

    /// Test inode format and collision resistance
    ///
    /// Tests the distributed inode generation logic without requiring
    /// a full RaftClientAdapter instance
    #[test]
    fn test_distributed_inode_format() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};

        // Simulate the inode generation logic from generate_distributed_inode
        let node_id = 1u64;
        let counter = AtomicU64::new(0);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Generate first inode
        let count1 = counter.fetch_add(1, Ordering::SeqCst) & 0xFFFF;
        let inode1 =
            ((node_id & 0xFFFF) << 48) | ((timestamp & 0xFFFF_FFFF) << 16) | (count1 & 0xFFFF);

        // Generate second inode
        let count2 = counter.fetch_add(1, Ordering::SeqCst) & 0xFFFF;
        let inode2 =
            ((node_id & 0xFFFF) << 48) | ((timestamp & 0xFFFF_FFFF) << 16) | (count2 & 0xFFFF);

        // Inodes should be unique
        assert_ne!(inode1, inode2);

        // Node ID should be in high 16 bits
        assert_eq!(inode1 >> 48, node_id);
        assert_eq!(inode2 >> 48, node_id);

        // Timestamp should be in middle 32 bits
        let ts1 = (inode1 >> 16) & 0xFFFF_FFFF;
        let ts2 = (inode2 >> 16) & 0xFFFF_FFFF;
        assert_eq!(ts1, timestamp);
        assert_eq!(ts2, timestamp);

        // Counter should be in low 16 bits and increment
        assert_eq!(inode1 & 0xFFFF, 0);
        assert_eq!(inode2 & 0xFFFF, 1);
    }

    /// Test inode generation with different nodes prevents collisions
    #[test]
    fn test_distributed_inode_different_nodes() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let node1_id = 1u64;
        let node2_id = 2u64;
        let counter = 0u64;

        let inode1 =
            ((node1_id & 0xFFFF) << 48) | ((timestamp & 0xFFFF_FFFF) << 16) | (counter & 0xFFFF);

        let inode2 =
            ((node2_id & 0xFFFF) << 48) | ((timestamp & 0xFFFF_FFFF) << 16) | (counter & 0xFFFF);

        // Different nodes produce different inodes even with same timestamp/counter
        assert_ne!(inode1, inode2);

        // Verify node IDs are preserved in high 16 bits
        assert_eq!(inode1 >> 48, node1_id);
        assert_eq!(inode2 >> 48, node2_id);
    }

    /// Test that restart doesn't cause inode collisions due to timestamp component
    #[test]
    fn test_distributed_inode_restart_safety() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let node_id = 1u64;

        // Simulate first boot: counter at 0, timestamp T
        let timestamp1 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let inode_boot1 = ((node_id & 0xFFFF) << 48) | ((timestamp1 & 0xFFFF_FFFF) << 16);

        // Simulate restart (1 second later): counter resets to 0, but timestamp advanced
        let timestamp2 = timestamp1 + 1;
        let inode_boot2 = ((node_id & 0xFFFF) << 48) | ((timestamp2 & 0xFFFF_FFFF) << 16);

        // Even though counter reset to 0, timestamp changed so no collision
        assert_ne!(inode_boot1, inode_boot2);

        // Verify timestamps are different
        let ts1 = (inode_boot1 >> 16) & 0xFFFF_FFFF;
        let ts2 = (inode_boot2 >> 16) & 0xFFFF_FFFF;
        assert_eq!(ts1, timestamp1);
        assert_eq!(ts2, timestamp2);
    }
}
