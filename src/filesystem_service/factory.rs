//! Factory for creating FileSystemService instances.
//!
//! The factory pattern separates construction concerns from the service trait,
//! allowing the service trait to remain mockable while still providing type-safe
//! construction with concrete dependency types.

use super::{Config, Error, FileSystemService};
use std::sync::Arc;

/// Factory trait for creating FileSystemService instances.
///
/// This trait uses associated types to specify the concrete implementations
/// of dependencies (RaftMember, MetadataStore, FileStore) at compile time.
pub trait FileSystemServiceFactory {
    /// StorageRaftMember implementation type
    type RaftMember: crate::storage_raft_member::StorageRaftMember<
        Operation = crate::storage_raft_member::types::WormFsOperation,
        OperationResult = (),
    >;

    /// MetadataStore implementation type
    type MetadataStore: crate::metadata_store::MetadataStore;

    /// FileStore implementation type
    type FileStore: crate::file_store::FileStore;

    /// FileSystemService implementation type
    type Service: FileSystemService;

    /// Create a new FileSystemService instance.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the filesystem service
    /// * `raft_member` - StorageRaftMember instance for metadata writes
    /// * `metadata_store` - MetadataStore instance for metadata reads
    /// * `file_store` - FileStore instance for chunk I/O operations
    ///
    /// # Returns
    ///
    /// A fully initialized FileSystemService instance.
    ///
    /// # Errors
    ///
    /// Returns an error if initialization fails.
    fn create(
        config: Config,
        raft_member: Arc<Self::RaftMember>,
        metadata_store: Self::MetadataStore,
        file_store: Arc<Self::FileStore>,
    ) -> Result<Self::Service, Error>;
}

// Concrete factory implementation will be added when all components are ready
//
// pub struct FileSystemServiceImplFactory;
//
// impl FileSystemServiceFactory for FileSystemServiceImplFactory {
//     type RaftMember = crate::storage_raft_member::StorageRaftMemberImpl;
//     type MetadataStore = crate::metadata_store::MetadataStoreImpl;
//     type FileStore = crate::file_store::FileStoreImpl;
//     type Service = crate::filesystem_service::FileSystemServiceImpl;
//
//     fn create(
//         config: Config,
//         raft_member: Arc<Self::RaftMember>,
//         metadata_store: Self::MetadataStore,
//         file_store: Arc<Self::FileStore>,
//     ) -> Result<Self::Service, Error> {
//         FileSystemServiceImpl::new(config, raft_member, metadata_store, file_store)
//     }
// }
