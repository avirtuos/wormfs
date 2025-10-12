//! # WormFS - Distributed Erasure-Coded Filesystem
//!
//! WormFS is a distributed filesystem that uses erasure coding (Reed-Solomon) to provide
//! durability and availability of file data across a cluster of storage nodes. It uses
//! Raft consensus for strong consistency of metadata operations and libp2p for peer-to-peer
//! networking.
//!
//! ## Architecture
//!
//! The system is built around 11 key components:
//!
//! - **StorageNode**: Top-level orchestrator that initializes and wires together all components
//! - **StorageRaftMember**: Raft consensus implementation for metadata consistency
//! - **StorageNetwork**: Peer-to-peer networking layer using libp2p
//! - **FileStore**: Erasure coding and chunk storage management
//! - **MetadataStore**: SQLite-based metadata persistence layer
//! - **SnapshotStore**: Metadata snapshot management for Raft log compaction
//! - **TransactionLogStore**: Persistent Raft log storage
//! - **StorageEndpoint**: gRPC API server for client and node-to-node communication
//! - **StorageWatchdog**: Data integrity monitoring and repair
//! - **MetricService**: Centralized metrics collection and aggregation
//! - **FileSystemService**: FUSE filesystem API implementation
//!
//! ## Design Principles
//!
//! - **Strong Consistency**: Raft consensus ensures linearizable metadata operations
//! - **High Durability**: Erasure coding provides configurable redundancy (e.g., 6+3)
//! - **Scalability**: Peer-to-peer architecture with no single point of failure
//! - **Modularity**: Clean component boundaries with well-defined traits
//! - **Async-First**: Built on Tokio for high-performance async I/O

pub mod file_store;
pub mod filesystem_service;
pub mod metadata_store;
pub mod metric_service;
pub mod snapshot_store;
pub mod storage_endpoint;
pub mod storage_network;
pub mod storage_node;
pub mod storage_raft_member;
pub mod storage_watchdog;
pub mod transaction_log_store;

// Re-export commonly used traits
pub use file_store::{FileStore, FuseFileSystem};
pub use filesystem_service::FileSystemService;
pub use metadata_store::MetadataStore;
pub use metric_service::MetricService;
pub use snapshot_store::SnapshotStore;
pub use storage_endpoint::StorageEndpoint;
pub use storage_network::StorageNetwork;
pub use storage_node::StorageNode;
pub use storage_raft_member::StorageRaftMember;
pub use storage_watchdog::StorageWatchdog;
pub use transaction_log_store::TransactionLogStore;

// Test utilities module (only available with test-utils feature or during testing)
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
