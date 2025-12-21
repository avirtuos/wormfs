//! Re-exports of generated protobuf types for StorageEndpoint.
//!
//! This module provides convenient access to all gRPC service definitions
//! and message types generated from proto files.
//!
//! This module is only available when the `tonic` feature is enabled.

#[cfg(feature = "tonic")]
/// Generated protobuf types for WormFS services.
pub mod wormfs {
    /// Common types shared across all services.
    pub mod common {
        tonic::include_proto!("wormfs.common");
    }

    /// Filesystem operations for FUSE clients.
    pub mod filesystem {
        tonic::include_proto!("wormfs.filesystem");
    }

    /// Chunk storage and transfer operations.
    pub mod chunk {
        tonic::include_proto!("wormfs.chunk");
    }

    /// Administrative operations for cluster management.
    pub mod admin {
        tonic::include_proto!("wormfs.admin");
    }

    /// Snapshot transfer for Raft state synchronization.
    pub mod snapshot {
        tonic::include_proto!("wormfs.snapshot");
    }

    /// Transaction log access for Raft.
    pub mod transaction_log {
        tonic::include_proto!("wormfs.transaction_log");
    }

    /// gRPC health checking protocol.
    pub mod health {
        tonic::include_proto!("wormfs.health");
    }
}
