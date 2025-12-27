//! Type conversion utilities between protobuf and internal types.
//!
//! This module provides conversion functions to translate between:
//! - Protobuf message types (from generated code)
//! - Internal WormFS types (FileId, ChunkId, StripeId, etc.)
//!
//! Conversions handle:
//! - UUID serialization/deserialization
//! - Error mapping
//! - Optional field handling

// Large error variants from tonic::Status (external library type)
#![allow(clippy::result_large_err)]

use tonic::Status;
use uuid::Uuid;

use crate::file_store::{ChunkId, FileId, StripeId};
use crate::storage_endpoint::proto::wormfs::common;

// ===== ID Conversions =====

/// Convert protobuf bytes to FileId.
pub fn bytes_to_file_id(bytes: &[u8]) -> Result<FileId, Status> {
    Uuid::from_slice(bytes)
        .map(FileId)
        .map_err(|e| Status::invalid_argument(format!("Invalid file_id: {}", e)))
}

/// Convert FileId to protobuf bytes.
pub fn file_id_to_bytes(file_id: FileId) -> Vec<u8> {
    file_id.0.as_bytes().to_vec()
}

/// Convert protobuf bytes to ChunkId.
pub fn bytes_to_chunk_id(bytes: &[u8]) -> Result<ChunkId, Status> {
    Uuid::from_slice(bytes)
        .map(ChunkId)
        .map_err(|e| Status::invalid_argument(format!("Invalid chunk_id: {}", e)))
}

/// Convert ChunkId to protobuf bytes.
pub fn chunk_id_to_bytes(chunk_id: ChunkId) -> Vec<u8> {
    chunk_id.0.as_bytes().to_vec()
}

/// Convert protobuf bytes to StripeId.
pub fn bytes_to_stripe_id(bytes: &[u8]) -> Result<StripeId, Status> {
    Uuid::from_slice(bytes)
        .map(StripeId)
        .map_err(|e| Status::invalid_argument(format!("Invalid stripe_id: {}", e)))
}

/// Convert StripeId to protobuf bytes.
pub fn stripe_id_to_bytes(stripe_id: StripeId) -> Vec<u8> {
    stripe_id.0.as_bytes().to_vec()
}

// ===== Algorithm Conversions =====

/// Convert protobuf ErasureAlgorithm to internal type.
pub fn proto_to_erasure_algorithm(
    algo: i32,
) -> Result<crate::file_store::ErasureAlgorithm, Status> {
    use crate::file_store::ErasureAlgorithm;
    use crate::storage_endpoint::proto::wormfs::common::ErasureAlgorithm as ProtoErasure;

    match ProtoErasure::try_from(algo) {
        Ok(ProtoErasure::ReedSolomon) => Ok(ErasureAlgorithm::ReedSolomon),
        _ => Err(Status::invalid_argument(format!(
            "Invalid erasure algorithm: {}",
            algo
        ))),
    }
}

/// Convert protobuf CompressionAlgorithm to internal type.
pub fn proto_to_compression_algorithm(
    algo: i32,
) -> Result<crate::file_store::CompressionAlgorithm, Status> {
    use crate::file_store::CompressionAlgorithm;
    use crate::storage_endpoint::proto::wormfs::common::CompressionAlgorithm as ProtoCompression;

    match ProtoCompression::try_from(algo) {
        Ok(ProtoCompression::None) => Ok(CompressionAlgorithm::None),
        Ok(ProtoCompression::Zstd) | Ok(ProtoCompression::Lz4) | Ok(ProtoCompression::Snappy) => {
            Err(Status::unimplemented(format!(
                "Compression algorithm {} not yet implemented",
                algo
            )))
        }
        _ => Err(Status::invalid_argument(format!(
            "Invalid compression algorithm: {}",
            algo
        ))),
    }
}

// ===== Error Conversions =====

/// Convert FileStore error to gRPC Status.
pub fn filestore_error_to_status(err: crate::file_store::Error) -> Status {
    use crate::file_store::Error;

    match err {
        Error::ChunkNotFound(_) => Status::not_found(err.to_string()),
        Error::DiskNotFound(_) => Status::not_found(err.to_string()),
        Error::InsufficientStorage { .. } => Status::failed_precondition(err.to_string()),
        Error::DiskFull { .. } => Status::resource_exhausted(err.to_string()),
        Error::ChecksumMismatch { .. } => Status::data_loss(err.to_string()),
        Error::ErasureEncodingFailed(_) => Status::internal(err.to_string()),
        Error::ErasureDecodingFailed(_) => Status::internal(err.to_string()),
        Error::ChunkWriteFailed { .. } => Status::internal(err.to_string()),
        Error::ChunkReadFailed { .. } => Status::internal(err.to_string()),
        _ => Status::internal(err.to_string()),
    }
}

/// Convert FileSystemService error to gRPC Status.
pub fn filesystem_error_to_status(err: crate::filesystem_service::Error) -> Status {
    use crate::filesystem_service::Error;

    match err {
        Error::NotFound(inode) => Status::not_found(format!("File not found: inode {}", inode)),
        Error::AlreadyExists(msg) => Status::already_exists(msg),
        Error::PermissionDenied(inode) => {
            Status::permission_denied(format!("Permission denied for inode {}", inode))
        }
        Error::DirectoryNotEmpty(inode) => {
            Status::failed_precondition(format!("Directory not empty: inode {}", inode))
        }
        Error::NotADirectory(inode) => {
            Status::failed_precondition(format!("Not a directory: inode {}", inode))
        }
        Error::IsADirectory(inode) => {
            Status::failed_precondition(format!("Is a directory: inode {}", inode))
        }
        Error::InvalidArgument(msg) => Status::invalid_argument(msg),
        Error::Io(e) => Status::internal(format!("I/O error: {}", e)),
        Error::LockConflict(msg) => Status::aborted(msg),
        Error::LockNotHeld(msg) => Status::failed_precondition(msg),
        _ => Status::internal(err.to_string()),
    }
}

/// Convert SnapshotStore error to gRPC Status.
pub fn snapshot_error_to_status(err: crate::snapshot_store::Error) -> Status {
    use crate::snapshot_store::Error;

    match err {
        Error::NotFound(snapshot_id) => {
            Status::not_found(format!("Snapshot {} not found", snapshot_id))
        }
        Error::IoError(e) => Status::internal(format!("I/O error: {}", e)),
        Error::Invalid(msg) => Status::invalid_argument(msg),
        Error::Corruption(msg) => Status::data_loss(msg),
        Error::ChecksumMismatch => Status::data_loss("Checksum mismatch"),
        _ => Status::internal(err.to_string()),
    }
}

/// Convert TransactionLogStore error to gRPC Status.
pub fn transaction_log_error_to_status(
    err: crate::transaction_log_store::types::LogError,
) -> Status {
    use crate::transaction_log_store::types::LogError;

    match err {
        LogError::EntryNotFound(index) => {
            Status::not_found(format!("Entry not found at index {}", index))
        }
        LogError::IoError(e) => Status::internal(format!("I/O error: {}", e)),
        LogError::ChecksumFailed(index) => {
            Status::data_loss(format!("Checksum failed at index {}", index))
        }
        LogError::InvalidIndex(index) => {
            Status::invalid_argument(format!("Invalid log index: {}", index))
        }
        LogError::InvalidRange(msg) => Status::invalid_argument(msg),
        _ => Status::internal(err.to_string()),
    }
}

/// Convert StorageRaftMember error to gRPC Status.
pub fn raft_error_to_status(err: crate::storage_raft_member::Error) -> Status {
    use crate::storage_raft_member::Error;

    match err {
        Error::NotLeader { leader } => {
            Status::failed_precondition(format!("Not leader (leader is: {:?})", leader))
        }
        Error::Timeout { timeout } => {
            Status::deadline_exceeded(format!("Operation timed out after {:?}", timeout))
        }
        Error::NoQuorum { available, total } => {
            Status::unavailable(format!("No quorum: {} of {} nodes", available, total))
        }
        Error::NodeNotFound(node_id) => Status::not_found(format!("Node {:?} not found", node_id)),
        Error::NodeAlreadyExists(node_id) => {
            Status::already_exists(format!("Node {:?} already exists", node_id))
        }
        _ => Status::internal(err.to_string()),
    }
}

/// Convert StorageNode error to gRPC Status.
pub fn storage_node_error_to_status(err: crate::storage_node::Error) -> Status {
    Status::internal(err.to_string())
}

// ===== StoragePolicy Conversion =====

/// Convert protobuf StoragePolicy to internal StoragePolicy.
pub fn proto_to_storage_policy(proto: &common::StoragePolicy) -> crate::file_store::StoragePolicy {
    // Convert stripe_size_kb to chunk_size
    // stripe_size = chunk_size * data_shards
    // So chunk_size = stripe_size / data_shards
    let stripe_size_bytes = (proto.stripe_size_kb as u64) * 1024;
    let chunk_size = if proto.data_shards > 0 {
        stripe_size_bytes / (proto.data_shards as u64)
    } else {
        64 * 1024 // Default 64KB chunk size if data_shards is 0
    };

    crate::file_store::StoragePolicy {
        data_shards: proto.data_shards as u8,
        parity_shards: proto.parity_shards as u8,
        chunk_size,
        compression: crate::file_store::CompressionAlgorithm::None, // TODO: Get from proto
    }
}

/// Convert internal StoragePolicy to protobuf StoragePolicy.
pub fn storage_policy_to_proto(policy: &crate::file_store::StoragePolicy) -> common::StoragePolicy {
    // Convert chunk_size to stripe_size_kb
    // stripe_size = chunk_size * data_shards
    let stripe_size_bytes = policy.chunk_size * (policy.data_shards as u64);
    let stripe_size_kb = (stripe_size_bytes / 1024) as u32;

    common::StoragePolicy {
        data_shards: policy.data_shards as u32,
        parity_shards: policy.parity_shards as u32,
        stripe_size_kb,
    }
}

// ===== LockType Conversion =====

/// Convert protobuf LockType to internal LockType.
pub fn proto_to_lock_type(
    proto_lock_type: i32,
) -> Result<crate::filesystem_service::LockType, Status> {
    use crate::filesystem_service::LockType;
    use crate::storage_endpoint::proto::wormfs::common::LockType as ProtoLockType;

    match ProtoLockType::try_from(proto_lock_type) {
        Ok(ProtoLockType::Read) => Ok(LockType::Read),
        Ok(ProtoLockType::Write) => Ok(LockType::Write),
        _ => Err(Status::invalid_argument("Invalid lock type")),
    }
}

/// Convert internal LockType to protobuf LockType.
pub fn lock_type_to_proto(lock_type: &crate::filesystem_service::LockType) -> i32 {
    use crate::filesystem_service::LockType;
    use crate::storage_endpoint::proto::wormfs::common::LockType as ProtoLockType;

    match lock_type {
        LockType::Read => ProtoLockType::Read as i32,
        LockType::Write => ProtoLockType::Write as i32,
    }
}
