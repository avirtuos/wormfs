// Protobuf type wrappers and conversion utilities
//
// This module provides conversions between our internal Rust types
// and the generated protobuf types for serialization.

use crate::raft::types::{
    FileMetadata as RaftFileMetadata, LockType as RaftLockType, MetadataOp as RaftMetadataOp,
    MetadataOpResponse as RaftMetadataOpResponse,
};
use prost::Message;
use uuid::Uuid;

// Include the generated protobuf code
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/wormfs.rs"));
}

use proto::{
    metadata_op, FileMetadata as ProtoFileMetadata, LockType as ProtoLockType,
    MetadataOp as ProtoMetadataOp, MetadataOpResponse as ProtoMetadataOpResponse,
};

// Re-export commonly used proto types
pub use proto::{
    ClusterStatus, LogEntry, LogId, NodeInfo, NodeState, RaftRequest as ProtoRaftRequest,
    RaftResponse as ProtoRaftResponse, ReadMode,
};

/// Error type for protobuf conversion operations
#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("Invalid UUID bytes: expected 16 bytes, got {0}")]
    InvalidUuid(usize),

    #[error("Protobuf encode error: {0}")]
    EncodeError(#[from] prost::EncodeError),

    #[error("Protobuf decode error: {0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid enum value: {0}")]
    InvalidEnum(String),
}

pub type Result<T> = std::result::Result<T, ProtoError>;

// ============================================================================
// UUID Conversion Helpers
// ============================================================================

fn uuid_to_bytes(uuid: Uuid) -> Vec<u8> {
    uuid.as_bytes().to_vec()
}

fn bytes_to_uuid(bytes: &[u8]) -> Result<Uuid> {
    if bytes.len() != 16 {
        return Err(ProtoError::InvalidUuid(bytes.len()));
    }
    let mut array = [0u8; 16];
    array.copy_from_slice(bytes);
    Ok(Uuid::from_bytes(array))
}

// ============================================================================
// LockType Conversion
// ============================================================================

impl From<RaftLockType> for ProtoLockType {
    fn from(lock_type: RaftLockType) -> Self {
        match lock_type {
            RaftLockType::Read => ProtoLockType::Read,
            RaftLockType::Write => ProtoLockType::Write,
        }
    }
}

impl TryFrom<ProtoLockType> for RaftLockType {
    type Error = ProtoError;

    fn try_from(proto: ProtoLockType) -> Result<Self> {
        match proto {
            ProtoLockType::Read => Ok(RaftLockType::Read),
            ProtoLockType::Write => Ok(RaftLockType::Write),
            ProtoLockType::Unspecified => {
                Err(ProtoError::InvalidEnum("Unspecified lock type".to_string()))
            }
        }
    }
}

impl TryFrom<i32> for RaftLockType {
    type Error = ProtoError;

    fn try_from(value: i32) -> Result<Self> {
        ProtoLockType::try_from(value)
            .map_err(|_| ProtoError::InvalidEnum(format!("lock_type: {}", value)))?
            .try_into()
    }
}

// ============================================================================
// FileMetadata Conversion
// ============================================================================

impl From<RaftFileMetadata> for ProtoFileMetadata {
    fn from(metadata: RaftFileMetadata) -> Self {
        ProtoFileMetadata {
            file_id: uuid_to_bytes(metadata.file_id),
            size: metadata.size,
            permissions: metadata.permissions,
            uid: metadata.uid,
            gid: metadata.gid,
            created_at: metadata.created_at,
            modified_at: metadata.modified_at,
            accessed_at: metadata.accessed_at,
            stripe_size: metadata.stripe_size,
            data_shards: metadata.data_shards as u32,
            parity_shards: metadata.parity_shards as u32,
        }
    }
}

impl TryFrom<ProtoFileMetadata> for RaftFileMetadata {
    type Error = ProtoError;

    fn try_from(proto: ProtoFileMetadata) -> Result<Self> {
        Ok(RaftFileMetadata {
            file_id: bytes_to_uuid(&proto.file_id)?,
            size: proto.size,
            permissions: proto.permissions,
            uid: proto.uid,
            gid: proto.gid,
            created_at: proto.created_at,
            modified_at: proto.modified_at,
            accessed_at: proto.accessed_at,
            stripe_size: proto.stripe_size,
            data_shards: proto.data_shards as u8,
            parity_shards: proto.parity_shards as u8,
        })
    }
}

// ============================================================================
// MetadataOp Conversion
// ============================================================================

impl From<RaftMetadataOp> for ProtoMetadataOp {
    fn from(op: RaftMetadataOp) -> Self {
        let operation = match op {
            RaftMetadataOp::CreateFile { metadata } => {
                metadata_op::Operation::CreateFile(metadata_op::CreateFile {
                    metadata: Some(metadata.into()),
                })
            }
            RaftMetadataOp::UpdateFile { file_id, metadata } => {
                metadata_op::Operation::UpdateFile(metadata_op::UpdateFile {
                    file_id: uuid_to_bytes(file_id),
                    metadata: Some(metadata.into()),
                })
            }
            RaftMetadataOp::DeleteFile { file_id } => {
                metadata_op::Operation::DeleteFile(metadata_op::DeleteFile {
                    file_id: uuid_to_bytes(file_id),
                })
            }
            RaftMetadataOp::RegisterChunk {
                chunk_id,
                node_id,
                stripe_id,
                file_id,
            } => metadata_op::Operation::RegisterChunk(metadata_op::RegisterChunk {
                chunk_id: uuid_to_bytes(chunk_id),
                node_id,
                stripe_id: uuid_to_bytes(stripe_id),
                file_id: uuid_to_bytes(file_id),
            }),
            RaftMetadataOp::UpdateChunkLocation {
                chunk_id,
                new_node_id,
            } => metadata_op::Operation::UpdateChunkLocation(metadata_op::UpdateChunkLocation {
                chunk_id: uuid_to_bytes(chunk_id),
                new_node_id,
            }),
            RaftMetadataOp::RemoveChunk { chunk_id } => {
                metadata_op::Operation::RemoveChunk(metadata_op::RemoveChunk {
                    chunk_id: uuid_to_bytes(chunk_id),
                })
            }
            RaftMetadataOp::AcquireLock {
                file_id,
                lock_type,
                client_id,
            } => metadata_op::Operation::AcquireLock(metadata_op::AcquireLock {
                file_id: uuid_to_bytes(file_id),
                lock_type: ProtoLockType::from(lock_type) as i32,
                client_id,
            }),
            RaftMetadataOp::ReleaseLock { file_id, client_id } => {
                metadata_op::Operation::ReleaseLock(metadata_op::ReleaseLock {
                    file_id: uuid_to_bytes(file_id),
                    client_id,
                })
            }
            RaftMetadataOp::ExtendLock { file_id, client_id } => {
                metadata_op::Operation::ExtendLock(metadata_op::ExtendLock {
                    file_id: uuid_to_bytes(file_id),
                    client_id,
                })
            }
            RaftMetadataOp::AddNode { node_id, address } => {
                metadata_op::Operation::AddNode(metadata_op::AddNode { node_id, address })
            }
            RaftMetadataOp::RemoveNode { node_id } => {
                metadata_op::Operation::RemoveNode(metadata_op::RemoveNode { node_id })
            }
        };

        ProtoMetadataOp {
            operation: Some(operation),
        }
    }
}

impl TryFrom<ProtoMetadataOp> for RaftMetadataOp {
    type Error = ProtoError;

    fn try_from(proto: ProtoMetadataOp) -> Result<Self> {
        let operation = proto
            .operation
            .ok_or_else(|| ProtoError::MissingField("operation".to_string()))?;

        Ok(match operation {
            metadata_op::Operation::CreateFile(op) => {
                let metadata = op
                    .metadata
                    .ok_or_else(|| ProtoError::MissingField("metadata".to_string()))?;
                RaftMetadataOp::CreateFile {
                    metadata: metadata.try_into()?,
                }
            }
            metadata_op::Operation::UpdateFile(op) => RaftMetadataOp::UpdateFile {
                file_id: bytes_to_uuid(&op.file_id)?,
                metadata: op
                    .metadata
                    .ok_or_else(|| ProtoError::MissingField("metadata".to_string()))?
                    .try_into()?,
            },
            metadata_op::Operation::DeleteFile(op) => RaftMetadataOp::DeleteFile {
                file_id: bytes_to_uuid(&op.file_id)?,
            },
            metadata_op::Operation::RegisterChunk(op) => RaftMetadataOp::RegisterChunk {
                chunk_id: bytes_to_uuid(&op.chunk_id)?,
                node_id: op.node_id,
                stripe_id: bytes_to_uuid(&op.stripe_id)?,
                file_id: bytes_to_uuid(&op.file_id)?,
            },
            metadata_op::Operation::UpdateChunkLocation(op) => {
                RaftMetadataOp::UpdateChunkLocation {
                    chunk_id: bytes_to_uuid(&op.chunk_id)?,
                    new_node_id: op.new_node_id,
                }
            }
            metadata_op::Operation::RemoveChunk(op) => RaftMetadataOp::RemoveChunk {
                chunk_id: bytes_to_uuid(&op.chunk_id)?,
            },
            metadata_op::Operation::AcquireLock(op) => RaftMetadataOp::AcquireLock {
                file_id: bytes_to_uuid(&op.file_id)?,
                lock_type: op.lock_type.try_into()?,
                client_id: op.client_id,
            },
            metadata_op::Operation::ReleaseLock(op) => RaftMetadataOp::ReleaseLock {
                file_id: bytes_to_uuid(&op.file_id)?,
                client_id: op.client_id,
            },
            metadata_op::Operation::ExtendLock(op) => RaftMetadataOp::ExtendLock {
                file_id: bytes_to_uuid(&op.file_id)?,
                client_id: op.client_id,
            },
            metadata_op::Operation::AddNode(op) => RaftMetadataOp::AddNode {
                node_id: op.node_id,
                address: op.address,
            },
            metadata_op::Operation::RemoveNode(op) => RaftMetadataOp::RemoveNode {
                node_id: op.node_id,
            },
        })
    }
}

// ============================================================================
// MetadataOpResponse Conversion
// ============================================================================

impl From<RaftMetadataOpResponse> for ProtoMetadataOpResponse {
    fn from(response: RaftMetadataOpResponse) -> Self {
        match response {
            RaftMetadataOpResponse::Success => ProtoMetadataOpResponse {
                success: true,
                error_message: String::new(),
            },
            RaftMetadataOpResponse::Error(msg) => ProtoMetadataOpResponse {
                success: false,
                error_message: msg,
            },
        }
    }
}

impl From<ProtoMetadataOpResponse> for RaftMetadataOpResponse {
    fn from(proto: ProtoMetadataOpResponse) -> Self {
        if proto.success {
            RaftMetadataOpResponse::Success
        } else {
            RaftMetadataOpResponse::Error(proto.error_message)
        }
    }
}

// ============================================================================
// Serialization Helpers
// ============================================================================

/// Serialize a MetadataOp to protobuf bytes
pub fn serialize_metadata_op(op: &RaftMetadataOp) -> Result<Vec<u8>> {
    let proto: ProtoMetadataOp = op.clone().into();
    let mut buf = Vec::new();
    proto.encode(&mut buf)?;
    Ok(buf)
}

/// Deserialize a MetadataOp from protobuf bytes
pub fn deserialize_metadata_op(bytes: &[u8]) -> Result<RaftMetadataOp> {
    let proto = ProtoMetadataOp::decode(bytes)?;
    proto.try_into()
}

/// Serialize a MetadataOpResponse to protobuf bytes
pub fn serialize_response(response: &RaftMetadataOpResponse) -> Result<Vec<u8>> {
    let proto: ProtoMetadataOpResponse = response.clone().into();
    let mut buf = Vec::new();
    proto.encode(&mut buf)?;
    Ok(buf)
}

/// Deserialize a MetadataOpResponse from protobuf bytes
pub fn deserialize_response(bytes: &[u8]) -> Result<RaftMetadataOpResponse> {
    let proto = ProtoMetadataOpResponse::decode(bytes)?;
    Ok(proto.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_roundtrip() {
        let uuid = Uuid::new_v4();
        let bytes = uuid_to_bytes(uuid);
        let recovered = bytes_to_uuid(&bytes).unwrap();
        assert_eq!(uuid, recovered);
    }

    #[test]
    fn test_lock_type_conversion() {
        assert_eq!(
            RaftLockType::Read,
            RaftLockType::try_from(ProtoLockType::from(RaftLockType::Read)).unwrap()
        );
        assert_eq!(
            RaftLockType::Write,
            RaftLockType::try_from(ProtoLockType::from(RaftLockType::Write)).unwrap()
        );
    }

    #[test]
    fn test_metadata_op_roundtrip() {
        let file_id = Uuid::new_v4();
        let op = RaftMetadataOp::DeleteFile { file_id };

        let bytes = serialize_metadata_op(&op).unwrap();
        let recovered = deserialize_metadata_op(&bytes).unwrap();

        match recovered {
            RaftMetadataOp::DeleteFile {
                file_id: recovered_id,
            } => {
                assert_eq!(file_id, recovered_id);
            }
            _ => panic!("Wrong operation type"),
        }
    }

    #[test]
    fn test_response_conversion() {
        let success = RaftMetadataOpResponse::Success;
        let proto: ProtoMetadataOpResponse = success.into();
        assert!(proto.success);
        assert!(proto.error_message.is_empty());

        let error = RaftMetadataOpResponse::Error("test error".to_string());
        let proto: ProtoMetadataOpResponse = error.into();
        assert!(!proto.success);
        assert_eq!(proto.error_message, "test error");
    }
}
