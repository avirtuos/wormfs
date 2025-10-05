//! Integration tests for metadata protocol messages
//!
//! These tests verify serialization, deserialization, and conversion of all
//! protocol buffer messages used in the WormFS metadata gossip protocol.

use std::path::PathBuf;
use std::time::SystemTime;
use uuid::Uuid;
use wormfs::{
    create_chunk_placed_event, create_file_created_event, create_file_deleted_event,
    create_protocol_version, validate_protocol_version, ChunkId, ChunkInfo, ChunkMetadata,
    ErasureCodingConfig, FileInfo, FileMetadata, ProtocolError, ProtocolVersion, StorageLocation,
    StorageLocationInfo, StripeId, StripeInfo, StripeMetadata,
};

#[test]
fn test_protocol_version_validation() {
    let version = create_protocol_version();
    assert_eq!(version.major, 1);
    assert_eq!(version.minor, 0);
    assert_eq!(version.patch, 0);

    // Valid version should pass
    assert!(validate_protocol_version(&version).is_ok());

    // Compatible minor/patch versions should pass (same major)
    let compatible = ProtocolVersion {
        major: 1,
        minor: 1,
        patch: 5,
    };
    assert!(validate_protocol_version(&compatible).is_ok());

    // Incompatible major version should fail
    let incompatible = ProtocolVersion {
        major: 2,
        minor: 0,
        patch: 0,
    };
    assert!(validate_protocol_version(&incompatible).is_err());
    match validate_protocol_version(&incompatible) {
        Err(ProtocolError::VersionMismatch { expected, got }) => {
            assert_eq!(expected.0, 1);
            assert_eq!(got.0, 2);
        }
        _ => panic!("Expected VersionMismatch error"),
    }
}

#[test]
fn test_file_metadata_roundtrip() {
    let original = FileMetadata::new(PathBuf::from("/test/data/file.txt"), 2048, 0o755);

    // Convert to protobuf
    let proto: FileInfo = (&original).into();

    // Verify proto fields
    assert_eq!(proto.file_id, original.file_id.to_string());
    assert_eq!(proto.path, "/test/data/file.txt");
    assert_eq!(proto.size, 2048);
    assert_eq!(proto.permissions, 0o755);
    assert_eq!(proto.stripe_count, 0);
    assert_eq!(proto.checksum, 0);

    // Convert back to domain type
    let converted: FileMetadata = (&proto).try_into().unwrap();

    // Verify all fields match
    assert_eq!(converted.file_id, original.file_id);
    assert_eq!(converted.path, original.path);
    assert_eq!(converted.size, original.size);
    assert_eq!(converted.permissions, original.permissions);
    assert_eq!(converted.stripe_count, original.stripe_count);
    assert_eq!(converted.checksum, original.checksum);
}

#[test]
fn test_storage_location_roundtrip() {
    let node_id = Uuid::new_v4();
    let original = StorageLocation::new(
        node_id,
        "ssd-01".to_string(),
        PathBuf::from("/mnt/storage/chunks/abc123def456"),
    );

    // Convert to protobuf
    let proto: StorageLocationInfo = (&original).into();

    // Verify proto fields
    assert_eq!(proto.node_id, node_id.to_string());
    assert_eq!(proto.disk_id, "ssd-01");
    assert_eq!(proto.path, "/mnt/storage/chunks/abc123def456");

    // Convert back to domain type
    let converted: StorageLocation = (&proto).try_into().unwrap();

    // Verify all fields match
    assert_eq!(converted.node_id, original.node_id);
    assert_eq!(converted.disk_id, original.disk_id);
    assert_eq!(converted.path, original.path);
}

#[test]
fn test_stripe_metadata_roundtrip() {
    let file_id = Uuid::new_v4();
    let config = ErasureCodingConfig::new(4, 2, 1024).unwrap();
    let original = StripeMetadata::new(file_id, 5, 1000, config);

    // Convert to protobuf
    let proto: StripeInfo = (&original).into();

    // Verify proto fields
    assert_eq!(proto.file_id, file_id.to_string());
    assert_eq!(proto.stripe_index, 5);
    assert_eq!(proto.original_size, 1000);
    assert_eq!(proto.chunk_count, 0);
    assert!(proto.erasure_config.is_some());

    let erasure_info = proto.erasure_config.as_ref().unwrap();
    assert_eq!(erasure_info.data_shards, 4);
    assert_eq!(erasure_info.parity_shards, 2);

    // Convert back to domain type
    let converted: StripeMetadata = (&proto).try_into().unwrap();

    // Verify all fields match
    assert_eq!(converted.file_id, original.file_id);
    assert_eq!(converted.stripe_index, original.stripe_index);
    assert_eq!(converted.original_size, original.original_size);
    assert_eq!(converted.chunk_count, original.chunk_count);
    assert_eq!(converted.erasure_config.data_shards, 4);
    assert_eq!(converted.erasure_config.parity_shards, 2);
}

#[test]
fn test_chunk_metadata_roundtrip() {
    let file_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let location = StorageLocation::new(
        node_id,
        "hdd-02".to_string(),
        PathBuf::from("/data/chunks/xyz789"),
    );

    let original = ChunkMetadata::new(file_id, 3, 2, 512, 0xABCDEF12, location);

    // Convert to protobuf
    let proto: ChunkInfo = (&original).into();

    // Verify proto fields
    assert_eq!(proto.file_id, file_id.to_string());
    assert_eq!(proto.stripe_index, 3);
    assert_eq!(proto.chunk_index, 2);
    assert_eq!(proto.size, 512);
    assert_eq!(proto.checksum, 0xABCDEF12);
    assert!(proto.storage_location.is_some());
    assert!(proto.last_verified.is_none());

    // Convert back to domain type
    let converted: ChunkMetadata = (&proto).try_into().unwrap();

    // Verify all fields match
    assert_eq!(converted.file_id, original.file_id);
    assert_eq!(converted.stripe_index, original.stripe_index);
    assert_eq!(converted.chunk_index, original.chunk_index);
    assert_eq!(converted.size, original.size);
    assert_eq!(converted.checksum, original.checksum);
    assert_eq!(converted.storage_location.node_id, node_id);
    assert!(converted.last_verified.is_none());
}

#[test]
fn test_file_created_event() {
    let file_metadata = FileMetadata::new(PathBuf::from("/test/new_file.dat"), 4096, 0o644);
    let peer_id = "12D3KooWTest".to_string();
    let node_id = Uuid::new_v4();

    let event = create_file_created_event(42, peer_id.clone(), node_id, &file_metadata);

    // Verify event structure
    assert_eq!(event.sequence_number, 42);
    assert!(event.version.is_some());
    assert!(event.originator.is_some());
    assert!(event.timestamp > 0);

    // Verify version
    let version = event.version.unwrap();
    assert_eq!(version.major, 1);

    // Verify originator
    let originator = event.originator.unwrap();
    assert_eq!(originator.peer_id, peer_id);
    assert_eq!(originator.node_id, node_id.to_string());

    // Verify event payload
    assert!(event.event.is_some());
}

#[test]
fn test_file_deleted_event() {
    let file_id = Uuid::new_v4();
    let path = PathBuf::from("/test/deleted.txt");
    let peer_id = "12D3KooWTest2".to_string();
    let node_id = Uuid::new_v4();

    let event = create_file_deleted_event(100, peer_id.clone(), node_id, file_id, path.clone());

    // Verify event structure
    assert_eq!(event.sequence_number, 100);
    assert!(event.version.is_some());
    assert!(event.originator.is_some());

    // Verify originator
    let originator = event.originator.unwrap();
    assert_eq!(originator.peer_id, peer_id);
    assert_eq!(originator.node_id, node_id.to_string());
}

#[test]
fn test_chunk_placed_event() {
    let file_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let location = StorageLocation::new(node_id, "disk1".to_string(), PathBuf::from("/chunks/1"));

    let chunk_metadata = ChunkMetadata::new(file_id, 0, 0, 256, 0x12345678, location);
    let peer_id = "12D3KooWTest3".to_string();

    let event = create_chunk_placed_event(50, peer_id.clone(), node_id, &chunk_metadata);

    // Verify event structure
    assert_eq!(event.sequence_number, 50);
    assert!(event.version.is_some());
    assert!(event.originator.is_some());
}

#[test]
fn test_invalid_uuid_conversion() {
    let bad_file_info = FileInfo {
        file_id: "not-a-valid-uuid".to_string(),
        path: "/test".to_string(),
        size: 100,
        permissions: 0o644,
        created_at: 0,
        modified_at: 0,
        accessed_at: 0,
        stripe_count: 0,
        checksum: 0,
    };

    let result: Result<FileMetadata, _> = (&bad_file_info).try_into();
    assert!(result.is_err());
    match result.unwrap_err() {
        ProtocolError::InvalidUuid(uuid) => {
            assert_eq!(uuid, "not-a-valid-uuid");
        }
        _ => panic!("Expected InvalidUuid error"),
    }
}

#[test]
fn test_missing_storage_location() {
    let chunk_info = ChunkInfo {
        file_id: Uuid::new_v4().to_string(),
        stripe_index: 0,
        chunk_index: 0,
        size: 100,
        checksum: 0,
        storage_location: None, // Missing required field
        created_at: 0,
        last_verified: None,
    };

    let result: Result<ChunkMetadata, _> = (&chunk_info).try_into();
    assert!(result.is_err());
    match result.unwrap_err() {
        ProtocolError::MissingField(field) => {
            assert_eq!(field, "storage_location");
        }
        _ => panic!("Expected MissingField error"),
    }
}

#[test]
fn test_missing_erasure_config() {
    let stripe_info = StripeInfo {
        file_id: Uuid::new_v4().to_string(),
        stripe_index: 0,
        original_size: 1000,
        chunk_count: 6,
        erasure_config: None, // Missing required field
        created_at: 0,
    };

    let result: Result<StripeMetadata, _> = (&stripe_info).try_into();
    assert!(result.is_err());
    match result.unwrap_err() {
        ProtocolError::MissingField(field) => {
            assert_eq!(field, "erasure_config");
        }
        _ => panic!("Expected MissingField error"),
    }
}

#[test]
fn test_multiple_event_creation() {
    let peer_id = "12D3KooWMulti".to_string();
    let node_id = Uuid::new_v4();

    // Create multiple events with increasing sequence numbers
    let file1 = FileMetadata::new(PathBuf::from("/file1.txt"), 100, 0o644);
    let event1 = create_file_created_event(1, peer_id.clone(), node_id, &file1);

    let file2 = FileMetadata::new(PathBuf::from("/file2.txt"), 200, 0o644);
    let event2 = create_file_created_event(2, peer_id.clone(), node_id, &file2);

    let file3_id = Uuid::new_v4();
    let event3 = create_file_deleted_event(
        3,
        peer_id.clone(),
        node_id,
        file3_id,
        PathBuf::from("/file3.txt"),
    );

    // Verify sequence numbers
    assert_eq!(event1.sequence_number, 1);
    assert_eq!(event2.sequence_number, 2);
    assert_eq!(event3.sequence_number, 3);

    // Verify all have same originator
    assert_eq!(
        event1.originator.as_ref().unwrap().peer_id,
        event2.originator.as_ref().unwrap().peer_id
    );
    assert_eq!(
        event2.originator.as_ref().unwrap().peer_id,
        event3.originator.as_ref().unwrap().peer_id
    );
}

#[test]
fn test_chunk_id_conversion() {
    let file_id = Uuid::new_v4();
    let chunk_id = ChunkId::new(file_id, 10, 5);

    // Create chunk metadata
    let location = StorageLocation::new(
        Uuid::new_v4(),
        "disk1".to_string(),
        PathBuf::from("/chunks/test"),
    );
    let chunk_metadata = ChunkMetadata::new(file_id, 10, 5, 512, 0xABCD, location);

    // Verify chunk_id() method returns correct ID
    assert_eq!(chunk_metadata.chunk_id(), chunk_id);

    // Verify stripe_id() method
    let stripe_id = chunk_metadata.stripe_id();
    assert_eq!(stripe_id.file_id, file_id);
    assert_eq!(stripe_id.stripe_index, 10);
}

#[test]
fn test_stripe_id_conversion() {
    let file_id = Uuid::new_v4();
    let stripe_id = StripeId::new(file_id, 7);

    let config = ErasureCodingConfig::new(4, 2, 1024).unwrap();
    let stripe_metadata = StripeMetadata::new(file_id, 7, 1000, config);

    // Verify stripe_id() method returns correct ID
    assert_eq!(stripe_metadata.stripe_id(), stripe_id);
}

#[test]
fn test_timestamp_preservation() {
    let now = SystemTime::now();
    let mut file_metadata = FileMetadata::new(PathBuf::from("/test.txt"), 100, 0o644);
    file_metadata.created_at = now;
    file_metadata.modified_at = now;
    file_metadata.accessed_at = now;

    // Convert to proto and back
    let proto: FileInfo = (&file_metadata).into();
    let converted: FileMetadata = (&proto).try_into().unwrap();

    // Timestamps should be preserved (within 1 second due to precision)
    let diff = file_metadata
        .created_at
        .duration_since(converted.created_at)
        .unwrap_or_else(|_| {
            converted
                .created_at
                .duration_since(file_metadata.created_at)
                .unwrap()
        });
    assert!(diff.as_secs() <= 1);
}
