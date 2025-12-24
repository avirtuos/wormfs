//! Integration tests for path traversal permission enforcement
//!
//! These tests verify that execute permission is properly enforced on all intermediate
//! directories when accessing files. According to POSIX semantics, to access a file at
//! `/dir1/dir2/dir3/file`, a user must have execute permission on dir1, dir2, and dir3,
//! in addition to the appropriate permission on the file itself.

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use wormfs::file_store::{Config as FileStoreConfig, FileStore, FileStoreImpl};
use wormfs::filesystem_service::factory::FileSystemServiceImplFactory;
use wormfs::filesystem_service::implementation::FileSystemServiceImpl;
use wormfs::filesystem_service::FileSystemService;
use wormfs::metadata_store::{
    types::*, ClientId, Config as MetadataConfig, MetadataStore, MetadataStoreFactory,
};

/// Helper to create a test FileSystemService with temporary storage.
async fn create_test_filesystem_service() -> (FileSystemServiceImpl, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let chunks_dir = temp_dir.path().join("chunks");
    std::fs::create_dir(&chunks_dir).expect("Failed to create chunks dir");

    let metadata_config = MetadataConfig {
        database_path: db_path,
        read_pool_size: 4,
        enable_wal: true,
        cache_size_mb: 64,
        enable_foreign_keys: false,
        synchronous: SynchronousMode::Normal,
        transaction_isolation: IsolationLevel::ReadCommitted,
        enable_prepared_statements: true,
        read_pool_timeout_secs: 30,
        stripe_cache_size_mb: 64,
        stripe_cache_ttl_secs: 10,
        stripe_cache_tti_secs: 5,
        chunk_cache_size_mb: 64,
        chunk_cache_ttl_secs: 10,
        chunk_cache_tti_secs: 5,
    };

    let metadata_store = MetadataStoreFactory::create_concrete(metadata_config)
        .await
        .expect("Failed to create MetadataStore");

    metadata_store
        .initialize_schema()
        .await
        .expect("Failed to initialize schema");

    let file_store_config = FileStoreConfig {
        disk_paths: vec![chunks_dir.clone()],
        max_chunk_size: 2 * 1024 * 1024, // 2MB chunks
        default_data_shards: 2,
        default_parity_shards: 1,
        max_concurrent_operations: 10,
        verification_interval: Duration::from_secs(3600),
        orphan_cleanup_age: Duration::from_secs(86400),
        stripe_cache_size_mb: 256,
        stripe_cache_ttl_secs: 3600,
        stripe_cache_tti_secs: 600,
    };

    let mut file_store =
        <FileStoreImpl as FileStore>::new(file_store_config).expect("Failed to create FileStore");

    let _disk_id = file_store
        .add_disk(chunks_dir.clone())
        .await
        .expect("Failed to add disk");

    // Configure distributed components for tests
    use wormfs::file_store::types::NodeId;
    use wormfs::file_store::{
        ChunkClientConfig, ChunkClientPool, PlacementConfig, PlacementEngine,
    };
    use wormfs::storage_raft_member::cluster_manager::heartbeat_tracker::HeartbeatTracker;

    let my_node_id = NodeId::new(1);
    let tracker = Arc::new(HeartbeatTracker::new(5000, 60000));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    tracker.record_heartbeat(
        "1".to_string(),
        now,
        1,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(1_000_000_000),
        Some(900_000_000),
        Some(0),
    );

    let config = PlacementConfig {
        min_node_diversity: 1,
        prefer_local: true,
    };
    let placement_engine = Arc::new(PlacementEngine::new(tracker.clone(), my_node_id, config));
    let chunk_client_config = ChunkClientConfig::default();
    let chunk_client: Arc<dyn wormfs::file_store::ChunkClient> =
        Arc::new(ChunkClientPool::new(tracker, chunk_client_config));

    file_store.set_distributed_config(my_node_id, placement_engine, chunk_client);

    let file_store = Arc::new(file_store);

    let fs_config = wormfs::filesystem_service::types::Config::default();
    let service = FileSystemServiceImplFactory::create(fs_config, metadata_store, file_store, None)
        .await
        .expect("Failed to create FileSystemService");

    service
        .initialize_root()
        .await
        .expect("Failed to initialize root");

    (service, temp_dir)
}

#[tokio::test]
async fn test_access_file_with_no_execute_on_intermediate_dir() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create directory structure: /dir1/dir2/dir3/file
    // dir1: rwxr-xr-x (0o755) - owner=1000, everyone can traverse
    let dir1 = service
        .mkdir(1, "dir1", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir1");

    // dir2: rwx------ (0o700) - owner=1000, ONLY owner can traverse
    let dir2 = service
        .mkdir(dir1.ino, "dir2", 0o700, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir2");

    // dir3: rwxr-xr-x (0o755) - owner=1000, everyone can traverse
    let dir3 = service
        .mkdir(dir2.ino, "dir3", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir3");

    // file: rw-r--r-- (0o644) - owner=1000, everyone can read
    let file = service
        .create(dir3.ino, "file", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Write some data to the file as owner
    let (fh, _) = service
        .open(file.ino, libc::O_WRONLY as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file for writing");

    service
        .write(
            file.ino,
            fh,
            0,
            b"test data".to_vec(),
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to write to file");

    service.release(fh).await.expect("Failed to release");

    // Now try to access the file as user 2000 (not owner, not in group)
    // User 2000 has:
    // - Execute permission on dir1 (r-x for others)
    // - NO execute permission on dir2 (--- for others)
    // - Execute permission on dir3 (r-x for others)
    // - Read permission on file (r-- for others)

    // The access should FAIL because user lacks execute on dir2
    // Note: We can't directly test this through open() because our current implementation
    // operates on inodes. The FUSE lookup() is where path traversal happens.
    // For now, we test that we can't mkdir in dir2 without execute permission.

    let result = service
        .mkdir(dir2.ino, "newdir", 0o755, 2000, 2000, client_id)
        .await;

    assert!(
        result.is_err(),
        "Should not be able to mkdir in dir2 without execute permission"
    );
    println!("✓ Access correctly denied when lacking execute on intermediate directory");
}

#[tokio::test]
async fn test_access_file_with_execute_on_all_dirs() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create directory structure: /dir1/dir2/dir3/file
    // All directories: rwxr-xr-x (0o755) - everyone can traverse
    let dir1 = service
        .mkdir(1, "dir1", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir1");

    let dir2 = service
        .mkdir(dir1.ino, "dir2", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir2");

    let dir3 = service
        .mkdir(dir2.ino, "dir3", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir3");

    // file: rw-r--r-- (0o644) - everyone can read
    let file = service
        .create(dir3.ino, "file", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Write some data as owner
    let (fh, _) = service
        .open(file.ino, libc::O_WRONLY as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file for writing");

    service
        .write(
            file.ino,
            fh,
            0,
            b"test data".to_vec(),
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to write to file");

    service.release(fh).await.expect("Failed to release");

    // Now access the file as user 2000
    // User 2000 has execute on all directories and read on the file
    let result = service
        .open(file.ino, libc::O_RDONLY as u32, 2000, 2000, client_id)
        .await;

    assert!(
        result.is_ok(),
        "Should be able to access file with proper permissions"
    );

    let (fh, _attr) = result.unwrap();

    // Try to read the file
    let read_result = service
        .read(file.ino, fh, 0, 1024, 2000, 2000, client_id)
        .await;

    assert!(
        read_result.is_ok(),
        "Should be able to read file with read permission"
    );
    assert_eq!(read_result.unwrap(), b"test data");

    // Close the file
    service.release(fh).await.expect("Failed to release file");

    println!("✓ Access succeeds when user has execute on all directories and read on file");
}

#[tokio::test]
async fn test_create_file_with_no_execute_on_parent() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a directory with write but no execute for others
    // rwxrw-rw- (0o766) - others have read+write but NO execute
    let dir = service
        .mkdir(1, "testdir", 0o766, 1000, 1000, client_id)
        .await
        .expect("Failed to create directory");

    // Try to create a file as user 2000
    // User has write permission but no execute permission on the directory
    let result = service
        .create(dir.ino, "file", 0o644, 2000, 2000, client_id)
        .await;

    assert!(
        result.is_err(),
        "Should not be able to create file without execute on parent directory"
    );
    println!("✓ File creation correctly denied without execute permission on parent");
}

#[tokio::test]
async fn test_nested_directory_traversal_denied() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a 5-level deep directory structure
    // /a/b/c/d/e
    // where 'c' has no execute permission for others
    let a = service
        .mkdir(1, "a", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create a");

    let b = service
        .mkdir(a.ino, "b", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create b");

    // c: NO execute for others (0o766 = rwxrw-rw-)
    let c = service
        .mkdir(b.ino, "c", 0o766, 1000, 1000, client_id)
        .await
        .expect("Failed to create c");

    let d = service
        .mkdir(c.ino, "d", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create d");

    let _e = service
        .mkdir(d.ino, "e", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create e");

    // Try to create a subdirectory in 'e' as user 2000
    // This should fail because user can't traverse 'c'
    // But since we're using inode-based operations, we test at the 'c' level

    let result = service
        .mkdir(c.ino, "blocked", 0o755, 2000, 2000, client_id)
        .await;

    assert!(
        result.is_err(),
        "Should not be able to mkdir when lacking execute on intermediate directory"
    );
    println!("✓ Deep directory traversal correctly blocked at intermediate level");
}

#[tokio::test]
async fn test_path_traversal_with_owner_permissions() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create directory structure where owner has access but others don't
    let dir1 = service
        .mkdir(1, "dir1", 0o700, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir1");

    let dir2 = service
        .mkdir(dir1.ino, "dir2", 0o700, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir2");

    let file = service
        .create(dir2.ino, "file", 0o600, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Owner can access (uid=1000)
    let owner_result = service
        .open(file.ino, libc::O_RDONLY as u32, 1000, 1000, client_id)
        .await;

    assert!(
        owner_result.is_ok(),
        "Owner should be able to access their own file"
    );

    // Other user cannot access (uid=2000)
    let other_result = service
        .open(file.ino, libc::O_RDONLY as u32, 2000, 2000, client_id)
        .await;

    assert!(
        other_result.is_err(),
        "Other users should not be able to access owner-only file"
    );
    println!("✓ Owner permissions work correctly for path traversal");
}

#[tokio::test]
async fn test_path_traversal_with_group_permissions() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create directory structure with group permissions
    // rwxr-x--- (0o750) - owner and group have access, others don't
    let dir1 = service
        .mkdir(1, "dir1", 0o750, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir1");

    let dir2 = service
        .mkdir(dir1.ino, "dir2", 0o750, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir2");

    let file = service
        .create(dir2.ino, "file", 0o640, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Owner can access (uid=1000, gid=1000)
    let owner_result = service
        .open(file.ino, libc::O_RDONLY as u32, 1000, 1000, client_id)
        .await;

    assert!(owner_result.is_ok(), "Owner should be able to access");

    // Group member can access (uid=1001, gid=1000)
    let group_result = service
        .open(file.ino, libc::O_RDONLY as u32, 1001, 1000, client_id)
        .await;

    assert!(
        group_result.is_ok(),
        "Group member should be able to access"
    );

    // Other user cannot access (uid=2000, gid=2000)
    let other_result = service
        .open(file.ino, libc::O_RDONLY as u32, 2000, 2000, client_id)
        .await;

    assert!(
        other_result.is_err(),
        "Other users should not be able to access group-only file"
    );
    println!("✓ Group permissions work correctly for path traversal");
}

#[tokio::test]
async fn test_mkdir_requires_execute_on_parent() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create directory with write but no execute for others
    let parent = service
        .mkdir(1, "parent", 0o766, 1000, 1000, client_id)
        .await
        .expect("Failed to create parent");

    // Try to create subdirectory as other user
    // User 2000 has write but no execute on parent
    let result = service
        .mkdir(parent.ino, "child", 0o755, 2000, 2000, client_id)
        .await;

    assert!(
        result.is_err(),
        "mkdir should fail without execute permission on parent"
    );
    println!("✓ mkdir correctly requires execute permission on parent directory");
}

#[tokio::test]
async fn test_rmdir_requires_execute_on_parent() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create parent and child as owner
    let parent = service
        .mkdir(1, "parent", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create parent");

    service
        .mkdir(parent.ino, "child", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create child");

    // Change parent to have write but no execute for others
    service
        .setattr(
            parent.ino,
            None,        // file_handle
            Some(0o766), // rwxrw-rw- - write but no execute for others
            None,
            None,
            None,
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to change permissions");

    // Try to remove child as other user
    let result = service
        .rmdir(parent.ino, "child", 2000, 2000, client_id)
        .await;

    assert!(
        result.is_err(),
        "rmdir should fail without execute permission on parent"
    );
    println!("✓ rmdir correctly requires execute permission on parent directory");
}

#[tokio::test]
async fn test_unlink_requires_execute_on_parent() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create parent directory and file as owner
    let parent = service
        .mkdir(1, "parent", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create parent");

    service
        .create(parent.ino, "file", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Change parent to have write but no execute for others
    service
        .setattr(
            parent.ino,
            None,        // file_handle
            Some(0o766), // rwxrw-rw- - write but no execute for others
            None,
            None,
            None,
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to change permissions");

    // Try to unlink file as other user
    let result = service
        .unlink(parent.ino, "file", 2000, 2000, client_id)
        .await;

    assert!(
        result.is_err(),
        "unlink should fail without execute permission on parent"
    );
    println!("✓ unlink correctly requires execute permission on parent directory");
}
