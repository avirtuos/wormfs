//! FilesystemService gRPC implementation.
//!
//! Provides file and directory operations for FUSE clients,
//! delegating to the FileSystemService component.

// Large error variants from tonic::Status (external library type)
#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use super::conversions::{
    bytes_to_file_id, file_id_to_bytes, filesystem_error_to_status, proto_to_lock_type,
};
use super::GRPC_STREAM_CHANNEL_BUFFER_SIZE;
use crate::filesystem_service::{ClientId, FileSystemService};
use crate::storage_endpoint::proto::wormfs::common::FileMetadata as ProtoFileMetadata;
use crate::storage_endpoint::proto::wormfs::filesystem::filesystem_service_server::FilesystemService;
use crate::storage_endpoint::proto::wormfs::filesystem::*;

/// FilesystemService gRPC implementation.
///
/// Delegates filesystem operations to the FileSystemService component.
///
/// ## Path vs Inode Handling
///
/// The gRPC API uses paths (for creation) and file_ids (UUIDs) for other operations,
/// while the FileSystemService trait uses inodes (u64). This implementation handles:
/// - Path parsing: "/path/to/file" → (parent_inode, "file")
/// - FileId mapping: UUID ↔ inode
///
/// TODO: Implement proper path resolution and file_id↔inode mapping.
/// For now, uses placeholder mappings.
pub struct FilesystemServiceImpl<F: FileSystemService> {
    filesystem: Arc<F>,
}

impl<F: FileSystemService> FilesystemServiceImpl<F> {
    /// Create a new FilesystemService.
    ///
    /// # Arguments
    ///
    /// * `filesystem` - FileSystemService instance for filesystem operations
    pub fn new(filesystem: Arc<F>) -> Self {
        Self { filesystem }
    }

    /// Parse a path into (parent_inode, name).
    ///
    /// This method extracts the parent directory path and filename from a full path,
    /// then resolves the parent directory to its inode using the metadata store.
    async fn parse_path(&self, path: &str) -> Result<(u64, String), Status> {
        let path = path.trim_start_matches('/');
        if path.is_empty() {
            return Err(Status::invalid_argument("Empty path"));
        }

        // Build path and extract filename
        let path_buf = std::path::PathBuf::from(format!("/{}", path));
        let name = path_buf
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| Status::invalid_argument("Invalid path"))?
            .to_string();

        // Get parent path
        let parent_path = path_buf.parent().unwrap_or(std::path::Path::new("/"));

        // Resolve parent to inode (root "/" has inode 1)
        const ROOT_INODE: u64 = 1;
        let parent_inode = if parent_path == std::path::Path::new("/") {
            ROOT_INODE
        } else {
            self.filesystem
                .resolve_path(parent_path)
                .await
                .map_err(filesystem_error_to_status)?
        };

        Ok((parent_inode, name))
    }

    /// Convert file_id bytes to inode.
    ///
    /// TODO: Implement proper file_id ↔ inode mapping.
    /// For now, uses a simple hash of the UUID.
    fn file_id_to_inode(&self, file_id_bytes: &[u8]) -> Result<u64, Status> {
        let file_id = bytes_to_file_id(file_id_bytes)?;

        // TODO: Look up inode from file_id in metadata store
        // For now, use a hash of the UUID as a placeholder
        Ok(file_id.0.as_u128() as u64)
    }

    /// Convert inode to file_id bytes.
    ///
    /// TODO: Implement proper inode → file_id mapping.
    /// For now, creates a deterministic UUID from the inode.
    fn inode_to_file_id(&self, inode: u64) -> Vec<u8> {
        // TODO: Look up file_id from inode in metadata store
        // For now, create a deterministic UUID from the inode
        let uuid = uuid::Uuid::from_u128(inode as u128);
        file_id_to_bytes(crate::file_store::FileId(uuid))
    }

    /// Convert FileAttr to protobuf FileMetadata.
    fn file_attr_to_proto(&self, attr: &crate::filesystem_service::FileAttr) -> ProtoFileMetadata {
        ProtoFileMetadata {
            inode: attr.ino,
            path: String::new(), // TODO: Maintain inode→path mapping
            size: attr.size,
            permissions: attr.perm as u32,
            uid: attr.uid,
            gid: attr.gid,
            created_at: attr
                .ctime
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            modified_at: attr
                .mtime
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            accessed_at: attr
                .atime
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        }
    }
}

#[tonic::async_trait]
impl<F: FileSystemService + 'static> FilesystemService for FilesystemServiceImpl<F> {
    // File operations

    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        let req = request.into_inner();
        debug!("CreateFile request: path={}", req.path);

        // Parse path into (parent_inode, name)
        let (parent_inode, name) = self.parse_path(&req.path).await?;

        // Extract metadata or use defaults
        let (mode, uid, gid) = if let Some(metadata) = req.metadata {
            (metadata.permissions, metadata.uid, metadata.gid)
        } else {
            (0o644, 1000, 1000) // Default permissions and owner
        };

        // Delegate to FileSystemService
        let attr = self
            .filesystem
            .create(
                parent_inode,
                &name,
                mode,
                uid,
                gid,
                ClientId(1), // TODO: Extract from auth context
            )
            .await
            .map_err(filesystem_error_to_status)?;

        info!(
            "File created: path={}, inode={}, size={}",
            req.path, attr.ino, attr.size
        );

        Ok(Response::new(CreateFileResponse {
            file_id: self.inode_to_file_id(attr.ino),
            inode: attr.ino,
        }))
    }

    type ReadFileStream = tokio_stream::wrappers::ReceiverStream<Result<FileChunk, Status>>;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let req = request.into_inner();
        debug!(
            "ReadFile request: file_id len={}, offset={}, size={}",
            req.file_id.len(),
            req.offset,
            req.size
        );

        // Convert file_id to inode
        let inode = self.file_id_to_inode(&req.file_id)?;

        let filesystem = self.filesystem.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(GRPC_STREAM_CHANNEL_BUFFER_SIZE);

        // Spawn task to read and stream file data
        tokio::spawn(async move {
            const CHUNK_SIZE: u32 = 64 * 1024; // 64KB chunks
            let mut offset = req.offset;
            let mut remaining = req.size;

            while remaining > 0 {
                let read_size = remaining.min(CHUNK_SIZE);

                // Read chunk from filesystem
                // TODO: Need file_handle - for now use placeholder (0)
                match filesystem
                    .read(
                        inode,
                        0, // file_handle placeholder
                        offset,
                        read_size,
                        1000,        // uid placeholder
                        1000,        // gid placeholder
                        ClientId(1), // TODO: Extract from auth context,
                    )
                    .await
                {
                    Ok(data) => {
                        if data.is_empty() {
                            // EOF reached
                            break;
                        }

                        let chunk_offset = offset;
                        offset += data.len() as u64;
                        remaining -= data.len() as u32;

                        if tx
                            .send(Ok(FileChunk {
                                data,
                                offset: chunk_offset,
                            }))
                            .await
                            .is_err()
                        {
                            // Client disconnected
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(filesystem_error_to_status(e))).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn write_file(
        &self,
        request: Request<tonic::Streaming<FileChunk>>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        let mut stream = request.into_inner();
        debug!("WriteFile request started");

        let mut bytes_written = 0u64;
        let mut inode: Option<u64> = None;

        // TODO: First chunk should include file_id in metadata
        // For now, we'll extract inode from first chunk's metadata or fail
        // This is a limitation of the current proto design

        // Process streaming chunks
        while let Some(chunk) = stream.message().await? {
            // On first chunk, we need to determine the inode
            // TODO: Protocol needs enhancement to include file_id in first chunk
            if inode.is_none() {
                // For now, create a placeholder inode
                // In real implementation, first chunk metadata would include file_id
                inode = Some(1); // Placeholder
            }

            let current_inode = inode.unwrap();
            let offset = chunk.offset;
            let data = chunk.data;

            if data.is_empty() {
                continue;
            }

            // Write chunk to filesystem
            // TODO: Need file_handle - for now use placeholder (0)
            let written = self
                .filesystem
                .write(
                    current_inode,
                    0, // file_handle placeholder
                    offset,
                    data.clone(),
                    1000,        // uid placeholder
                    1000,        // gid placeholder
                    ClientId(1), // TODO: Extract from auth context,
                )
                .await
                .map_err(filesystem_error_to_status)?;

            bytes_written += written as u64;
        }

        info!("File write completed: {} bytes written", bytes_written);

        Ok(Response::new(WriteFileResponse { bytes_written }))
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let req = request.into_inner();
        debug!("DeleteFile request: file_id len={}", req.file_id.len());

        // Convert file_id to inode
        let inode = self.file_id_to_inode(&req.file_id)?;

        // TODO: We need parent_inode and name for unlink, but proto only provides file_id
        // This is a design limitation - unlink requires parent+name, not just inode
        // For now, use placeholders
        let parent_inode = 1; // Placeholder
        let name = format!("file-{}", inode); // Placeholder

        self.filesystem
            .unlink(
                parent_inode,
                &name,
                1000,        // uid placeholder
                1000,        // gid placeholder
                ClientId(1), // TODO: Extract from auth context,
            )
            .await
            .map_err(filesystem_error_to_status)?;

        info!("File deleted: inode={}", inode);

        Ok(Response::new(DeleteFileResponse { success: true }))
    }

    async fn get_file_metadata(
        &self,
        request: Request<GetFileMetadataRequest>,
    ) -> Result<Response<FileMetadataResponse>, Status> {
        let req = request.into_inner();
        debug!("GetFileMetadata request: file_id len={}", req.file_id.len());

        // Convert file_id to inode
        let inode = self.file_id_to_inode(&req.file_id)?;

        // Get file attributes
        let attr = self
            .filesystem
            .getattr(inode)
            .await
            .map_err(filesystem_error_to_status)?;

        let metadata = self.file_attr_to_proto(&attr);

        Ok(Response::new(FileMetadataResponse {
            metadata: Some(metadata),
        }))
    }

    // Directory operations

    async fn create_directory(
        &self,
        request: Request<CreateDirectoryRequest>,
    ) -> Result<Response<CreateDirectoryResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "CreateDirectory request: path={}, permissions={}",
            req.path, req.permissions
        );

        // Parse path into (parent_inode, name)
        let (parent_inode, name) = self.parse_path(&req.path).await?;

        // Use provided permissions or default
        let mode = if req.permissions != 0 {
            req.permissions
        } else {
            0o755
        };

        // Delegate to FileSystemService
        let attr = self
            .filesystem
            .mkdir(
                parent_inode,
                &name,
                mode,
                1000,        // uid placeholder
                1000,        // gid placeholder
                ClientId(1), // TODO: Extract from auth context,
            )
            .await
            .map_err(filesystem_error_to_status)?;

        info!("Directory created: path={}, inode={}", req.path, attr.ino);

        Ok(Response::new(CreateDirectoryResponse { inode: attr.ino }))
    }

    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        let req = request.into_inner();
        debug!("ListDirectory request: dir_id len={}", req.dir_id.len());

        // Convert dir_id to inode
        let inode = self.file_id_to_inode(&req.dir_id)?;

        // Read directory contents
        let entries = self
            .filesystem
            .readdir(inode, 0, ClientId(1)) // TODO: Extract from auth context
            .await
            .map_err(filesystem_error_to_status)?;

        // Convert DirEntry to FileMetadata
        let proto_entries: Vec<ProtoFileMetadata> = entries
            .iter()
            .map(|entry| {
                // Get attributes for each entry
                // TODO: This is inefficient - should batch getattr calls or return attrs from readdir
                ProtoFileMetadata {
                    inode: entry.ino,
                    path: entry.name.clone(),
                    size: 0, // TODO: Get from getattr
                    permissions: 0,
                    uid: 0,
                    gid: 0,
                    created_at: 0,
                    modified_at: 0,
                    accessed_at: 0,
                }
            })
            .collect();

        Ok(Response::new(ListDirectoryResponse {
            entries: proto_entries,
        }))
    }

    async fn delete_directory(
        &self,
        request: Request<DeleteDirectoryRequest>,
    ) -> Result<Response<DeleteDirectoryResponse>, Status> {
        let req = request.into_inner();
        debug!("DeleteDirectory request: dir_id len={}", req.dir_id.len());

        // Convert dir_id to inode
        let inode = self.file_id_to_inode(&req.dir_id)?;

        // TODO: We need parent_inode and name for rmdir, but proto only provides dir_id
        // This is a design limitation
        let parent_inode = 1; // Placeholder
        let name = format!("dir-{}", inode); // Placeholder

        self.filesystem
            .rmdir(
                parent_inode,
                &name,
                1000,        // uid placeholder
                1000,        // gid placeholder
                ClientId(1), // TODO: Extract from auth context,
            )
            .await
            .map_err(filesystem_error_to_status)?;

        info!("Directory deleted: inode={}", inode);

        Ok(Response::new(DeleteDirectoryResponse { success: true }))
    }

    // Lock operations

    async fn acquire_lock(
        &self,
        request: Request<AcquireLockRequest>,
    ) -> Result<Response<AcquireLockResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "AcquireLock request: file_id len={}, client={}, lock_type={:?}",
            req.file_id.len(),
            req.client_id,
            req.lock_type
        );

        // Convert file_id to inode
        let inode = self.file_id_to_inode(&req.file_id)?;

        // Convert lock type
        let lock_type = proto_to_lock_type(req.lock_type)?;

        // Calculate expiration time
        let expires_at = SystemTime::now() + Duration::from_secs(req.duration_secs as u64);

        // TODO: Convert client_id string to u64
        // For now, use a simple hash of the string
        let client_id = req
            .client_id
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));

        // Acquire lock
        let lock_id = self
            .filesystem
            .acquire_lock(inode, lock_type, expires_at, ClientId(client_id))
            .await
            .map_err(filesystem_error_to_status)?;

        let expires_at_secs = expires_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        info!(
            "Lock acquired: inode={}, client={}, lock_id={}, expires_at={}",
            inode, req.client_id, lock_id, expires_at_secs
        );

        Ok(Response::new(AcquireLockResponse {
            lock_id,
            expires_at: expires_at_secs,
        }))
    }

    async fn release_lock(
        &self,
        request: Request<ReleaseLockRequest>,
    ) -> Result<Response<ReleaseLockResponse>, Status> {
        let req = request.into_inner();
        debug!("ReleaseLock request: lock_id={}", req.lock_id);

        // TODO: The FileSystemService::release_lock takes (inode, client_id),
        // but the proto only provides lock_id. We need a lock_id → inode mapping.
        // For now, use a placeholder inode derived from lock_id
        let inode = req.lock_id; // Placeholder mapping

        self.filesystem
            .release_lock(inode, ClientId(1)) // TODO: Extract from auth context
            .await
            .map_err(filesystem_error_to_status)?;

        info!("Lock released: lock_id={}", req.lock_id);

        Ok(Response::new(ReleaseLockResponse { success: true }))
    }

    async fn extend_lock(
        &self,
        request: Request<ExtendLockRequest>,
    ) -> Result<Response<ExtendLockResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "ExtendLock request: lock_id={}, duration={}",
            req.lock_id, req.duration_secs
        );

        // TODO: Similar to release_lock, we need lock_id → inode mapping
        let inode = req.lock_id; // Placeholder mapping

        // Calculate new expiration time
        let new_expiry = SystemTime::now() + Duration::from_secs(req.duration_secs as u64);

        self.filesystem
            .extend_lock(inode, new_expiry, ClientId(1)) // TODO: Extract from auth context
            .await
            .map_err(filesystem_error_to_status)?;

        let new_expires_at = new_expiry
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        info!(
            "Lock extended: lock_id={}, new_expires_at={}",
            req.lock_id, new_expires_at
        );

        Ok(Response::new(ExtendLockResponse { new_expires_at }))
    }

    // Stripe operations (low-level)

    async fn read_stripe(
        &self,
        request: Request<ReadStripeRequest>,
    ) -> Result<Response<ReadStripeResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "ReadStripe request: file_id len={}, stripe_id len={}",
            req.file_id.len(),
            req.stripe_id.len()
        );

        // Low-level stripe operations are not delegated to FileSystemService
        // They would go directly to FileStore for reconstruction
        // This is a specialized operation for direct chunk access

        // TODO: Implement stripe read via FileStore::rebuild_stripe()
        // This requires:
        // 1. Get stripe metadata (which chunks, where they are)
        // 2. Read required chunks from FileStore
        // 3. Reconstruct stripe data using erasure decoding
        // 4. Return reconstructed data

        warn!("ReadStripe not yet implemented - requires FileStore integration");

        Err(Status::unimplemented(
            "ReadStripe is not yet implemented - use ReadFile for file data access",
        ))
    }

    async fn write_stripe(
        &self,
        request: Request<WriteStripeRequest>,
    ) -> Result<Response<WriteStripeResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "WriteStripe request: file_id len={}, stripe_id len={}, data len={}",
            req.file_id.len(),
            req.stripe_id.len(),
            req.data.len()
        );

        // Low-level stripe operations are not delegated to FileSystemService
        // They would go directly to FileStore for encoding and distribution
        // This is a specialized operation for direct chunk access

        // TODO: Implement stripe write via FileStore::create_stripe()
        // This requires:
        // 1. Apply erasure coding to stripe data
        // 2. Create chunks (data + parity)
        // 3. Distribute chunks across nodes according to StoragePolicy
        // 4. Return stripe_id and chunk locations

        warn!("WriteStripe not yet implemented - requires FileStore integration");

        Err(Status::unimplemented(
            "WriteStripe is not yet implemented - use WriteFile for file data writes",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem_service::MockFileSystemService;

    #[tokio::test]
    async fn test_create_file() {
        use std::time::SystemTime;

        let mut mock_fs = MockFileSystemService::default();

        // Set up expectation
        mock_fs.expect_create().returning(|_, _, _, _, _, _| {
            Box::pin(async {
                Ok(crate::filesystem_service::FileAttr {
                    ino: 42,
                    size: 0,
                    blocks: 0,
                    atime: SystemTime::now(),
                    mtime: SystemTime::now(),
                    ctime: SystemTime::now(),
                    crtime: SystemTime::now(),
                    kind: crate::filesystem_service::FileType::RegularFile,
                    perm: 0o644,
                    nlink: 1,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                })
            })
        });

        let service = FilesystemServiceImpl::new(Arc::new(mock_fs));

        let request = Request::new(CreateFileRequest {
            path: "/test.txt".to_string(),
            metadata: None,
        });

        let response = service.create_file(request).await;
        assert!(response.is_ok());
        let inner = response.unwrap().into_inner();
        assert_eq!(inner.inode, 42);
    }

    #[tokio::test]
    async fn test_create_directory() {
        use std::time::SystemTime;

        let mut mock_fs = MockFileSystemService::default();

        // Set up expectation
        mock_fs.expect_mkdir().returning(|_, _, _, _, _, _| {
            Box::pin(async {
                Ok(crate::filesystem_service::FileAttr {
                    ino: 43,
                    size: 0,
                    blocks: 0,
                    atime: SystemTime::now(),
                    mtime: SystemTime::now(),
                    ctime: SystemTime::now(),
                    crtime: SystemTime::now(),
                    kind: crate::filesystem_service::FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                })
            })
        });

        let service = FilesystemServiceImpl::new(Arc::new(mock_fs));

        let request = Request::new(CreateDirectoryRequest {
            path: "/testdir".to_string(),
            permissions: 0o755,
        });

        let response = service.create_directory(request).await;
        assert!(response.is_ok());
        let inner = response.unwrap().into_inner();
        assert_eq!(inner.inode, 43);
    }
}
