//! FilesystemService gRPC implementation.
//!
//! Provides file and directory operations for FUSE clients,
//! delegating to the FileSystemService component.

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::filesystem_service::FileSystemService;
use crate::storage_endpoint::proto::wormfs::filesystem::filesystem_service_server::FilesystemService;
use crate::storage_endpoint::proto::wormfs::filesystem::*;

/// FilesystemService gRPC implementation.
///
/// Delegates filesystem operations to the FileSystemService component.
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

        // TODO: Implement actual file creation
        warn!("CreateFile not yet fully implemented");

        Ok(Response::new(CreateFileResponse {
            file_id: vec![],
            inode: 0,
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

        let (tx, rx) = tokio::sync::mpsc::channel(32);

        // TODO: Implement streaming file read
        tokio::spawn(async move {
            warn!("ReadFile not yet fully implemented");
            let _ = tx
                .send(Ok(FileChunk {
                    data: vec![],
                    offset: 0,
                }))
                .await;
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

        // TODO: Implement streaming file write
        while let Some(chunk) = stream.message().await? {
            bytes_written += chunk.data.len() as u64;
        }

        warn!(
            "WriteFile not yet fully implemented (received {} bytes)",
            bytes_written
        );

        Ok(Response::new(WriteFileResponse { bytes_written }))
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        let req = request.into_inner();
        debug!("DeleteFile request: file_id len={}", req.file_id.len());

        // TODO: Implement file deletion
        warn!("DeleteFile not yet fully implemented");

        Ok(Response::new(DeleteFileResponse { success: true }))
    }

    async fn get_file_metadata(
        &self,
        request: Request<GetFileMetadataRequest>,
    ) -> Result<Response<FileMetadataResponse>, Status> {
        let req = request.into_inner();
        debug!("GetFileMetadata request: file_id len={}", req.file_id.len());

        // TODO: Implement metadata retrieval
        warn!("GetFileMetadata not yet fully implemented");

        Ok(Response::new(FileMetadataResponse { metadata: None }))
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

        // TODO: Implement directory creation
        warn!("CreateDirectory not yet fully implemented");

        Ok(Response::new(CreateDirectoryResponse { inode: 0 }))
    }

    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        let req = request.into_inner();
        debug!("ListDirectory request: dir_id len={}", req.dir_id.len());

        // TODO: Implement directory listing
        warn!("ListDirectory not yet fully implemented");

        Ok(Response::new(ListDirectoryResponse { entries: vec![] }))
    }

    async fn delete_directory(
        &self,
        request: Request<DeleteDirectoryRequest>,
    ) -> Result<Response<DeleteDirectoryResponse>, Status> {
        let req = request.into_inner();
        debug!("DeleteDirectory request: dir_id len={}", req.dir_id.len());

        // TODO: Implement directory deletion
        warn!("DeleteDirectory not yet fully implemented");

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

        // TODO: Implement lock acquisition
        warn!("AcquireLock not yet fully implemented");

        Ok(Response::new(AcquireLockResponse {
            lock_id: 0,
            expires_at: 0,
        }))
    }

    async fn release_lock(
        &self,
        request: Request<ReleaseLockRequest>,
    ) -> Result<Response<ReleaseLockResponse>, Status> {
        let req = request.into_inner();
        debug!("ReleaseLock request: lock_id={}", req.lock_id);

        // TODO: Implement lock release
        warn!("ReleaseLock not yet fully implemented");

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

        // TODO: Implement lock extension
        warn!("ExtendLock not yet fully implemented");

        Ok(Response::new(ExtendLockResponse { new_expires_at: 0 }))
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

        // TODO: Implement stripe read
        warn!("ReadStripe not yet fully implemented");

        Ok(Response::new(ReadStripeResponse { data: vec![] }))
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

        // TODO: Implement stripe write with erasure coding
        warn!("WriteStripe not yet fully implemented");

        Ok(Response::new(WriteStripeResponse {
            stripe_id: vec![],
            chunks: vec![],
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem_service::MockFileSystemService;

    #[tokio::test]
    async fn test_create_file() {
        let mock_fs = Arc::new(MockFileSystemService::new());
        let service = FilesystemServiceImpl::new(mock_fs);

        let request = Request::new(CreateFileRequest {
            path: "/test.txt".to_string(),
            metadata: None,
        });

        let response = service.create_file(request).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_create_directory() {
        let mock_fs = Arc::new(MockFileSystemService::new());
        let service = FilesystemServiceImpl::new(mock_fs);

        let request = Request::new(CreateDirectoryRequest {
            path: "/testdir".to_string(),
            permissions: 0o755,
        });

        let response = service.create_directory(request).await;
        assert!(response.is_ok());
    }
}
