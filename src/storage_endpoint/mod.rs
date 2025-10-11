//! # StorageEndpoint Component
//!
//! StorageEndpoint provides a gRPC API server for client and node-to-node communication.
//!
//! ## Responsibilities
//!
//! - Exposing gRPC APIs for FUSE filesystem clients
//! - Providing node-to-node APIs for chunk data transfer
//! - Providing node-to-node APIs for metadata snapshot transfer
//! - Routing filesystem requests to FileSystemService
//! - Routing chunk requests to FileStore
//! - Routing snapshot requests to SnapshotStore
//! - Implementing request authentication and authorization
//! - Managing connection lifecycle and health checks
//! - Providing request rate limiting and backpressure
//!
//! ## API Categories
//!
//! ### FUSE Filesystem APIs
//! These APIs are used by client FUSE filesystems to interact with WormFS:
//! - File operations (create, read, write, delete, rename)
//! - Directory operations (mkdir, rmdir, readdir)
//! - Metadata operations (getattr, setattr, chmod, chown)
//! - Lock operations (acquire, release, extend)
//!
//! ### Chunk Transfer APIs
//! These APIs are used by storage nodes to request chunk data:
//! - GetChunk: Retrieve a chunk by ID
//! - VerifyChunk: Check chunk integrity
//! - StreamChunks: Bulk chunk transfer
//!
//! ### Snapshot Transfer APIs
//! These APIs are used by storage nodes to catch up on metadata:
//! - GetLatestSnapshot: Retrieve the latest metadata snapshot
//! - GetSnapshotAtIndex: Retrieve a specific snapshot
//! - StreamSnapshot: Efficient snapshot transfer
//!
//! ## Request Routing
//!
//! ```text
//! Client Request
//!      │
//!      ▼
//! ┌─────────────────┐
//! │ StorageEndpoint │
//! └────────┬────────┘
//!          │
//!          ├─── FUSE API ────────────► FileSystemService
//!          │
//!          ├─── Chunk API ───────────► FileStore
//!          │
//!          └─── Snapshot API ────────► SnapshotStore
//! ```
//!
//! ## gRPC Service Definition
//!
//! The service is defined using Protocol Buffers and compiled with `tonic`.
//! See `proto/wormfs.proto` for the complete service definition.

pub mod types;

use async_trait::async_trait;
use std::net::SocketAddr;
pub use types::{Config, Error};

/// StorageEndpoint trait defines the interface for the gRPC API server.
///
/// Implementations provide gRPC endpoints for client and node-to-node communication.
#[async_trait]
pub trait StorageEndpoint: Send + Sync {
    /// Create a new StorageEndpoint.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including listen address and TLS settings
    ///
    /// # Returns
    ///
    /// A new StorageEndpoint instance ready to serve requests.
    fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Start the gRPC server and begin serving requests.
    ///
    /// This method starts the gRPC server and blocks until shutdown is requested.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Server cannot bind to the listen address
    /// - TLS configuration is invalid
    /// - Server fails during operation
    async fn serve(&self) -> Result<(), Error>;

    /// Gracefully shutdown the gRPC server.
    ///
    /// This method stops accepting new requests and waits for in-flight
    /// requests to complete before shutting down.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for in-flight requests
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown cannot complete within the timeout.
    async fn shutdown(&self, timeout: std::time::Duration) -> Result<(), Error>;

    /// Get the address the server is listening on.
    ///
    /// # Returns
    ///
    /// The socket address the server is bound to.
    fn local_addr(&self) -> SocketAddr;

    /// Check if the server is currently serving requests.
    ///
    /// # Returns
    ///
    /// `true` if the server is active, `false` otherwise.
    fn is_serving(&self) -> bool;
}
