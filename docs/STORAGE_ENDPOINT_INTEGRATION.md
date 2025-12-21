# StorageEndpoint Integration Guide

This document provides guidance on integrating the StorageEndpoint gRPC server with the StorageNode component.

## Overview

The StorageEndpoint implementation (Phase 3.2, Issue #81) provides a fully functional gRPC server infrastructure with:
- 6 gRPC services (Health, Chunk, Snapshot, TransactionLog, Admin, Filesystem)
- PSK-based authentication
- Two-level rate limiting (per-client + overall)
- Request metrics collection
- Graceful shutdown support

## Architecture

```
StorageNode
    |
    +-- FileSystemService (FUSE operations)
    +-- FileStore (chunk storage)
    +-- SnapshotStore (Raft snapshots)
    +-- TransactionLogStore (Raft logs)
    +-- StorageRaftMember (consensus)
    +-- MetricService (metrics)
    |
    +-- StorageEndpoint (gRPC API server)  <-- NEW
            |
            +-- HealthService
            +-- ChunkService
            +-- SnapshotService
            +-- TransactionLogService
            +-- AdminService
            +-- FilesystemService
```

## Integration Steps

### 1. Add StorageEndpoint Field to StorageNodeImpl

```rust
// In src/storage_node/implementation.rs

use crate::storage_endpoint::{StorageEndpoint, implementation::StorageEndpointImpl};

pub struct StorageNodeImpl {
    // ... existing fields ...

    /// StorageEndpoint for gRPC API (Phase 3+)
    storage_endpoint: Option<Arc<StorageEndpointImpl<
        FileSystemServiceImpl,
        FileStoreImpl,
        SnapshotStoreImpl,
        TransactionLogStoreImpl,
        StorageRaftMemberImpl,
        Self, // StorageNode
        MetricServiceImpl,
    >>>,

    /// Endpoint server task handle
    endpoint_handle: Option<tokio::task::JoinHandle<()>>,
}
```

### 2. Initialize StorageEndpoint in Factory

```rust
// In src/storage_node/factory.rs or initialization code

use crate::storage_endpoint::factory::StorageEndpointFactory;
use crate::storage_endpoint::types::EndpointConfig;

// During StorageNode construction:
let endpoint_config = EndpointConfig {
    listen_address: config.grpc_listen_address, // e.g., "0.0.0.0:7000"
    enable_auth: config.enable_grpc_auth,
    enable_tls: config.enable_grpc_tls,
    identities_dir: config.grpc_identities_dir.clone(),
    node_identity: config.grpc_node_identity.clone(),
    rate_limit_per_client: config.grpc_rate_limit_per_client,
    rate_limit_overall: config.grpc_rate_limit_overall,
    rate_limit_burst_size: config.grpc_rate_limit_burst_size,
    enable_logging: true,
    enable_metrics: true,
    ..Default::default()
};

let storage_endpoint = StorageEndpointFactory::create(
    endpoint_config,
    filesystem_service.clone(),
    file_store.clone(),
    snapshot_store.clone(),
    transaction_log_store.clone(),
    raft_member.clone(),
    storage_node_arc.clone(), // Arc<Self>
    metrics.clone(),
)
.await?;
```

### 3. Start StorageEndpoint in start() Method

```rust
// In src/storage_node/implementation.rs

async fn start(&mut self) -> Result<(), Error> {
    if self.started {
        return Ok(());
    }

    info!("Starting StorageNode components...");

    // ... existing component startup ...

    // Start gRPC endpoint (Phase 3+)
    if let Some(endpoint) = &self.storage_endpoint {
        info!("Starting StorageEndpoint gRPC server...");
        let endpoint_clone = endpoint.clone();

        let endpoint_handle = tokio::spawn(async move {
            if let Err(e) = endpoint_clone.serve().await {
                error!("StorageEndpoint server error: {}", e);
            }
        });

        self.endpoint_handle = Some(endpoint_handle);

        // Wait briefly to ensure server starts
        tokio::time::sleep(Duration::from_millis(100)).await;

        if endpoint.is_serving() {
            info!("StorageEndpoint gRPC server started on {}", endpoint.local_addr());
        } else {
            warn!("StorageEndpoint may not have started correctly");
        }
    }

    self.started = true;
    Ok(())
}
```

### 4. Shutdown StorageEndpoint in shutdown() Method

```rust
// In src/storage_node/implementation.rs

async fn shutdown(&mut self) -> Result<(), Error> {
    if !self.started {
        return Ok(());
    }

    info!("Shutting down StorageNode...");

    // Shutdown in reverse order

    // Step 1: Stop accepting new gRPC requests
    if let Some(endpoint) = &self.storage_endpoint {
        info!("Shutting down StorageEndpoint...");
        if let Err(e) = endpoint.shutdown(Duration::from_secs(30)).await {
            warn!("Failed to shutdown StorageEndpoint gracefully: {}", e);
        }

        // Wait for server task to complete
        if let Some(handle) = self.endpoint_handle.take() {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => info!("StorageEndpoint task completed"),
                Ok(Err(e)) => warn!("StorageEndpoint task panicked: {:?}", e),
                Err(_) => warn!("StorageEndpoint task shutdown timed out"),
            }
        }
    }

    // ... rest of shutdown sequence ...

    self.started = false;
    Ok(())
}
```

### 5. Update ComponentStatus

```rust
// In src/storage_node/implementation.rs

pub fn get_component_status(&self) -> ComponentStatus {
    ComponentStatus {
        // ... existing fields ...
        endpoint: self.storage_endpoint.as_ref().map_or(false, |e| e.is_serving()),
    }
}
```

## Configuration Example

```toml
# config.toml

[grpc]
listen_address = "0.0.0.0:7000"
enable_auth = true
enable_tls = true
identities_dir = "/etc/wormfs/identities"
node_identity = "storage_node_01"

[grpc.rate_limiting]
per_client = 100        # requests/second per client
overall = 1000          # total requests/second
burst_size = 100        # burst capacity
```

## Service API Division

**gRPC Services** (StorageEndpoint - Port 7000):
- Cluster mutation operations (AddNode, RemoveNode, SetStoragePolicy, TriggerRebalance)
- Chunk operations (read/write/verify/repair)
- Snapshot/log operations (for Raft replication)
- Filesystem operations (for FUSE clients)

**HTTP Admin API** (AdminServer - Port 9090):
- Read-only monitoring (metrics, status, health)
- Configuration viewing
- Log viewing
- Network/Raft status

## Security Considerations

1. **PSK Authentication**: Store identity files securely in `/etc/wormfs/identities/`
2. **TLS**: Configure TLS certificates for production deployments
3. **Rate Limiting**: Adjust per-client and overall limits based on cluster size
4. **Network Isolation**: Bind to internal network interfaces only

## Testing

See `tests/storage_endpoint_integration.rs` for integration test examples.

## Current Limitations

1. Service implementations are stubs - business logic needs to be filled in
2. TLS configuration is not yet implemented
3. Upload tokens use hardcoded values - need proper cryptographic implementation
4. Some required mock types not exported for comprehensive testing

## Future Enhancements

1. Complete service implementations with actual business logic
2. Add TLS 1.3 support with certificate management
3. Implement proper JWT-based upload tokens
4. Add circuit breaker middleware
5. Add request tracing and distributed tracing support
6. Implement connection pooling for inter-node communication
