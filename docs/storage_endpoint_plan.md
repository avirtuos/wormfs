# StorageEndpoint Implementation Plan

## Overview

This document outlines the plan to refactor the gRPC data transfer services into a dedicated `StorageEndpoint` module, separating bulk data transfer concerns from the Raft consensus network layer.

## Architecture Goals

### Separation of Concerns
- **StorageNetwork** (libp2p): Handles Raft consensus protocol, peer discovery, and control messages
- **StorageEndpoint** (gRPC): Handles bulk data transfers for snapshots and chunks

### Design Principles
1. **Single Responsibility**: Each module has one clear purpose
2. **Loose Coupling**: Components interact through well-defined interfaces
3. **Extensibility**: Easy to add new data transfer services
4. **Independent Lifecycle**: Can start/stop services independently

## Component Architecture

```
StorageEndpoint/
├── Configuration
│   ├── bind_address: String (default: "0.0.0.0")
│   ├── port: u16 (default: 8082)
│   ├── snapshot_dir: PathBuf
│   └── tls_config: Option<TlsConfig> (future)
│
├── Services
│   ├── SnapshotTransferService (Phase 2B - current)
│   │   ├── TransferSnapshot(request) -> Stream<SnapshotChunk>
│   │   └── Serves snapshots from snapshot_dir
│   │
│   └── ChunkDataService (Phase 3A - future)
│       ├── ReadChunk(chunk_id) -> ChunkData
│       ├── WriteChunk(chunk_id, data) -> Result
│       └── ListChunks(file_id) -> Vec<ChunkInfo>
│
└── Server Management
    ├── new(config) -> Self
    ├── start() -> Result<ServerHandle>
    ├── stop() -> Result<()>
    └── endpoint_address() -> String
```

## Implementation Tasks

### Task 1: Create StorageEndpoint Module Structure - COMPLETED
**Priority: High**
**Estimated Effort: 2 hours**

Sub-tasks:
1. Create `src/storage_endpoint/mod.rs`
2. Define `StorageEndpointConfig` struct
3. Define `StorageEndpointServer` struct
4. Add module exports to `src/lib.rs`

Files to create:
- `src/storage_endpoint/mod.rs`
- `src/storage_endpoint/config.rs`
- `src/storage_endpoint/server.rs`

### Task 2: Move SnapshotTransferService - COMPLETED
**Priority: High**
**Estimated Effort: 1 hour**

Sub-tasks:
1. Move `SnapshotTransferServiceImpl` from `src/transport/snapshot_transfer.rs` to `src/storage_endpoint/services/snapshot.rs`
2. Update import paths
3. Keep `SnapshotTransferClient` in transport module (it's a client utility)

Files to modify:
- `src/transport/snapshot_transfer.rs` (remove server, keep client)
- `src/storage_endpoint/services/snapshot.rs` (new file with server)
- `src/transport/mod.rs` (update exports)

### Task 3: Implement StorageEndpointServer - COMPLETED
**Priority: High**
**Estimated Effort: 3 hours**

Sub-tasks:
1. Implement server lifecycle management
2. Add gRPC server setup with tonic
3. Register SnapshotTransferService
4. Implement graceful shutdown
5. Add health check endpoint

```rust
pub struct StorageEndpointServer {
    config: StorageEndpointConfig,
    server_handle: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl StorageEndpointServer {
    pub async fn start(&mut self) -> Result<String> {
        // Build gRPC server
        // Register services
        // Return endpoint address
    }
    
    pub async fn stop(&mut self) -> Result<()> {
        // Send shutdown signal
        // Wait for graceful termination
    }
}
```

### Task 4: Update NetworkConfig - COMPLETED
**Priority: Medium**
**Estimated Effort: 1 hour**

Sub-tasks:
1. Remove `snapshot_server_port` from `NetworkConfig`
2. Remove `snapshot_dir` from `NetworkConfig`  
3. Add `storage_endpoint_address: Option<String>` for peers to know where to download from
4. Update tests to reflect changes

Files to modify:
- `src/transport/libp2p_network.rs`
- `tests/transport_tests.rs`

### Task 5: Update Node Initialization
**Priority: Medium**
**Estimated Effort: 2 hours**

Sub-tasks:
1. Update `StorageNode` to create both `StorageNetwork` and `StorageEndpoint`
2. Start StorageEndpoint before StorageNetwork
3. Pass endpoint address to components that need it
4. Update configuration file structure

Files to modify:
- `src/node/storage_node.rs`
- `config/storage_node.yaml`

Example configuration:
```yaml
network:
  node_id: 1
  listen_address: "/ip4/0.0.0.0/tcp/3000"
  peers: [...]

storage_endpoint:
  bind_address: "0.0.0.0"
  port: 8082
  snapshot_dir: "./data/snapshots"
```

### Task 6: Modify InstallSnapshot Handling
**Priority: High**
**Estimated Effort: 3 hours**

Sub-tasks:
1. Update `InstallSnapshotRequest` to include `leader_endpoint_address`
2. Modify leader to include its StorageEndpoint address
3. Update follower to download from the provided endpoint
4. Add error handling for endpoint unavailability

Files to modify:
- `proto/wormfs.proto` (already has `leader_address` field)
- Raft layer integration (TBD based on implementation)

### Task 7: Add Integration Tests
**Priority: Medium**
**Estimated Effort: 3 hours**

Test scenarios:
1. Start StorageEndpoint and verify it's accessible
2. Upload and download snapshot via gRPC
3. Test concurrent downloads
4. Test shutdown during transfer
5. Test hash verification
6. Test retry logic on failure

Files to create:
- `tests/storage_endpoint_tests.rs`

### Task 8: Documentation
**Priority: Low**
**Estimated Effort: 2 hours**

Sub-tasks:
1. Update architecture documentation
2. Add usage examples
3. Document configuration options
4. Add sequence diagrams for snapshot transfer flow

Files to modify:
- `docs/implementation.md`
- `README.md`

## Notes

- This design keeps the transport layer (libp2p) separate from data transfer (gRPC)
- The StorageEndpoint can evolve independently of the consensus layer
- Future services can be added without modifying existing code
- The architecture supports horizontal scaling of storage endpoints
