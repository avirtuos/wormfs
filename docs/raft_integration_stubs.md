# Raft Integration Stubs for FileSystemService

## Overview

This document describes the stubbed Raft integration added to FileSystemService to prepare for Phase 2 consensus layer implementation. All metadata modification operations now route through a stubbed `StorageRaftMember` interface, ensuring the correct architectural pattern is established from Phase 1.

## Architecture

### Data Flow

```
┌─────────────────────┐
│ FileSystemService   │
│  (FUSE operations)  │
└──────────┬──────────┘
           │
           │ Metadata Modifications
           ▼
┌─────────────────────┐
│ RaftCommand Enum    │
│ (Serializable ops)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐      Phase 1: Returns immediately
│ StorageRaftMember   │◄──── Phase 2+: Consensus protocol
│       (Stub)        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  MetadataStore      │
│   (Direct write)    │
└─────────────────────┘
```

### Key Principle

**All metadata writes go through Raft, all metadata reads can bypass Raft**

- **Write Path**: create_file, update_file, delete_file, allocate_stripes, update_stripe, commit_chunks → Raft
- **Read Path**: getattr, readdir, lookup → Direct to MetadataStore

## Files Added

### 1. `src/filesystem_service/raft_commands.rs`

Defines all Raft commands and the stub implementation.

#### RaftCommand Enum

```rust
pub enum RaftCommand {
    // File operations
    CreateFile { parent_inode, name, file_type, mode, uid, gid },
    UpdateFile { inode, updates },
    DeleteFile { parent_inode, name },

    // Stripe operations
    AllocateStripes { file_id, stripes },
    UpdateStripe { file_id, stripe_id, metadata },
    CommitChunks { stripe_id, chunk_ids },

    // Lock operations
    AcquireLock { inode, lock_type, client_id, expires_at },
    ReleaseLock { inode, client_id },
    ExtendLock { inode, client_id, new_expiry },

    // Transaction operations (for Phase 3+)
    BeginTransaction { transaction_id, operations, timeout },
    CommitTransaction { transaction_id },
    AbortTransaction { transaction_id },
}
```

#### StorageRaftMemberStub

Phase 1 stub that immediately returns success:

```rust
pub struct StorageRaftMemberStub;

impl StorageRaftMemberStub {
    pub async fn propose_operation(&self, command: RaftCommand)
        -> Result<RaftCommandResult, RaftError>
    {
        // Logs command for debugging
        tracing::debug!("STUB: Raft operation proposed: {:?}", command);

        // Immediately returns success (no consensus)
        Ok(/* appropriate result */)
    }

    pub fn is_leader(&self) -> bool {
        true  // Always leader in Phase 1
    }
}
```

### 2. `src/filesystem_service/raft_integration.rs`

Shows how FileSystemService will integrate with Raft.

#### Example Integration Methods

```rust
impl RaftIntegratedFileSystemService {
    /// Create a file through Raft consensus
    pub async fn create_file_via_raft(
        &self,
        parent: u64,
        name: &str,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        // 1. Create Raft command
        let command = RaftCommand::CreateFile { ... };

        // 2. Propose through Raft (stub returns immediately)
        let result = self.raft_member.propose_operation(command).await?;

        // 3. Handle result
        match result {
            RaftCommandResult::FileCreated { inode, file_id } => {
                // In Phase 2+, Raft state machine writes to MetadataStore
                // In Phase 1, we write directly (temporary)
                self.write_to_metadata_store_temp(...).await?;
                Ok(FileAttr { ... })
            }
            ...
        }
    }
}
```

### 3. `src/filesystem_service/types.rs` (Updated)

Added error variants for Raft operations:

```rust
pub enum Error {
    // Existing errors...

    // New Raft-related errors
    RaftError(String),
    MetadataError(String),
    Internal(String),
    NotSupported(String),
    LockConflictSimple(String),
    LockNotHeldSimple(String),
}
```

Added `kind` and `blksize` fields to `FileAttr` for FUSE compatibility.

## Operation Examples

### File Creation Flow

```rust
// Phase 1: FileSystemService calls stub
let file_attr = service.create_file_via_raft(
    1,              // parent (root)
    "test.txt",     // name
    FileType::RegularFile,
    0o644,          // mode
    1000, 1000,     // uid, gid
    client_id,
).await?;

// Internally:
// 1. RaftCommand::CreateFile is created
// 2. Stub immediately returns success
// 3. Temporary direct write to MetadataStore
// 4. FileAttr returned to caller

// Phase 2+: Same code path
// 1. RaftCommand::CreateFile is created
// 2. Command goes through Raft consensus
// 3. Raft state machine writes to MetadataStore
// 4. Result propagated back to caller
```

### Stripe Allocation Flow

```rust
// Allocate stripes for a file
let stripe_ids = service.allocate_stripes_via_raft(
    file_id,
    1,              // count
    1024 * 1024,    // 1MB stripes
    2,              // data shards
    1,              // parity shards
).await?;

// Write chunks (data path - NOT through Raft)
// Chunks are staged but not in metadata yet

// Update stripe metadata (through Raft)
service.update_stripe_via_raft(
    file_id,
    stripe_id,
    stripe_metadata,
).await?;

// Commit chunks (through Raft)
service.commit_chunks_via_raft(
    stripe_id,
    vec![chunk_id_1, chunk_id_2, chunk_id_3],
).await?;
```

## Phase 1 vs Phase 2+ Behavior

| Aspect | Phase 1 (Current) | Phase 2+ (Future) |
|--------|-------------------|-------------------|
| Raft Member | `StorageRaftMemberStub` | `StorageRaftMemberImpl` |
| Consensus | None (immediate return) | Full Raft protocol |
| MetadataStore Write | Direct (temp helper) | Via Raft state machine |
| Leader Check | Always true | Actual leader election |
| Transaction Support | Stub only | Full 2PC |
| Cluster Support | Single node | Multi-node cluster |

## Integration Points

### When FileSystemService Needs to Modify Metadata

**Always use the Raft path:**

```rust
// ✅ CORRECT - Through Raft
service.create_file_via_raft(...).await?;
service.update_file_via_raft(...).await?;
service.allocate_stripes_via_raft(...).await?;

// ❌ WRONG - Direct MetadataStore write
metadata_store.create_file(...).await?;  // Skip in FileSystemService
```

### When FileSystemService Needs to Read Metadata

**Direct to MetadataStore (no Raft needed):**

```rust
// ✅ CORRECT - Direct read
let file = metadata_store.get_file_by_inode(inode).await?;
let entries = metadata_store.list_directory(parent).await?;
```

## Benefits of This Approach

1. **Correct Architecture From Day 1**: Even in Phase 1, all writes go through the Raft interface
2. **Easy Phase 2 Integration**: Just swap `StorageRaftMemberStub` with `StorageRaftMemberImpl`
3. **No Code Refactoring**: FileSystemService code doesn't change in Phase 2
4. **Clear Separation**: Data plane (chunks) vs control plane (metadata)
5. **Testable**: Can test FileSystemService logic independently of Raft

## Testing

Tests in `src/filesystem_service/raft_commands.rs`:

```bash
$ cargo test filesystem_service::raft_commands
running 2 tests
test filesystem_service::raft_commands::tests::test_stub_acquire_lock ... ok
test filesystem_service::raft_commands::tests::test_stub_create_file ... ok
```

## Next Steps for Phase 2

1. Implement `StorageRaftMemberImpl` using `openraft` crate
2. Replace stub in `RaftIntegratedFileSystemService::new()`
3. Remove `write_to_metadata_store_temp()` helper
4. Implement Raft state machine that applies commands to MetadataStore
5. Add leader redirection logic for non-leader nodes

## References

- `docs/implementation_plan/phase2_consensus_layer.md`
- `src/storage_raft_member/mod.rs` - Raft trait definition
- `docs/filesystem_transactions.md` - 2PC protocol details
