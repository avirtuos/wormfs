# StripeCache Removal Checklist

**Related**: #101 (BufferedFileHandle design)
**Date**: 2025-10-19

## Overview

This document tracks all StripeCache integration points that must be removed when migrating to BufferedFileHandle.

---

## Source Files to Modify

### 1. `src/filesystem_service/stripe_cache.rs`

**Action**: **DELETE ENTIRE FILE** (~1300 lines)

**Status**: ⏸️ Pending Phase 5

**Contains**:
- `StripeCache` struct and implementation
- `CacheEntry` struct
- `WriteContext` struct
- `StripeCacheConfig`
- All flush logic (now in BufferedFileHandle)
- Background flush task
- LRU eviction logic

---

### 2. `src/filesystem_service/implementation.rs`

**Line References**:
- Line 5: Import `stripe_cache::*`
- Line 70-78: `FileSystemServiceImpl` struct field `stripe_cache: Arc<StripeCache<StripeWriteContext>>`
- Line 135-253: StripeCache creation and flush callback setup
- Line 261: `stripe_cache: Arc::new(stripe_cache)`
- Line 377-397: `flush_file()` method (calls `stripe_cache.flush_write_group`)
- Line 1088-1399: `write()` method - extensive StripeCache usage

**Actions**:
1. Remove `stripe_cache` field from `FileSystemServiceImpl`
2. Remove StripeCache import
3. Remove flush_callback creation (lines 135-253)
4. Simplify `flush_file()` to just call `buffered_handle.full_flush()`
5. Rewrite `write()` to delegate to `buffered_handle.write()`
6. Add `buffered_handle: Arc<BufferedFileHandle>` to `OpenFile` struct

**Status**: ⏸️ Pending Phase 4

---

### 3. `src/filesystem_service/factory.rs`

**Line References**:
- Lines related to StripeCache creation in `create()` method

**Actions**:
1. Remove StripeCache creation
2. StripeCache config will be removed
3. BufferedFileHandle will be created per file open instead

**Status**: ⏸️ Pending Phase 4

---

### 4. `src/filesystem_service/mod.rs`

**Line References**:
- Line: `pub mod stripe_cache;`
- Line: `pub use stripe_cache::*;`

**Actions**:
1. Remove `pub mod stripe_cache;`
2. Remove exports
3. Add `pub mod buffered_file_handle;`
4. Add exports for BufferedFileHandle

**Status**: ⏸️ Pending Phase 4

---

### 5. `src/filesystem_service/types.rs`

**Likely contains**: `Config` struct with StripeCache settings

**Search for**:
- `enable_stripe_cache`
- `stripe_cache_max_memory_bytes`
- `stripe_cache_dirty_timeout`

**Actions**:
1. Remove StripeCache config fields
2. Add BufferedFileHandle config fields:
   - `buffered_handle_max_memory_bytes`
   - `buffered_handle_max_flush_interval`
   - `buffered_handle_max_writes_before_flush`

**Status**: ⏸️ Pending Phase 4

---

### 6. `src/filesystem_service/mount.rs`

**Likely usage**: Config parsing, initialization

**Actions**:
1. Update config parsing to remove StripeCache options
2. Add BufferedFileHandle config parsing

**Status**: ⏸️ Pending Phase 4

---

### 7. `src/admin/ui/templates.rs`

**Likely contains**: AdminUI section for StripeCache metrics

**Search for**:
- `stripe_cache.*` metric names
- HTML sections for cache display

**Actions**:
1. Remove StripeCache metrics section
2. Add BufferedFileHandle metrics section:
   - Active handles
   - Total buffered memory
   - Flush counts and latencies
   - Writes coalesced

**Status**: ⏸️ Pending Phase 6

---

## Test Files to Modify/Remove

### 8. `tests/stripe_cache_test.rs`

**Action**: **DELETE ENTIRE FILE**

**Status**: ⏸️ Pending Phase 5

**Contains**: Unit tests for StripeCache internals

**Replacement**: New unit tests in `tests/buffered_file_handle_test.rs`

---

### 9. `tests/stripe_cache_integration_test.rs`

**Action**: **DELETE ENTIRE FILE**

**Status**: ⏸️ Pending Phase 5

**Contains**: Integration tests for StripeCache with FileStore

**Replacement**: Covered by updated integration tests using BufferedFileHandle

---

### 10. `tests/incremental_write_test.rs`

**Action**: **UPDATE** (minor changes)

**Status**: ⏸️ Pending Phase 4

**Current usage**:
- Line 71: Sets `enable_stripe_cache = true`
- Line 72-73: StripeCache config

**Actions**:
1. Update config to use BufferedFileHandle settings
2. Test should still pass (validates incremental writes work)

---

## Configuration Files

### 11. Demo Script (`scripts/demo_wormfs.sh`)

**Search for**: StripeCache config comments and settings

**Actions**:
1. Update config generation to use BufferedFileHandle settings
2. Update comments explaining buffering mechanism

**Status**: ⏸️ Pending Phase 6

---

### 12. Example Configs (if any in repo)

**Search for**: `*.toml` files with StripeCache settings

**Actions**:
1. Update to BufferedFileHandle settings
2. Add migration notes

---

## Documentation Files

### 13. `README.md`

**Search for**: StripeCache mentions

**Actions**:
1. Update architecture descriptions
2. Replace StripeCache with BufferedFileHandle in explanations

**Status**: ⏸️ Pending Phase 6

---

### 14. `CLAUDE.md`

**Search for**: StripeCache references

**Actions**:
1. Update if mentioned
2. Add BufferedFileHandle design principles

**Status**: ⏸️ Pending Phase 6

---

## Metrics and Observability

### Current StripeCache Metrics (to be removed)

```
stripe_cache.write.buffered
stripe_cache.write.bypassed
stripe_cache.write.bytes_buffered
stripe_cache.memory_bytes
stripe_cache.entries
stripe_cache.enabled
stripe_cache.flush.immediate
stripe_cache.flush.total
stripe_cache.flush.timeout
stripe_cache.write_groups.flushed
stripe_cache.api.flush_write_group
stripe_cache.api.flush_file
```

### New BufferedFileHandle Metrics (to be added)

```
buffered_file_handles.active
buffered_file_handles.memory_bytes
buffered_file_handles.partial_flushes
buffered_file_handles.full_flushes
buffered_file_handles.flush_latency
buffered_file_handles.writes_coalesced
buffered_file_handles.builders_active
```

---

## Removal Order (Critical!)

**Phase 1-3**: Build new components (no removals yet)

**Phase 4**: Integrate BufferedFileHandle
1. Add BufferedFileHandle to OpenFile
2. Update write() to use handle
3. Keep StripeCache code but unused
4. Verify tests pass

**Phase 5**: Remove StripeCache
1. Delete stripe_cache.rs
2. Delete stripe_cache_test.rs
3. Delete stripe_cache_integration_test.rs
4. Remove from imports/exports
5. Clean up struct fields

**Phase 6**: Update docs and metrics
1. Update AdminUI
2. Update docs
3. Update demo script

---

## Verification Checklist

After removal, verify:

- [ ] `cargo build` succeeds with no warnings about unused code
- [ ] `cargo test` passes all tests
- [ ] `cargo clippy` shows no StripeCache references
- [ ] `grep -r "StripeCache" src/` returns no results
- [ ] `grep -r "stripe_cache" src/` returns no results
- [ ] Demo script runs successfully
- [ ] AdminUI shows BufferedFileHandle metrics
- [ ] No orphaned config options in Config structs

---

## Git Commits Strategy

Recommended commit sequence:

1. `feat: Add StripeBuilder utility (#101)`
2. `feat: Add BufferedFileHandle core implementation (#101)`
3. `feat: Add BatchFileUpdate Raft command (#101)`
4. `feat: Integrate BufferedFileHandle with FileSystemService (#101)`
5. `test: Verify all tests pass with BufferedFileHandle`
6. `refactor: Remove StripeCache implementation (#101)`
7. `refactor: Remove StripeCache tests (#101)`
8. `refactor: Clean up StripeCache config options (#101)`
9. `feat: Add BufferedFileHandle metrics and AdminUI (#101)`
10. `docs: Update documentation for BufferedFileHandle (#101)`

---

## Risk Assessment

### High Risk

- **Data corruption during migration**: Mitigate by extensive testing before removal
- **Performance regression**: Mitigate by benchmarking before/after

### Medium Risk

- **Config migration issues**: Mitigate by supporting both configs temporarily
- **Metric discontinuity**: Mitigate by documenting metric changes

### Low Risk

- **Test coverage gaps**: Mitigate by maintaining >80% coverage
- **Documentation lag**: Mitigate by updating docs in same PR

---

## Success Criteria for Complete Removal

1. ✅ All code compiles without StripeCache
2. ✅ All tests pass (100% pass rate)
3. ✅ 300MB file test passes (the current failure!)
4. ✅ No performance regression vs StripeCache
5. ✅ Metrics show similar or better IO amplification
6. ✅ Documentation updated
7. ✅ Demo works with new config

---

**Last Updated**: 2025-10-19
**Status**: Planning phase - no removals started
