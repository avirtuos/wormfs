//! FileSystemService implementation with MetadataStore integration.
//!
//! This module provides the concrete implementation of FileSystemService
//! that integrates with MetadataStore for metadata operations and prepares
//! for FileStore integration for data operations.

use super::buffered_file_handle::BufferedFileHandle;
use super::inode::{InodeCache, InodeManager, ROOT_INODE};
use super::raft_commands::StorageRaftMemberStub;
use super::types::{ClientId, Config, DirEntry, Error, FileAttr, FileType, LockType, OpenFile};
use super::FileSystemService;
use crate::file_store::{FileStore, FileStoreImpl};
use crate::metadata_store::{FileId, FileMetadata, FileRecord, MetadataStore, MetadataStoreImpl};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};
use tokio::time::Instant;

// Import MetricService trait
use crate::metric_service::MetricService;

/// Overflow-safe helper: Check if offset + len would overflow u64.
///
/// Returns the end offset if safe, otherwise returns InvalidArgument error.
/// This prevents integer overflow in file I/O operations that could lead to
/// data corruption or security vulnerabilities.
fn checked_end_offset(offset: u64, len: usize) -> Result<u64, Error> {
    offset.checked_add(len as u64).ok_or_else(|| {
        Error::InvalidArgument("File operation would exceed maximum offset (u64 overflow)".into())
    })
}

/// Overflow-safe helper: Check if stripe_idx * stripe_size would overflow u64.
///
/// Returns the stripe offset if safe, otherwise returns Internal error.
/// This prevents overflow when calculating stripe boundaries for very large files.
fn checked_stripe_offset(stripe_idx: u64, stripe_size: u64) -> Result<u64, Error> {
    stripe_idx.checked_mul(stripe_size).ok_or_else(|| {
        Error::Internal(format!(
            "Stripe offset calculation overflow: {} * {}",
            stripe_idx, stripe_size
        ))
    })
}

/// Metrics collector for BufferedFileHandle operations.
///
/// Tracks flush operations, write coalescence, and latencies for observability.
/// Uses atomic operations for thread-safe concurrent updates.
#[derive(Debug)]
struct BufferedMetricsCollector {
    /// Count of partial flushes (memory pressure, force=false)
    partial_flushes: AtomicU64,
    /// Count of full flushes (time/count based, force=true)
    full_flushes: AtomicU64,
    /// Count of writes that reused existing buffers (coalesced)
    writes_coalesced: AtomicU64,
    /// Count of flushes triggered by memory pressure
    memory_pressure_flushes: AtomicU64,
    /// Count of inform(Truncate) calls
    inform_truncate: AtomicU64,
    /// Count of inform(Setattr) calls
    inform_setattr: AtomicU64,
    /// Count of inform(Rename) calls
    inform_rename: AtomicU64,
    /// Count of inform(Lock) calls
    inform_lock: AtomicU64,
    /// Count of inform(Flush) calls
    inform_flush: AtomicU64,
    /// Count of inform(Fsync) calls
    inform_fsync: AtomicU64,
    /// Count of inform(Release) calls
    inform_release: AtomicU64,
    /// Recent flush latencies for histogram calculation
    flush_latencies: Arc<std::sync::Mutex<Vec<f64>>>,
    /// Previous partial_flushes value (for delta calculation)
    previous_partial_flushes: AtomicU64,
    /// Previous full_flushes value (for delta calculation)
    previous_full_flushes: AtomicU64,
    /// Previous writes_coalesced value (for delta calculation)
    previous_writes_coalesced: AtomicU64,
    /// Previous memory_pressure_flushes value (for delta calculation)
    previous_memory_pressure_flushes: AtomicU64,
    /// Previous inform_truncate value (for delta calculation)
    previous_inform_truncate: AtomicU64,
    /// Previous inform_setattr value (for delta calculation)
    previous_inform_setattr: AtomicU64,
    /// Previous inform_rename value (for delta calculation)
    previous_inform_rename: AtomicU64,
    /// Previous inform_lock value (for delta calculation)
    previous_inform_lock: AtomicU64,
    /// Previous inform_flush value (for delta calculation)
    previous_inform_flush: AtomicU64,
    /// Previous inform_fsync value (for delta calculation)
    previous_inform_fsync: AtomicU64,
    /// Previous inform_release value (for delta calculation)
    previous_inform_release: AtomicU64,
}

impl BufferedMetricsCollector {
    fn new() -> Self {
        Self {
            partial_flushes: AtomicU64::new(0),
            full_flushes: AtomicU64::new(0),
            writes_coalesced: AtomicU64::new(0),
            memory_pressure_flushes: AtomicU64::new(0),
            inform_truncate: AtomicU64::new(0),
            inform_setattr: AtomicU64::new(0),
            inform_rename: AtomicU64::new(0),
            inform_lock: AtomicU64::new(0),
            inform_flush: AtomicU64::new(0),
            inform_fsync: AtomicU64::new(0),
            inform_release: AtomicU64::new(0),
            flush_latencies: Arc::new(std::sync::Mutex::new(Vec::new())),
            previous_partial_flushes: AtomicU64::new(0),
            previous_full_flushes: AtomicU64::new(0),
            previous_writes_coalesced: AtomicU64::new(0),
            previous_memory_pressure_flushes: AtomicU64::new(0),
            previous_inform_truncate: AtomicU64::new(0),
            previous_inform_setattr: AtomicU64::new(0),
            previous_inform_rename: AtomicU64::new(0),
            previous_inform_lock: AtomicU64::new(0),
            previous_inform_flush: AtomicU64::new(0),
            previous_inform_fsync: AtomicU64::new(0),
            previous_inform_release: AtomicU64::new(0),
        }
    }
}

impl super::buffered_file_handle::BufferedMetricsReporter for BufferedMetricsCollector {
    fn report_flush(&self, is_full: bool, latency_secs: f64) {
        if is_full {
            self.full_flushes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.partial_flushes.fetch_add(1, Ordering::Relaxed);
        }

        // Store latency for histogram
        if let Ok(mut latencies) = self.flush_latencies.lock() {
            latencies.push(latency_secs);
        }
    }

    fn report_write_coalesced(&self) {
        self.writes_coalesced.fetch_add(1, Ordering::Relaxed);
    }

    fn report_memory_pressure_flush(&self) {
        self.memory_pressure_flushes.fetch_add(1, Ordering::Relaxed);
    }

    fn report_inform(&self, op_type: super::buffered_file_handle::OperationType) {
        use super::buffered_file_handle::OperationType;
        match op_type {
            OperationType::Truncate => self.inform_truncate.fetch_add(1, Ordering::Relaxed),
            OperationType::Setattr => self.inform_setattr.fetch_add(1, Ordering::Relaxed),
            OperationType::Rename => self.inform_rename.fetch_add(1, Ordering::Relaxed),
            OperationType::Lock => self.inform_lock.fetch_add(1, Ordering::Relaxed),
            OperationType::Flush => self.inform_flush.fetch_add(1, Ordering::Relaxed),
            OperationType::Fsync => self.inform_fsync.fetch_add(1, Ordering::Relaxed),
            OperationType::Release => self.inform_release.fetch_add(1, Ordering::Relaxed),
        };
    }
}

/// Metrics collector for FileSystemService API operations.
///
/// Tracks call counts and latencies for all public FileSystemService APIs
/// to provide observability into filesystem operation patterns and performance.
#[derive(Debug)]
struct ApiMetricsCollector {
    // File operation counters
    create_calls: AtomicU64,
    open_calls: AtomicU64,
    read_calls: AtomicU64,
    write_calls: AtomicU64,
    unlink_calls: AtomicU64,
    symlink_calls: AtomicU64,
    readlink_calls: AtomicU64,
    flush_calls: AtomicU64,
    fsync_calls: AtomicU64,
    release_calls: AtomicU64,

    // Directory operation counters
    mkdir_calls: AtomicU64,
    rmdir_calls: AtomicU64,
    readdir_calls: AtomicU64,

    // Metadata operation counters
    getattr_calls: AtomicU64,
    setattr_calls: AtomicU64,

    // Lock operation counters
    acquire_lock_calls: AtomicU64,
    release_lock_calls: AtomicU64,
    extend_lock_calls: AtomicU64,

    // Latency vectors (for calculating averages)
    create_latencies: Arc<Mutex<Vec<f64>>>,
    open_latencies: Arc<Mutex<Vec<f64>>>,
    read_latencies: Arc<Mutex<Vec<f64>>>,
    write_latencies: Arc<Mutex<Vec<f64>>>,
    unlink_latencies: Arc<Mutex<Vec<f64>>>,
    symlink_latencies: Arc<Mutex<Vec<f64>>>,
    readlink_latencies: Arc<Mutex<Vec<f64>>>,
    flush_latencies: Arc<Mutex<Vec<f64>>>,
    fsync_latencies: Arc<Mutex<Vec<f64>>>,
    release_latencies: Arc<Mutex<Vec<f64>>>,
    mkdir_latencies: Arc<Mutex<Vec<f64>>>,
    rmdir_latencies: Arc<Mutex<Vec<f64>>>,
    readdir_latencies: Arc<Mutex<Vec<f64>>>,
    getattr_latencies: Arc<Mutex<Vec<f64>>>,
    setattr_latencies: Arc<Mutex<Vec<f64>>>,
    acquire_lock_latencies: Arc<Mutex<Vec<f64>>>,
    release_lock_latencies: Arc<Mutex<Vec<f64>>>,
    extend_lock_latencies: Arc<Mutex<Vec<f64>>>,

    // Previous values for delta calculation
    previous_create_calls: AtomicU64,
    previous_open_calls: AtomicU64,
    previous_read_calls: AtomicU64,
    previous_write_calls: AtomicU64,
    previous_unlink_calls: AtomicU64,
    previous_symlink_calls: AtomicU64,
    previous_readlink_calls: AtomicU64,
    previous_flush_calls: AtomicU64,
    previous_fsync_calls: AtomicU64,
    previous_release_calls: AtomicU64,
    previous_mkdir_calls: AtomicU64,
    previous_rmdir_calls: AtomicU64,
    previous_readdir_calls: AtomicU64,
    previous_getattr_calls: AtomicU64,
    previous_setattr_calls: AtomicU64,
    previous_acquire_lock_calls: AtomicU64,
    previous_release_lock_calls: AtomicU64,
    previous_extend_lock_calls: AtomicU64,
}

impl ApiMetricsCollector {
    fn new() -> Self {
        Self {
            // Initialize counters
            create_calls: AtomicU64::new(0),
            open_calls: AtomicU64::new(0),
            read_calls: AtomicU64::new(0),
            write_calls: AtomicU64::new(0),
            unlink_calls: AtomicU64::new(0),
            symlink_calls: AtomicU64::new(0),
            readlink_calls: AtomicU64::new(0),
            flush_calls: AtomicU64::new(0),
            fsync_calls: AtomicU64::new(0),
            release_calls: AtomicU64::new(0),
            mkdir_calls: AtomicU64::new(0),
            rmdir_calls: AtomicU64::new(0),
            readdir_calls: AtomicU64::new(0),
            getattr_calls: AtomicU64::new(0),
            setattr_calls: AtomicU64::new(0),
            acquire_lock_calls: AtomicU64::new(0),
            release_lock_calls: AtomicU64::new(0),
            extend_lock_calls: AtomicU64::new(0),

            // Initialize latency vectors
            create_latencies: Arc::new(Mutex::new(Vec::new())),
            open_latencies: Arc::new(Mutex::new(Vec::new())),
            read_latencies: Arc::new(Mutex::new(Vec::new())),
            write_latencies: Arc::new(Mutex::new(Vec::new())),
            unlink_latencies: Arc::new(Mutex::new(Vec::new())),
            symlink_latencies: Arc::new(Mutex::new(Vec::new())),
            readlink_latencies: Arc::new(Mutex::new(Vec::new())),
            flush_latencies: Arc::new(Mutex::new(Vec::new())),
            fsync_latencies: Arc::new(Mutex::new(Vec::new())),
            release_latencies: Arc::new(Mutex::new(Vec::new())),
            mkdir_latencies: Arc::new(Mutex::new(Vec::new())),
            rmdir_latencies: Arc::new(Mutex::new(Vec::new())),
            readdir_latencies: Arc::new(Mutex::new(Vec::new())),
            getattr_latencies: Arc::new(Mutex::new(Vec::new())),
            setattr_latencies: Arc::new(Mutex::new(Vec::new())),
            acquire_lock_latencies: Arc::new(Mutex::new(Vec::new())),
            release_lock_latencies: Arc::new(Mutex::new(Vec::new())),
            extend_lock_latencies: Arc::new(Mutex::new(Vec::new())),

            // Initialize previous values
            previous_create_calls: AtomicU64::new(0),
            previous_open_calls: AtomicU64::new(0),
            previous_read_calls: AtomicU64::new(0),
            previous_write_calls: AtomicU64::new(0),
            previous_unlink_calls: AtomicU64::new(0),
            previous_symlink_calls: AtomicU64::new(0),
            previous_readlink_calls: AtomicU64::new(0),
            previous_flush_calls: AtomicU64::new(0),
            previous_fsync_calls: AtomicU64::new(0),
            previous_release_calls: AtomicU64::new(0),
            previous_mkdir_calls: AtomicU64::new(0),
            previous_rmdir_calls: AtomicU64::new(0),
            previous_readdir_calls: AtomicU64::new(0),
            previous_getattr_calls: AtomicU64::new(0),
            previous_setattr_calls: AtomicU64::new(0),
            previous_acquire_lock_calls: AtomicU64::new(0),
            previous_release_lock_calls: AtomicU64::new(0),
            previous_extend_lock_calls: AtomicU64::new(0),
        }
    }

    /// Record a call and its latency for a specific API.
    fn record_call(&self, api_name: &str, latency_secs: f64) {
        // Increment counter and store latency based on API name
        match api_name {
            "create" => {
                self.create_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.create_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "open" => {
                self.open_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.open_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "read" => {
                self.read_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.read_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "write" => {
                self.write_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.write_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "unlink" => {
                self.unlink_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.unlink_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "symlink" => {
                self.symlink_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.symlink_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "readlink" => {
                self.readlink_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.readlink_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "flush" => {
                self.flush_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.flush_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "fsync" => {
                self.fsync_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.fsync_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "release" => {
                self.release_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.release_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "mkdir" => {
                self.mkdir_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.mkdir_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "rmdir" => {
                self.rmdir_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.rmdir_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "readdir" => {
                self.readdir_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.readdir_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "getattr" => {
                self.getattr_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.getattr_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "setattr" => {
                self.setattr_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.setattr_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "acquire_lock" => {
                self.acquire_lock_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.acquire_lock_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "release_lock" => {
                self.release_lock_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.release_lock_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            "extend_lock" => {
                self.extend_lock_calls.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut latencies) = self.extend_lock_latencies.lock() {
                    latencies.push(latency_secs);
                }
            }
            _ => {} // Unknown API name - ignore
        }
    }
}

/// Concrete implementation of FileSystemService.
///
/// This implementation:
/// - Uses MetadataStore for file metadata operations
/// - Uses FileStore for chunk data operations (Phase 1: stub only)
/// - Routes metadata writes through RaftStub (Phase 1) / Raft (Phase 2+)
/// - Caches frequently accessed inodes for performance
pub struct FileSystemServiceImpl {
    /// Configuration
    config: Config,

    /// MetadataStore for file metadata
    metadata_store: MetadataStoreImpl,

    /// FileStore for chunk data (Phase 1: minimal use)
    file_store: Arc<FileStoreImpl>,

    /// RaftClient for metadata writes (Phase 1: stub, Phase 2+: real Raft)
    raft_client: Arc<dyn crate::filesystem_service::buffered_file_handle::RaftClient + Send + Sync>,

    /// Inode management (allocation and caching)
    inode_manager: Arc<InodeManager>,

    /// Open file handles (file handle → open file state)
    open_files: Arc<RwLock<HashMap<u64, Arc<OpenFile>>>>,

    /// Next file handle to allocate
    next_file_handle: AtomicU64,

    /// Client session tracking (client_id → last heartbeat time)
    /// Used to determine which clients are still alive for lock extension
    client_sessions: Arc<RwLock<HashMap<ClientId, SystemTime>>>,

    /// Lock extension background task handle
    lock_extension_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,

    /// Optional metrics service for instrumentation
    metrics: Option<Arc<crate::metric_service::MetricServiceImpl>>,

    /// Metrics collector for BufferedFileHandle operations
    buffered_metrics: Arc<BufferedMetricsCollector>,

    /// Metrics collector for FileSystemService API operations
    api_metrics: Arc<ApiMetricsCollector>,
}

impl FileSystemServiceImpl {
    /// Set the metrics service for instrumentation.
    ///
    /// This method allows dependency injection of the metrics service after
    /// FileSystemService construction, avoiding circular dependencies during initialization.
    pub async fn set_metrics(&mut self, metrics: Arc<crate::metric_service::MetricServiceImpl>) {
        self.metrics = Some(metrics);
    }

    /// Calculate and publish the total memory usage of all buffered file handles.
    ///
    /// This method aggregates memory usage across all active BufferedFileHandles
    /// and publishes it as a gauge metric. It can be called periodically by a
    /// background task or on-demand for monitoring.
    ///
    /// Returns (total_bytes, complete_stripe_bytes, partial_stripe_bytes, handle_count)
    pub fn publish_buffered_memory_usage(&self) -> (usize, usize, usize, usize) {
        let open_files = self
            .open_files
            .read()
            .expect("open_files lock poisoned - indicates panic in file operation");

        let mut total_bytes = 0usize;
        let mut total_complete = 0usize;
        let mut total_partial = 0usize;
        let mut handle_count = 0usize;

        for open_file in open_files.values() {
            if let Some(buffered_handle) = &open_file.buffered_handle {
                let (complete, partial, total) = buffered_handle.memory_usage_detailed();
                total_bytes += total;
                total_complete += complete;
                total_partial += partial;
                handle_count += 1;
            }
        }

        // Publish metrics if MetricService is available
        if let Some(metrics) = &self.metrics {
            // Total buffered memory
            let _ = metrics.publish_gauge(
                "filesystem.buffered_memory.total_bytes",
                total_bytes as f64,
                crate::metric_service::UnitType::Bytes,
            );

            // Complete stripe memory (triggers flushes)
            let _ = metrics.publish_gauge(
                "filesystem.buffered_memory.complete_stripe_bytes",
                total_complete as f64,
                crate::metric_service::UnitType::Bytes,
            );

            // Partial stripe memory (doesn't trigger flushes)
            let _ = metrics.publish_gauge(
                "filesystem.buffered_memory.partial_stripe_bytes",
                total_partial as f64,
                crate::metric_service::UnitType::Bytes,
            );

            // Number of active buffered handles
            let _ = metrics.publish_gauge(
                "filesystem.buffered_memory.handle_count",
                handle_count as f64,
                crate::metric_service::UnitType::Count,
            );

            // Flush operation counts - publish deltas since last publish
            // Partial flushes
            let current_partial = self
                .buffered_metrics
                .partial_flushes
                .load(Ordering::Relaxed);
            let previous_partial = self
                .buffered_metrics
                .previous_partial_flushes
                .load(Ordering::Relaxed);
            let delta_partial = current_partial.saturating_sub(previous_partial);
            if delta_partial > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.partial_flushes",
                    delta_partial,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_partial_flushes
                    .store(current_partial, Ordering::Relaxed);
            }

            // Full flushes
            let current_full = self.buffered_metrics.full_flushes.load(Ordering::Relaxed);
            let previous_full = self
                .buffered_metrics
                .previous_full_flushes
                .load(Ordering::Relaxed);
            let delta_full = current_full.saturating_sub(previous_full);
            if delta_full > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.full_flushes",
                    delta_full,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_full_flushes
                    .store(current_full, Ordering::Relaxed);
            }

            // Write coalescence count
            let current_coalesced = self
                .buffered_metrics
                .writes_coalesced
                .load(Ordering::Relaxed);
            let previous_coalesced = self
                .buffered_metrics
                .previous_writes_coalesced
                .load(Ordering::Relaxed);
            let delta_coalesced = current_coalesced.saturating_sub(previous_coalesced);
            if delta_coalesced > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.writes_coalesced",
                    delta_coalesced,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_writes_coalesced
                    .store(current_coalesced, Ordering::Relaxed);
            }

            // Memory pressure flushes
            let current_memory_pressure = self
                .buffered_metrics
                .memory_pressure_flushes
                .load(Ordering::Relaxed);
            let previous_memory_pressure = self
                .buffered_metrics
                .previous_memory_pressure_flushes
                .load(Ordering::Relaxed);
            let delta_memory_pressure =
                current_memory_pressure.saturating_sub(previous_memory_pressure);
            if delta_memory_pressure > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.memory_pressure_flushes",
                    delta_memory_pressure,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_memory_pressure_flushes
                    .store(current_memory_pressure, Ordering::Relaxed);
            }

            // inform() operation counts
            // Truncate
            let current_truncate = self
                .buffered_metrics
                .inform_truncate
                .load(Ordering::Relaxed);
            let previous_truncate = self
                .buffered_metrics
                .previous_inform_truncate
                .load(Ordering::Relaxed);
            let delta_truncate = current_truncate.saturating_sub(previous_truncate);
            if delta_truncate > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.inform_truncate",
                    delta_truncate,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_inform_truncate
                    .store(current_truncate, Ordering::Relaxed);
            }

            // Setattr
            let current_setattr = self.buffered_metrics.inform_setattr.load(Ordering::Relaxed);
            let previous_setattr = self
                .buffered_metrics
                .previous_inform_setattr
                .load(Ordering::Relaxed);
            let delta_setattr = current_setattr.saturating_sub(previous_setattr);
            if delta_setattr > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.inform_setattr",
                    delta_setattr,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_inform_setattr
                    .store(current_setattr, Ordering::Relaxed);
            }

            // Rename
            let current_rename = self.buffered_metrics.inform_rename.load(Ordering::Relaxed);
            let previous_rename = self
                .buffered_metrics
                .previous_inform_rename
                .load(Ordering::Relaxed);
            let delta_rename = current_rename.saturating_sub(previous_rename);
            if delta_rename > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.inform_rename",
                    delta_rename,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_inform_rename
                    .store(current_rename, Ordering::Relaxed);
            }

            // Lock
            let current_lock = self.buffered_metrics.inform_lock.load(Ordering::Relaxed);
            let previous_lock = self
                .buffered_metrics
                .previous_inform_lock
                .load(Ordering::Relaxed);
            let delta_lock = current_lock.saturating_sub(previous_lock);
            if delta_lock > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.inform_lock",
                    delta_lock,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_inform_lock
                    .store(current_lock, Ordering::Relaxed);
            }

            // Flush
            let current_inform_flush = self.buffered_metrics.inform_flush.load(Ordering::Relaxed);
            let previous_inform_flush = self
                .buffered_metrics
                .previous_inform_flush
                .load(Ordering::Relaxed);
            let delta_inform_flush = current_inform_flush.saturating_sub(previous_inform_flush);
            if delta_inform_flush > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.inform_flush",
                    delta_inform_flush,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_inform_flush
                    .store(current_inform_flush, Ordering::Relaxed);
            }

            // Fsync
            let current_fsync = self.buffered_metrics.inform_fsync.load(Ordering::Relaxed);
            let previous_fsync = self
                .buffered_metrics
                .previous_inform_fsync
                .load(Ordering::Relaxed);
            let delta_fsync = current_fsync.saturating_sub(previous_fsync);
            if delta_fsync > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.inform_fsync",
                    delta_fsync,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_inform_fsync
                    .store(current_fsync, Ordering::Relaxed);
            }

            // Release
            let current_release = self.buffered_metrics.inform_release.load(Ordering::Relaxed);
            let previous_release = self
                .buffered_metrics
                .previous_inform_release
                .load(Ordering::Relaxed);
            let delta_release = current_release.saturating_sub(previous_release);
            if delta_release > 0 {
                let _ = metrics.publish_counter(
                    "filesystem.buffered_file_handles.inform_release",
                    delta_release,
                    crate::metric_service::UnitType::Count,
                );
                self.buffered_metrics
                    .previous_inform_release
                    .store(current_release, Ordering::Relaxed);
            }

            // Flush latency (average of recent flushes)
            if let Ok(mut latencies) = self.buffered_metrics.flush_latencies.lock() {
                if !latencies.is_empty() {
                    let avg_latency = latencies.iter().sum::<f64>() / latencies.len() as f64;
                    let _ = metrics.publish_gauge(
                        "filesystem.buffered_file_handles.flush_latency_avg",
                        avg_latency,
                        crate::metric_service::UnitType::Seconds,
                    );

                    // Also publish min/max for better insight
                    if let (Some(min), Some(max)) = (
                        latencies.iter().min_by(|a, b| a.partial_cmp(b).unwrap()),
                        latencies.iter().max_by(|a, b| a.partial_cmp(b).unwrap()),
                    ) {
                        let _ = metrics.publish_gauge(
                            "filesystem.buffered_file_handles.flush_latency_min",
                            *min,
                            crate::metric_service::UnitType::Seconds,
                        );
                        let _ = metrics.publish_gauge(
                            "filesystem.buffered_file_handles.flush_latency_max",
                            *max,
                            crate::metric_service::UnitType::Seconds,
                        );
                    }

                    // Clear latencies after publishing to avoid unbounded growth
                    latencies.clear();
                }
            }
        }

        (total_bytes, total_complete, total_partial, handle_count)
    }

    /// Publish FileSystemService API metrics (call counts and average latencies).
    ///
    /// This method publishes delta-based counters for API call counts and
    /// gauge metrics for average latencies. Called periodically by background task.
    pub fn publish_api_metrics(&self) {
        if let Some(metrics) = &self.metrics {
            // Helper macro to publish metrics for an API
            macro_rules! publish_api {
                ($api_name:expr, $calls_counter:ident, $prev_counter:ident, $latencies:ident) => {
                    // Publish call count (delta)
                    let current_calls = self.api_metrics.$calls_counter.load(Ordering::Relaxed);
                    let previous_calls = self.api_metrics.$prev_counter.load(Ordering::Relaxed);
                    let delta_calls = current_calls.saturating_sub(previous_calls);
                    if delta_calls > 0 {
                        let _ = metrics.publish_counter(
                            &format!("filesystem.api.{}.calls", $api_name),
                            delta_calls,
                            crate::metric_service::UnitType::Count,
                        );
                        self.api_metrics
                            .$prev_counter
                            .store(current_calls, Ordering::Relaxed);
                    }

                    // Publish average latency
                    if let Ok(mut latencies) = self.api_metrics.$latencies.lock() {
                        if !latencies.is_empty() {
                            let avg_latency =
                                latencies.iter().sum::<f64>() / latencies.len() as f64;
                            let _ = metrics.publish_gauge(
                                &format!("filesystem.api.{}.latency_avg", $api_name),
                                avg_latency,
                                crate::metric_service::UnitType::Seconds,
                            );
                            latencies.clear();
                        }
                    }
                };
            }

            // File operations
            publish_api!(
                "create",
                create_calls,
                previous_create_calls,
                create_latencies
            );
            publish_api!("open", open_calls, previous_open_calls, open_latencies);
            publish_api!("read", read_calls, previous_read_calls, read_latencies);
            publish_api!("write", write_calls, previous_write_calls, write_latencies);
            publish_api!(
                "unlink",
                unlink_calls,
                previous_unlink_calls,
                unlink_latencies
            );
            publish_api!(
                "symlink",
                symlink_calls,
                previous_symlink_calls,
                symlink_latencies
            );
            publish_api!(
                "readlink",
                readlink_calls,
                previous_readlink_calls,
                readlink_latencies
            );
            publish_api!("flush", flush_calls, previous_flush_calls, flush_latencies);
            publish_api!("fsync", fsync_calls, previous_fsync_calls, fsync_latencies);
            publish_api!(
                "release",
                release_calls,
                previous_release_calls,
                release_latencies
            );

            // Directory operations
            publish_api!("mkdir", mkdir_calls, previous_mkdir_calls, mkdir_latencies);
            publish_api!("rmdir", rmdir_calls, previous_rmdir_calls, rmdir_latencies);
            publish_api!(
                "readdir",
                readdir_calls,
                previous_readdir_calls,
                readdir_latencies
            );

            // Metadata operations
            publish_api!(
                "getattr",
                getattr_calls,
                previous_getattr_calls,
                getattr_latencies
            );
            publish_api!(
                "setattr",
                setattr_calls,
                previous_setattr_calls,
                setattr_latencies
            );

            // Lock operations
            publish_api!(
                "acquire_lock",
                acquire_lock_calls,
                previous_acquire_lock_calls,
                acquire_lock_latencies
            );
            publish_api!(
                "release_lock",
                release_lock_calls,
                previous_release_lock_calls,
                release_lock_latencies
            );
            publish_api!(
                "extend_lock",
                extend_lock_calls,
                previous_extend_lock_calls,
                extend_lock_latencies
            );
        }
    }

    /// Create a BufferedFileHandle for a file.
    ///
    /// This helper creates a per-handle write buffer with Raft integration.
    /// Called during file open to give each handle isolated buffering.
    fn create_buffered_handle(
        &self,
        file_id: FileId,
        inode: u64,
        attributes: FileAttr,
    ) -> Arc<crate::filesystem_service::buffered_file_handle::BufferedFileHandle> {
        use crate::filesystem_service::buffered_file_handle::{
            BufferedFileHandle, BufferedFileHandleConfig,
        };
        

        // Get storage policy from default
        let storage_policy = Arc::new(self.default_storage_policy());

        // Calculate max stripe size from policy
        let max_stripe_size =
            (storage_policy.chunk_size * storage_policy.data_shards as u64) as usize;

        // Use the existing Raft client
        let raft_client = Arc::clone(&self.raft_client);

        // Configure buffered handle - use config from filesystem config but override max_stripe_size
        let config = BufferedFileHandleConfig {
            max_memory_bytes: self.config.buffered_file_handle_config.max_memory_bytes,
            max_flush_interval: self.config.buffered_file_handle_config.max_flush_interval,
            max_writes_before_flush: self
                .config
                .buffered_file_handle_config
                .max_writes_before_flush,
            max_stripe_size, // Dynamically computed from FileStore config
        };

        Arc::new(BufferedFileHandle::new(
            file_id,
            inode,
            attributes,
            storage_policy,
            config,
            Arc::new(self.metadata_store.clone()),
            Arc::clone(&self.file_store) as Arc<dyn crate::file_store::FileStore + Send + Sync>,
            raft_client,
            Some(Arc::clone(&self.buffered_metrics)
                as Arc<
                    dyn super::buffered_file_handle::BufferedMetricsReporter,
                >),
        ))
    }

    /// Create a new FileSystemServiceImpl.
    ///
    /// This constructor is crate-private and should only be called via
    /// `FileSystemServiceImplFactory::create()`. This ensures consistent
    /// initialization and proper dependency injection.
    ///
    /// # Arguments
    ///
    /// * `config` - FileSystemService configuration
    /// * `metadata_store` - MetadataStore instance for metadata operations
    /// * `file_store` - FileStore instance for chunk operations
    pub(crate) fn new(
        config: Config,
        metadata_store: MetadataStoreImpl,
        file_store: Arc<FileStoreImpl>,
        raft_client: Option<
            Arc<dyn crate::filesystem_service::buffered_file_handle::RaftClient + Send + Sync>,
        >,
    ) -> Self {
        let inode_manager = Arc::new(InodeManager::new(
            config.inode_cache_size,
            config.inode_cache_ttl,
        ));

        // Use provided RaftClient or fallback to stub for backward compatibility
        let raft_client = raft_client.unwrap_or_else(|| {
            let stub = Arc::new(StorageRaftMemberStub::new(metadata_store.clone()));
            Arc::new(crate::filesystem_service::raft_commands::RaftClientImpl::new(stub))
                as Arc<
                    dyn crate::filesystem_service::buffered_file_handle::RaftClient + Send + Sync,
                >
        });

        Self {
            config,
            raft_client,
            metadata_store,
            file_store,
            inode_manager,
            open_files: Arc::new(RwLock::new(HashMap::new())),
            next_file_handle: AtomicU64::new(1), // Start file handles at 1
            client_sessions: Arc::new(RwLock::new(HashMap::new())),
            lock_extension_task: Arc::new(RwLock::new(None)),
            metrics: None, // Will be set via dependency injection
            buffered_metrics: Arc::new(BufferedMetricsCollector::new()),
            api_metrics: Arc::new(ApiMetricsCollector::new()),
        }
    }

    /// Initialize the root directory if it doesn't exist.
    ///
    /// This should be called once during filesystem mount.
    pub async fn initialize_root(&self) -> Result<(), Error> {
        // Check if root already exists
        match self.metadata_store.get_file_by_inode(ROOT_INODE).await {
            Ok(root) => {
                // Root exists (likely from migration) - check if ownership needs updating
                if root.uid != self.config.uid || root.gid != self.config.gid {
                    tracing::info!(
                        "Updating root directory ownership from {}:{} to {}:{}",
                        root.uid,
                        root.gid,
                        self.config.uid,
                        self.config.gid
                    );

                    // Update metadata with new ownership
                    let updated_metadata = FileMetadata {
                        file_type: root.file_type,
                        size: root.size,
                        permissions: root.permissions,
                        uid: self.config.uid,
                        gid: self.config.gid,
                        created_at: root.created_at,
                        modified_at: root.modified_at,
                        accessed_at: root.accessed_at,
                        target: root.target,
                    };

                    self.metadata_store
                        .update_file(root.file_id, updated_metadata)
                        .await
                        .map_err(|e| {
                            Error::MetadataError(format!(
                                "Failed to update root directory ownership: {}",
                                e
                            ))
                        })?;

                    tracing::info!(
                        "Root directory ownership updated to {}:{}",
                        self.config.uid,
                        self.config.gid
                    );
                } else {
                    tracing::debug!("Root directory already has correct ownership");
                }
                Ok(())
            }
            Err(_) => {
                // Root doesn't exist (shouldn't happen with migration) - create it
                tracing::warn!(
                    "Root directory not found, creating with uid={}, gid={}",
                    self.config.uid,
                    self.config.gid
                );

                let root_metadata = FileMetadata {
                    file_type: crate::metadata_store::FileType::Directory,
                    size: 0,
                    permissions: 0o755,
                    uid: self.config.uid,
                    gid: self.config.gid,
                    created_at: SystemTime::now(),
                    modified_at: SystemTime::now(),
                    accessed_at: SystemTime::now(),
                    target: None, // Directories don't have targets
                };

                self.metadata_store
                    .create_file(
                        super::inode::ROOT_FILE_ID,
                        Path::new("/"),
                        ROOT_INODE,
                        root_metadata,
                    )
                    .await
                    .map_err(|e| {
                        Error::MetadataError(format!("Failed to create root directory: {}", e))
                    })?;

                tracing::info!("Created root directory with inode {}", ROOT_INODE);
                Ok(())
            }
        }
    }

    /// Get the inode cache for external use (e.g., FuseAdapter).
    pub fn inode_cache(&self) -> Arc<InodeCache> {
        self.inode_manager.cache()
    }

    /// Get reference to metadata store (for FUSE adapter).
    pub fn metadata_store(&self) -> &MetadataStoreImpl {
        &self.metadata_store
    }

    /// Get reference to inode manager (for testing).
    pub fn inode_manager(&self) -> &Arc<InodeManager> {
        &self.inode_manager
    }

    /// Record a client heartbeat (keeps client session alive).
    ///
    /// In Phase 1: Called automatically on open() to create session (stub mode)
    /// In Phase 2: Called via gRPC heartbeat endpoint by client libraries
    ///
    /// Clients that send recent heartbeats have their locks extended automatically
    /// by the background lock extension task.
    pub fn heartbeat(&self, client_id: ClientId) {
        let mut sessions = self
            .client_sessions
            .write()
            .expect("client_sessions lock poisoned - indicates panic in session management");
        sessions.insert(client_id, SystemTime::now());
        tracing::debug!("Heartbeat recorded for client {}", client_id.as_u64());
    }

    /// Start background tasks (lock extension, cleanup, etc.).
    ///
    /// Should be called after FileSystemService is initialized, typically during
    /// filesystem mount or storage node startup.
    pub async fn start_background_tasks(self: Arc<Self>) {
        // BufferedFileHandle doesn't require background tasks for flushing - flushes happen on
        // write count/time/memory thresholds automatically. However, we do need to publish
        // BufferedFileHandle metrics periodically for monitoring.

        // Task 1: Lock extension
        let service_lock = Arc::clone(&self);
        let lock_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(service_lock.config.lock_extend_interval);

            loop {
                interval.tick().await;

                if let Err(e) = service_lock.extend_active_locks().await {
                    tracing::error!("Lock extension failed: {}", e);
                }
            }
        });

        // Task 2: Periodic metrics publication for BufferedFileHandle memory usage
        let service_metrics = Arc::clone(&self);
        let _metrics_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

            loop {
                interval.tick().await;

                // Publish BufferedFileHandle memory metrics
                service_metrics.publish_buffered_memory_usage();

                // Publish FileSystemService API metrics
                service_metrics.publish_api_metrics();
            }
        });

        // Store task handles
        let mut lock_task_handle = self
            .lock_extension_task
            .write()
            .expect("lock_extension_task lock poisoned");
        *lock_task_handle = Some(lock_task);
        // Note: metrics_task runs independently and doesn't need to be tracked for shutdown

        tracing::info!(
            "Background tasks started (lock extension interval: {:?}, metrics interval: 5s)",
            self.config.lock_extend_interval
        );
    }

    /// Flush all cached data for a specific file to persistent storage.
    ///
    /// This forces all buffered writes for the file to be written to disk immediately,
    /// bypassing the normal flush intervals. Useful for ensuring data persistence
    /// or for testing.
    pub async fn flush_file(&self, inode: u64) -> Result<(), Error> {
        // Find all file handles for this inode and collect their buffered handles
        let buffered_handles: Vec<_> = {
            let open_files = self
                .open_files
                .read()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files
                .values()
                .filter(|of| of.inode == inode)
                .filter_map(|of| of.buffered_handle.clone())
                .collect()
        };

        // Flush each buffered handle (force=true for explicit flush_file calls)
        for handle in buffered_handles {
            handle.full_flush(true).await.map_err(|e| {
                Error::Internal(format!("Failed to flush BufferedFileHandle: {}", e))
            })?;
        }

        Ok(())
    }

    /// Find a BufferedFileHandle for the given inode (if any file handle is open).
    ///
    /// This allows operations without a file_handle parameter to benefit from metadata caching.
    /// Returns the first BufferedFileHandle found for the inode (if multiple handles exist).
    ///
    /// # Arguments
    ///
    /// * `inode` - The inode to search for
    ///
    /// # Returns
    ///
    /// Some(BufferedFileHandle) if any open file handle exists for this inode, None otherwise.
    fn get_buffered_handle_by_inode(&self, inode: u64) -> Option<Arc<BufferedFileHandle>> {
        let open_files = self
            .open_files
            .read()
            .expect("open_files lock poisoned - indicates panic in file operation");
        open_files
            .values()
            .find(|of| of.inode == inode)
            .and_then(|of| of.buffered_handle.clone())
    }

    /// Extend locks for all active files with alive clients.
    ///
    /// This method is called periodically by the background task. It:
    /// 1. Checks all open files with locks
    /// 2. Verifies client is still alive (recent heartbeat)
    /// 3. Extends lock expiration via Raft
    async fn extend_active_locks(&self) -> Result<(), Error> {
        let now = SystemTime::now();
        let new_expiry = now + self.config.lock_timeout;

        // Get snapshot of open files with locks
        let files_to_extend: Vec<_> = {
            let open_files = self
                .open_files
                .read()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files
                .values()
                .filter(|f| f.lock_id.is_some())
                .map(|f| (f.inode, f.client_id, f.lock_id.unwrap()))
                .collect()
        };

        if files_to_extend.is_empty() {
            return Ok(());
        }

        tracing::debug!("Extending {} active locks", files_to_extend.len());

        // Check client heartbeats and build list of locks to extend
        // Drop the lock before doing async operations
        let locks_to_extend: Vec<_> = {
            let sessions = self
                .client_sessions
                .read()
                .expect("client_sessions lock poisoned - indicates panic in session management");

            files_to_extend
                .iter()
                .filter_map(|(inode, client_id, lock_id)| {
                    if let Some(last_heartbeat) = sessions.get(client_id) {
                        let heartbeat_age = now.duration_since(*last_heartbeat).unwrap_or(
                            self.config.client_heartbeat_timeout + Duration::from_secs(1),
                        );

                        if heartbeat_age < self.config.client_heartbeat_timeout {
                            Some((*inode, *client_id, *lock_id))
                        } else {
                            tracing::warn!(
                                "Client {} heartbeat timeout ({:?} > {:?}), lock {} will expire",
                                client_id.as_u64(),
                                heartbeat_age,
                                self.config.client_heartbeat_timeout,
                                lock_id
                            );
                            None
                        }
                    } else {
                        tracing::warn!(
                            "No heartbeat record for client {}, lock {} will expire",
                            client_id.as_u64(),
                            lock_id
                        );
                        None
                    }
                })
                .collect()
        }; // Lock dropped here

        // Now extend locks without holding any locks
        for (inode, client_id, lock_id) in locks_to_extend {
            // Look up file_id from inode
            let file_id = match self.metadata_store.get_file_by_inode(inode).await {
                Ok(file_record) => file_record.file_id,
                Err(e) => {
                    tracing::warn!(
                        "Failed to get file_id for inode {} during lock extension: {}",
                        inode,
                        e
                    );
                    continue;
                }
            };

            match self
                .raft_client
                .extend_lock(file_id, inode, client_id.as_u64(), new_expiry)
                .await
            {
                Ok(_) => {
                    tracing::trace!(
                        "Extended lock {} for inode {}, client {}",
                        lock_id,
                        inode,
                        client_id.as_u64()
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to extend lock {} for inode {}: {}",
                        lock_id,
                        inode,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// Gracefully shutdown the filesystem service.
    ///
    /// Stops background tasks and releases all held locks.
    /// Should be called during filesystem unmount.
    pub async fn shutdown(&self) {
        tracing::info!("Shutting down FileSystemService...");

        // Stop background tasks
        {
            let mut task_guard = self.lock_extension_task.write().unwrap();
            if let Some(task) = task_guard.take() {
                task.abort();
                tracing::info!("Lock extension task stopped");
            }
        }

        // Release all held locks
        let files_to_release: Vec<_> = {
            let open_files = self
                .open_files
                .read()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files.keys().copied().collect()
        };

        for fh in files_to_release {
            if let Err(e) = self.release(fh).await {
                tracing::error!(
                    "Failed to release file handle {} during shutdown: {}",
                    fh,
                    e
                );
            }
        }

        tracing::info!("FileSystemService shutdown complete");
    }

    /// Convert FileRecord to FileAttr for FUSE.
    fn file_record_to_attr(&self, record: &FileRecord) -> FileAttr {
        // Convert MetadataStore FileType to FileSystemService FileType
        let kind = match record.file_type {
            crate::metadata_store::FileType::Directory => FileType::Directory,
            crate::metadata_store::FileType::RegularFile => FileType::RegularFile,
            crate::metadata_store::FileType::Symlink => FileType::Symlink,
        };

        FileAttr {
            ino: record.inode,
            size: record.size,
            blocks: (record.size + 4095) / 4096, // 4KB blocks
            atime: record.created_at, // Return creation time as atime (we don't track access time)
            mtime: record.modified_at,
            ctime: record.modified_at, // SQLite doesn't have ctime
            crtime: record.created_at,
            kind,
            perm: record.permissions as u16,
            nlink: 1,
            uid: record.uid,
            gid: record.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// Convert cached metadata to FileAttr for FUSE (cache-hit path).
    ///
    /// This method constructs a FileAttr directly from cached metadata without
    /// querying the database, providing significant performance benefits for hot files.
    fn cached_metadata_to_attr(&self, inode: u64, cached: &super::inode::CachedInode) -> FileAttr {
        // Convert MetadataStore FileType to FileSystemService FileType
        let kind = match cached.metadata.file_type {
            crate::metadata_store::FileType::Directory => FileType::Directory,
            crate::metadata_store::FileType::RegularFile => FileType::RegularFile,
            crate::metadata_store::FileType::Symlink => FileType::Symlink,
        };

        FileAttr {
            ino: inode,
            size: cached.metadata.size,
            blocks: (cached.metadata.size + 4095) / 4096, // 4KB blocks
            atime: cached.metadata.created_at, // Return creation time as atime (we don't track access time)
            mtime: cached.metadata.modified_at,
            ctime: cached.metadata.modified_at, // Use mtime as ctime
            crtime: cached.metadata.created_at,
            kind,
            perm: cached.metadata.permissions as u16,
            nlink: 1,
            uid: cached.metadata.uid,
            gid: cached.metadata.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// Get default storage policy for Phase 1.
    ///
    /// In Phase 2+, this will be configurable per-file via storage policies.
    fn default_storage_policy(&self) -> crate::file_store::StoragePolicy {
        crate::file_store::StoragePolicy {
            data_shards: 2,
            parity_shards: 1,
            chunk_size: 2 * 1024 * 1024, // 2MB chunks = 4MB stripes (2 data shards)
            compression: crate::file_store::CompressionAlgorithm::None,
        }
    }

    /// Get the stripe size from the current storage policy.
    ///
    /// Stripe size = chunk_size × data_shards.
    /// For the default policy (2 data shards × 2MB chunks), this returns 4MB.
    fn stripe_size(&self) -> u64 {
        self.default_storage_policy().stripe_size()
    }

    /// Convert ChunkRecord from MetadataStore to ChunkMetadata for FileStore.
    fn chunk_record_to_metadata(
        &self,
        record: &crate::metadata_store::ChunkRecord,
    ) -> crate::file_store::ChunkMetadata {
        crate::file_store::ChunkMetadata {
            chunk_id: record.chunk_id,
            node_id: record.node_id,
            disk_id: record.disk_id,
            chunk_index: record.chunk_index,
        }
    }

    /// Helper to convert MetadataStore errors to FileSystemService errors.
    fn convert_metadata_error(&self, error: crate::metadata_store::Error) -> Error {
        match error {
            crate::metadata_store::Error::FileNotFoundByPath(path) => {
                // We don't have the inode for a path-based lookup failure
                Error::MetadataError(format!("File not found at path: {}", path))
            }
            crate::metadata_store::Error::FileNotFoundByInode(inode) => {
                // We have the inode - preserve it for better debugging
                Error::NotFound(inode)
            }
            crate::metadata_store::Error::FileNotFoundByFileId(file_id) => {
                // File ID is internal - convert to generic metadata error with context
                Error::MetadataError(format!("File not found with file_id: {:?}", file_id))
            }
            crate::metadata_store::Error::FileAlreadyExists(path) => {
                Error::AlreadyExists(path.to_string_lossy().to_string())
            }
            crate::metadata_store::Error::ParentNotFound(path) => {
                // Parent not found - provide path context
                Error::MetadataError(format!("Parent directory not found: {:?}", path))
            }
            crate::metadata_store::Error::LockConflict { file_id, lock_type } => {
                // Convert lock conflict with context
                Error::LockConflict(format!(
                    "Cannot acquire {} lock on file_id {:?}",
                    lock_type, file_id
                ))
            }
            crate::metadata_store::Error::LockNotFound { file_id, client_id } => {
                // Convert lock not held with context
                Error::LockNotHeld(format!(
                    "Lock not found for file_id {:?} and client {:?}",
                    file_id, client_id
                ))
            }
            _ => Error::MetadataError(format!("{}", error)),
        }
    }
}

#[async_trait]
impl FileSystemService for FileSystemServiceImpl {
    // ===== File Operations =====

    async fn create(
        &self,
        parent: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        let _start = Instant::now();
        tracing::debug!("create: parent={}, name={}, mode={:o}", parent, name, mode);

        // Step 1: Validate parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            self.api_metrics
                .record_call("create", _start.elapsed().as_secs_f64());
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Check create permission on parent directory (write + execute)
        crate::filesystem_service::permissions::check_create_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(parent))?;

        // Step 3: Reserve inode before Raft operation
        let inode = self
            .metadata_store
            .reserve_inode()
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 4: Construct path
        let path = parent_record.path.join(name);

        // Step 5: Propose file creation through Raft stub
        use crate::filesystem_service::raft_commands::{FileType as RaftFileType, RaftCommand};
        let command = RaftCommand::CreateFile {
            parent_inode: parent,
            name: name.to_string(),
            file_type: RaftFileType::Regular,
            mode,
            uid,
            gid,
        };

        let result = self
            .raft_client
            .propose_raft_command(command)
            .await
            .map_err(|e| {
                // Note: Reserved inode will be cleaned up by TTL (1 hour)
                // Can't release here due to async/lifetime constraints
                Error::RaftError(format!("{}", e))
            })?;

        // Step 5: Extract inode and file_id from Raft result
        // File is already created on all nodes by Raft state machine
        let (raft_inode, file_id) = match result {
            crate::filesystem_service::raft_commands::RaftCommandResult::FileCreated {
                inode: raft_inode,
                file_id,
            } => (raft_inode, file_id),
            crate::filesystem_service::raft_commands::RaftCommandResult::Error { message } => {
                // Release the reserved inode on error
                let _ = self.metadata_store.release_inode(inode).await;
                self.api_metrics
                    .record_call("create", _start.elapsed().as_secs_f64());
                return Err(Error::MetadataError(message));
            }
            _ => {
                let _ = self.metadata_store.release_inode(inode).await;
                self.api_metrics
                    .record_call("create", _start.elapsed().as_secs_f64());
                return Err(Error::Internal("Unexpected Raft result for create".into()));
            }
        };

        // Release the locally reserved inode since Raft generated a different one
        let _ = self.metadata_store.release_inode(inode).await;

        // Step 6: File is already created by Raft state machine on all nodes
        // Fetch the file metadata to populate caches and return FileAttr
        let file_record = self
            .metadata_store
            .get_file_by_inode(raft_inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 7: Cache the inode
        let metadata = crate::metadata_store::FileMetadata {
            file_type: file_record.file_type,
            size: file_record.size,
            permissions: file_record.permissions,
            uid: file_record.uid,
            gid: file_record.gid,
            created_at: file_record.created_at,
            modified_at: file_record.modified_at,
            accessed_at: file_record.accessed_at,
            target: file_record.target.clone(),
        };
        self.inode_manager
            .cache()
            .insert(raft_inode, file_id, metadata);

        // Step 8: Return FileAttr
        let now = SystemTime::now();
        let attr = FileAttr {
            ino: raft_inode,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::RegularFile,
            perm: mode as u16,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        };

        tracing::info!(
            "Created file via Raft: path={:?}, inode={}, file_id={:?}",
            path,
            raft_inode,
            file_id
        );
        self.api_metrics
            .record_call("create", _start.elapsed().as_secs_f64());
        Ok(attr)
    }

    async fn open(
        &self,
        inode: u64,
        flags: u32,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<(u64, FileAttr), Error> {
        let _start = Instant::now();
        tracing::debug!(
            "open: inode={}, flags={}, uid={}, gid={}",
            inode,
            flags,
            uid,
            gid
        );

        // Step 1: Verify file exists and get metadata
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 2: Check file type (can't open directories with open())
        if record.file_type == crate::metadata_store::FileType::Directory {
            self.api_metrics
                .record_call("open", _start.elapsed().as_secs_f64());
            return Err(Error::IsADirectory(inode));
        }

        // Step 3: Parse open flags and check permissions
        let is_write_mode = (flags & libc::O_ACCMODE as u32) != libc::O_RDONLY as u32;

        // Check appropriate permission based on access mode
        if is_write_mode {
            crate::filesystem_service::permissions::check_write_permission(
                uid,
                gid,
                record.uid,
                record.gid,
                record.permissions,
            )
            .map_err(|_| Error::PermissionDenied(inode))?;
        } else {
            crate::filesystem_service::permissions::check_read_permission(
                uid,
                gid,
                record.uid,
                record.gid,
                record.permissions,
            )
            .map_err(|_| Error::PermissionDenied(inode))?;
        }

        // Step 4: Acquire distributed write lock if opening for write
        // This ensures write exclusivity across the entire cluster, not just locally.
        // The lock is enforced via Raft consensus and stored in MetadataStore.
        let lock_id = if is_write_mode {
            use crate::filesystem_service::raft_commands::LockType;

            // Lock expires in 5 minutes (will be extended by keepalives in Phase 2)
            let expires_at = SystemTime::now() + std::time::Duration::from_secs(300);

            match self
                .raft_client
                .acquire_lock(
                    record.file_id,
                    inode,
                    LockType::Write,
                    _client_id.as_u64(),
                    self.config.node_id,
                    expires_at,
                )
                .await
            {
                Ok(lock_id) => {
                    tracing::debug!(
                        "Acquired write lock on inode {}, lock_id={}",
                        inode,
                        lock_id
                    );
                    Some(lock_id)
                }
                Err(e) => {
                    // Lock acquisition failed - likely already locked
                    tracing::warn!("Failed to acquire write lock on inode {}: {}", inode, e);
                    self.api_metrics
                        .record_call("open", _start.elapsed().as_secs_f64());
                    return Err(Error::InvalidArgument(format!(
                        "File inode {} is already open for writing",
                        inode
                    )));
                }
            }
        } else {
            None // No lock needed for read-only opens
        };

        // Step 5: Generate unique file handle
        let file_handle = self.next_file_handle.fetch_add(1, Ordering::SeqCst);

        // Step 5a: Create BufferedFileHandle for all opens
        // BufferedFileHandle is mandatory and provides:
        // 1. Metadata caching to avoid MetadataStore queries
        // 2. Read-through caching for both reads and writes
        // 3. Write buffering and coalescing to reduce I/O amplification
        // 4. Unified code path for all file operations
        let attrs = self.file_record_to_attr(&record);
        let buffered_handle = Some(self.create_buffered_handle(record.file_id, inode, attrs));

        // Step 6: Create OpenFile state
        let open_file = Arc::new(OpenFile {
            file_id: record.file_id,
            inode,
            client_id: _client_id, // Store client ID for lock release
            lock_id,               // Store the distributed lock ID
            flags: super::types::OpenFlags {
                read: (flags & libc::O_ACCMODE as u32) != libc::O_WRONLY as u32,
                write: is_write_mode,
                append: (flags & libc::O_APPEND as u32) != 0,
                truncate: (flags & libc::O_TRUNC as u32) != 0,
                create: (flags & libc::O_CREAT as u32) != 0,
                exclusive: (flags & libc::O_EXCL as u32) != 0,
            },
            offset: AtomicU64::new(0),
            refcount: AtomicU32::new(1),
            buffered_handle,
        });

        // Step 7: Track open file
        {
            let mut open_files = self
                .open_files
                .write()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files.insert(file_handle, open_file);
        }

        // Step 7a: Register client heartbeat (stub mode - creates session for lock extension)
        // In Phase 2, clients will send periodic heartbeats via gRPC
        self.heartbeat(_client_id);

        // Step 8: Handle O_TRUNC flag (truncate file to 0)
        if (flags & libc::O_TRUNC as u32) != 0 {
            // Update file size to 0 via Raft
            use crate::filesystem_service::raft_commands::{FileUpdateFields, RaftCommand};
            let command = RaftCommand::UpdateFile {
                inode,
                updates: FileUpdateFields {
                    size: Some(0),
                    mode: None,
                    uid: None,
                    gid: None,
                    atime: None,
                    mtime: Some(SystemTime::now()),
                },
            };

            let _result = self
                .raft_client
                .propose_raft_command(command)
                .await
                .map_err(|e| Error::RaftError(format!("{}", e)))?;

            // [TEMP Phase 1] Update metadata store directly
            let updated_metadata = FileMetadata {
                file_type: record.file_type,
                size: 0, // Truncated
                permissions: record.permissions,
                uid: record.uid,
                gid: record.gid,
                created_at: record.created_at,
                modified_at: SystemTime::now(),
                accessed_at: record.accessed_at,
                target: record.target.clone(), // Preserve target for symlinks
            };

            self.metadata_store
                .update_file(record.file_id, updated_metadata.clone())
                .await
                .map_err(|e| self.convert_metadata_error(e))?;

            // Invalidate cache
            self.inode_manager.cache().invalidate(inode);
        }

        // Note: We do NOT update access time on file open (see read() for rationale).

        // Step 7: Return file handle and attributes
        let attr = self.file_record_to_attr(&record);
        tracing::info!("Opened file: inode={}, handle={}", inode, file_handle);
        self.api_metrics
            .record_call("open", _start.elapsed().as_secs_f64());
        Ok((file_handle, attr))
    }

    async fn read(
        &self,
        inode: u64,
        file_handle: u64,
        offset: u64,
        size: u32,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<Vec<u8>, Error> {
        // Start timing for metrics
        let start = Instant::now();

        tracing::debug!(
            "read: inode={}, file_handle={}, offset={}, size={}, uid={}, gid={}",
            inode,
            file_handle,
            offset,
            size,
            uid,
            gid
        );

        // Require valid file_handle with BufferedFileHandle
        // All reads must use open() first to get a file_handle
        if file_handle == 0 {
            self.api_metrics
                .record_call("read", start.elapsed().as_secs_f64());
            return Err(Error::InvalidFileHandle(file_handle));
        }

        // Get BufferedFileHandle from OpenFile
        let buffered_handle = {
            let open_files = self
                .open_files
                .read()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files
                .get(&file_handle)
                .and_then(|of| of.buffered_handle.clone())
        };

        // BufferedFileHandle is mandatory for all reads
        let handle = buffered_handle.ok_or_else(|| {
            tracing::error!(
                "read: Missing BufferedFileHandle for file_handle={} - this should never happen",
                file_handle
            );
            Error::InvalidFileHandle(file_handle)
        })?;

        tracing::trace!(
            "read: Using BufferedFileHandle for file_handle={}, inode={}",
            file_handle,
            inode
        );

        // BufferedFileHandle.read() automatically handles:
        // - Read-your-writes consistency
        // - Permission checks
        // - Sparse regions
        // - Stripe reading
        let data = handle.read(offset, size).await?;

        // Record metrics
        if let Some(ref metrics) = self.metrics {
            let elapsed = start.elapsed().as_secs_f64();
            let bytes_read = data.len() as u64;

            let _ = metrics.publish_counter(
                "filesystem.read_ops.total",
                1,
                crate::metric_service::UnitType::Operations,
            );

            let _ = metrics.publish_counter(
                "filesystem.read_ops.bytes",
                bytes_read,
                crate::metric_service::UnitType::Bytes,
            );

            let _ = metrics.publish_histogram(
                "filesystem.read_ops.latency",
                elapsed,
                crate::metric_service::UnitType::Seconds,
            );
        }

        self.api_metrics
            .record_call("read", start.elapsed().as_secs_f64());
        Ok(data)
    }

    async fn write(
        &self,
        inode: u64,
        file_handle: u64,
        offset: u64,
        data: Vec<u8>,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<u32, Error> {
        // Start timing for metrics
        let start = Instant::now();
        let bytes_to_write = data.len() as u64;

        tracing::debug!(
            "write: inode={}, fh={}, offset={}, size={}, uid={}, gid={}",
            inode,
            file_handle,
            offset,
            data.len(),
            uid,
            gid
        );

        if data.is_empty() {
            self.api_metrics
                .record_call("write", start.elapsed().as_secs_f64());
            return Ok(0);
        }

        // Step 1: Get the BufferedFileHandle from OpenFile
        // We'll use it for both cached metadata and the write operation
        let buffered_handle = {
            let open_files = self
                .open_files
                .read()
                .expect("open_files lock poisoned - indicates panic in file operation");
            let open_file = open_files.get(&file_handle).ok_or_else(|| {
                Error::InvalidArgument(format!("File handle {} not found", file_handle))
            })?;

            // Validate that the file handle matches the inode
            if open_file.inode != inode {
                self.api_metrics
                    .record_call("write", start.elapsed().as_secs_f64());
                return Err(Error::InvalidArgument(format!(
                    "File handle {} does not match inode {}",
                    file_handle, inode
                )));
            }

            // Clone the Arc to use outside the lock
            open_file.buffered_handle.clone()
        }; // Lock dropped here

        // Step 2: Get cached file attributes from BufferedFileHandle
        // This avoids a MetadataStore query since all opens create BufferedFileHandle
        let attrs = if let Some(ref handle) = buffered_handle {
            handle.get_file_by_inode()
        } else {
            self.api_metrics
                .record_call("write", start.elapsed().as_secs_f64());
            return Err(Error::Internal(
                "BufferedFileHandle required - enable_stripe_cache must be true".to_string(),
            ));
        };

        // Step 3: Check write permission using cached attributes
        crate::filesystem_service::permissions::check_write_permission(
            uid,
            gid,
            attrs.uid,
            attrs.gid,
            attrs.perm as u32,
        )
        .map_err(|_| Error::PermissionDenied(inode))?;

        // Step 4: Validate file size won't exceed maximum
        let end_offset = checked_end_offset(offset, data.len())?;
        if end_offset > self.config.max_file_size {
            self.api_metrics
                .record_call("write", start.elapsed().as_secs_f64());
            return Err(Error::NoSpace); // ENOSPC - file would exceed maximum size
        }

        // Step 5: Dispatch write based on whether BufferedFileHandle is enabled
        let bytes_written = if let Some(handle) = buffered_handle {
            // NEW PATH: Use BufferedFileHandle for optimized write buffering
            // This handles:
            // - Stripe splitting and alignment
            // - Read-modify-write for partial stripes
            // - Write coalescing and buffering
            // - Automatic flush on memory pressure or timeout
            // - Atomic metadata commits via Raft batching
            handle
                .write(offset, &data)
                .await
                .map_err(|e| Error::Internal(format!("BufferedFileHandle write failed: {}", e)))?
        } else {
            // LEGACY PATH: Fall back to direct metadata update (no buffering)
            // This path is used when enable_stripe_cache is disabled
            tracing::warn!(
                "BufferedFileHandle not enabled for file handle {}, writes will not be buffered",
                file_handle
            );
            self.api_metrics
                .record_call("write", start.elapsed().as_secs_f64());
            return Err(Error::Internal(
                "BufferedFileHandle required - enable_stripe_cache must be true".to_string(),
            ));
        };

        // Step 6: Invalidate cache
        // Note: Metadata updates (size, mtime) are handled by BufferedFileHandle.full_flush()
        // which atomically commits both data and metadata changes through RaftClient
        self.inode_manager.cache().invalidate(inode);

        // Publish metrics if available
        if let Some(ref metrics) = self.metrics {
            let elapsed = start.elapsed().as_secs_f64();

            // Track write operation count
            let _ = metrics.publish_counter(
                "filesystem.write_ops.total",
                1,
                crate::metric_service::UnitType::Operations,
            );

            // Track bytes written (client-level)
            let _ = metrics.publish_counter(
                "filesystem.write_ops.bytes",
                bytes_to_write,
                crate::metric_service::UnitType::Bytes,
            );

            // Track write latency
            let _ = metrics.publish_histogram(
                "filesystem.write_ops.latency",
                elapsed,
                crate::metric_service::UnitType::Seconds,
            );
        }

        tracing::info!(
            "Wrote {} bytes to inode {} at offset {}",
            bytes_written,
            inode,
            offset
        );
        self.api_metrics
            .record_call("write", start.elapsed().as_secs_f64());
        Ok(bytes_written as u32)
    }

    async fn unlink(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<(), Error> {
        let _start = Instant::now();
        tracing::debug!(
            "unlink: parent={}, name={}, uid={}, gid={}",
            parent,
            name,
            uid,
            gid
        );

        // Step 1: Verify parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            self.api_metrics
                .record_call("unlink", _start.elapsed().as_secs_f64());
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Check write permission on parent directory (needed to delete files)
        crate::filesystem_service::permissions::check_unlink_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(parent))?;

        // Step 3: Construct path and lookup file
        let path = parent_record.path.join(name);
        let file_record = self
            .metadata_store
            .get_file_by_path(&path)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 3: Check file type (can't unlink directories - use rmdir)
        if file_record.file_type == crate::metadata_store::FileType::Directory {
            self.api_metrics
                .record_call("unlink", _start.elapsed().as_secs_f64());
            return Err(Error::IsADirectory(file_record.inode));
        }

        // Step 4: Propose file deletion through Raft (handles metadata and stripe cleanup)
        use crate::filesystem_service::raft_commands::RaftCommand;
        let command = RaftCommand::DeleteFile {
            parent_inode: parent,
            name: name.to_string(),
        };

        let result = self
            .raft_client
            .propose_raft_command(command)
            .await
            .map_err(|e| {
                let error_msg = format!("{}", e);
                // Convert specific Raft errors to appropriate FileSystemService errors
                if error_msg.contains("File not found") || error_msg.contains("not found") {
                    // We don't have the inode from the error, use a placeholder
                    Error::MetadataError(format!("File not found: {}", name))
                } else {
                    Error::RaftError(error_msg)
                }
            })?;

        match result {
            crate::filesystem_service::raft_commands::RaftCommandResult::FileDeleted => {}
            crate::filesystem_service::raft_commands::RaftCommandResult::Error { message } => {
                self.api_metrics
                    .record_call("unlink", _start.elapsed().as_secs_f64());
                return Err(Error::MetadataError(message));
            }
            _ => {
                self.api_metrics
                    .record_call("unlink", _start.elapsed().as_secs_f64());
                return Err(Error::Internal("Unexpected Raft result for unlink".into()));
            }
        }

        // Step 5: Invalidate cache
        // Note: Physical chunk deletion is handled by StorageWatchdog in Phase 2
        self.inode_manager.cache().invalidate(file_record.inode);

        tracing::info!(
            "Unlinked file: path={:?}, inode={}",
            path,
            file_record.inode
        );
        self.api_metrics
            .record_call("unlink", _start.elapsed().as_secs_f64());
        Ok(())
    }

    async fn symlink(
        &self,
        parent: u64,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        let _start = Instant::now();
        tracing::debug!(
            "symlink: parent={}, name={}, target={}",
            parent,
            name,
            target
        );

        // Step 1: Verify parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            self.api_metrics
                .record_call("symlink", _start.elapsed().as_secs_f64());
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Check if symlink already exists
        let path = parent_record.path.join(name);
        if let Ok(_existing) = self.metadata_store.get_file_by_path(&path).await {
            self.api_metrics
                .record_call("symlink", _start.elapsed().as_secs_f64());
            return Err(Error::AlreadyExists(path.to_string_lossy().into_owned()));
        }

        // Step 3: Create the symlink through Raft for consistency
        use crate::filesystem_service::raft_commands::{RaftCommand, RaftCommandResult};
        let command = RaftCommand::CreateSymlink {
            parent_inode: parent,
            name: name.to_string(),
            target: target.to_string(),
            uid,
            gid,
        };

        let result = self
            .raft_client
            .propose_raft_command(command)
            .await
            .map_err(|e| Error::RaftError(format!("Failed to create symlink: {}", e)))?;

        // Step 4: Extract inode and file_id from result
        let (inode, file_id) = match result {
            RaftCommandResult::SymlinkCreated { inode, file_id } => (inode, file_id),
            RaftCommandResult::Error { message } => {
                self.api_metrics
                    .record_call("symlink", _start.elapsed().as_secs_f64());
                return Err(Error::MetadataError(message));
            }
            _ => {
                self.api_metrics
                    .record_call("symlink", _start.elapsed().as_secs_f64());
                return Err(Error::Internal(
                    "Unexpected Raft result for symlink creation".into(),
                ));
            }
        };

        // Step 5: Create FileAttr for the response
        let now = SystemTime::now();
        let attr = FileAttr {
            ino: inode,
            size: target.len() as u64, // Size of symlink is length of target path
            blocks: 0,                 // Symlinks don't use data blocks
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Symlink,
            perm: 0o777, // Symlinks typically have 777 permissions
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        };

        // Step 6: Cache the new symlink's inode
        // Convert FileAttr to FileMetadata for caching
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Symlink,
            size: attr.size,
            permissions: attr.perm as u32,
            uid: attr.uid,
            gid: attr.gid,
            created_at: attr.ctime,
            modified_at: attr.mtime,
            accessed_at: attr.atime,
            target: Some(target.to_string()),
        };
        self.inode_manager.cache().insert(inode, file_id, metadata);

        tracing::info!(
            "Created symlink: path={:?}, inode={}, target={}, file_id={:?}",
            path,
            inode,
            target,
            file_id
        );

        self.api_metrics
            .record_call("symlink", _start.elapsed().as_secs_f64());
        Ok(attr)
    }

    async fn readlink(&self, inode: u64) -> Result<String, Error> {
        let _start = Instant::now();
        tracing::debug!("readlink: inode={}", inode);

        // Get the file record
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Verify it's a symlink
        if record.file_type != crate::metadata_store::FileType::Symlink {
            return Err(Error::NotASymlink(inode));
        }

        // Return the target path
        let result = record.target.ok_or_else(|| {
            Error::Internal(format!("Symlink at inode {} has no target path", inode))
        });
        self.api_metrics
            .record_call("readlink", _start.elapsed().as_secs_f64());
        result
    }

    async fn flush(&self, file_handle: u64) -> Result<(), Error> {
        let _start = Instant::now();
        tracing::debug!("flush: file_handle={}", file_handle);

        // Get the buffered handle if present
        let buffered_handle = {
            let open_files = self
                .open_files
                .read()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files
                .get(&file_handle)
                .and_then(|open_file| open_file.buffered_handle.clone())
        };

        // Inform BufferedFileHandle about flush operation
        // This is called on each close() of a file descriptor (dup, dup2, fork)
        if let Some(handle) = buffered_handle {
            tracing::debug!(
                "Informing BufferedFileHandle for file_handle={}",
                file_handle
            );
            handle
                .inform(crate::filesystem_service::buffered_file_handle::OperationType::Flush)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to flush file_handle {}: {}",
                        file_handle, e
                    ))
                })?;
        } else {
            tracing::debug!(
                "No BufferedFileHandle found for file_handle={}",
                file_handle
            );
        }

        self.api_metrics
            .record_call("flush", _start.elapsed().as_secs_f64());
        Ok(())
    }

    async fn fsync(&self, file_handle: u64) -> Result<(), Error> {
        let _start = Instant::now();
        tracing::debug!("fsync: file_handle={}", file_handle);

        // Get the buffered handle if present
        let buffered_handle = {
            let open_files = self
                .open_files
                .read()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files
                .get(&file_handle)
                .and_then(|open_file| open_file.buffered_handle.clone())
        };

        // Inform BufferedFileHandle about fsync operation
        // fsync() must guarantee data reaches persistent storage
        if let Some(handle) = buffered_handle {
            tracing::debug!(
                "Informing BufferedFileHandle for file_handle={}",
                file_handle
            );
            handle
                .inform(crate::filesystem_service::buffered_file_handle::OperationType::Fsync)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to sync file_handle {}: {}", file_handle, e))
                })?;
        } else {
            tracing::debug!(
                "No BufferedFileHandle found for file_handle={}",
                file_handle
            );
        }

        self.api_metrics
            .record_call("fsync", _start.elapsed().as_secs_f64());
        Ok(())
    }

    async fn release(&self, file_handle: u64) -> Result<(), Error> {
        let _start = Instant::now();
        tracing::debug!("release: file_handle={}", file_handle);

        // Inform BufferedFileHandle about release before closing (if present)
        let buffered_handle_to_flush = {
            let open_files = self
                .open_files
                .read()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files
                .get(&file_handle)
                .and_then(|open_file| open_file.buffered_handle.clone())
        };

        // Inform the buffered handle about release (will flush everything)
        if let Some(handle) = buffered_handle_to_flush {
            tracing::debug!(
                "Informing BufferedFileHandle for file_handle={}",
                file_handle
            );
            handle
                .inform(crate::filesystem_service::buffered_file_handle::OperationType::Release)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to flush BufferedFileHandle on release: {}",
                        e
                    ))
                })?;
        }

        // Remove the file handle from tracking and extract lock info
        let removed = {
            let mut open_files = self
                .open_files
                .write()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files.remove(&file_handle)
        };

        match removed {
            Some(open_file) => {
                // If this file was locked (opened for write), release the distributed lock
                if let Some(lock_id) = open_file.lock_id {
                    tracing::debug!(
                        "Releasing distributed lock: inode={}, lock_id={}, client_id={}",
                        open_file.inode,
                        lock_id,
                        open_file.client_id.as_u64()
                    );

                    match self
                        .raft_client
                        .release_lock(
                            open_file.file_id,
                            open_file.inode,
                            open_file.client_id.as_u64(),
                        )
                        .await
                    {
                        Ok(_) => {
                            tracing::debug!(
                                "Successfully released lock on inode {}",
                                open_file.inode
                            );
                        }
                        Err(e) => {
                            // Log error but don't fail the release - file is already closed
                            tracing::error!(
                                "Failed to release lock on inode {}: {}",
                                open_file.inode,
                                e
                            );
                        }
                    }
                }
            }
            None => {
                tracing::warn!("release: file_handle {} not found", file_handle);
                // Don't return an error - FUSE may call release multiple times
            }
        }

        self.api_metrics
            .record_call("release", _start.elapsed().as_secs_f64());
        Ok(())
    }

    // ===== Directory Operations =====

    async fn mkdir(
        &self,
        parent: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        let _start = Instant::now();
        tracing::debug!(
            "mkdir: parent={}, name={}, mode={:o}, uid={}, gid={}",
            parent,
            name,
            mode,
            uid,
            gid
        );

        // Step 1: Validate parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            self.api_metrics
                .record_call("mkdir", _start.elapsed().as_secs_f64());
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Check mkdir permission on parent directory (write + execute)
        crate::filesystem_service::permissions::check_mkdir_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(parent))?;

        // Step 3: Check if directory already exists
        let path = parent_record.path.join(name);
        if let Ok(_existing) = self.metadata_store.get_file_by_path(&path).await {
            self.api_metrics
                .record_call("mkdir", _start.elapsed().as_secs_f64());
            return Err(Error::AlreadyExists(path.to_string_lossy().into_owned()));
        }

        // Step 4: Reserve inode before Raft operation
        let inode = self
            .metadata_store
            .reserve_inode()
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 5: Create directory through Raft for consistency
        use crate::filesystem_service::raft_commands::{FileType as RaftFileType, RaftCommand};
        let command = RaftCommand::CreateFile {
            parent_inode: parent,
            name: name.to_string(),
            file_type: RaftFileType::Directory,
            mode,
            uid,
            gid,
        };

        let result = self
            .raft_client
            .propose_raft_command(command)
            .await
            .map_err(|e| {
                // Release reserved inode on error
                let _ = self.metadata_store.release_inode(inode);
                Error::RaftError(format!("Failed to create directory: {}", e))
            })?;

        // Step 6: Extract inode and file_id from Raft result
        // Directory is already created on all nodes by Raft state machine
        let (raft_inode, file_id) = match result {
            crate::filesystem_service::raft_commands::RaftCommandResult::FileCreated {
                inode: raft_inode,
                file_id,
            } => (raft_inode, file_id),
            crate::filesystem_service::raft_commands::RaftCommandResult::Error { message } => {
                let _ = self.metadata_store.release_inode(inode).await;
                self.api_metrics
                    .record_call("mkdir", _start.elapsed().as_secs_f64());
                return Err(Error::MetadataError(message));
            }
            _ => {
                let _ = self.metadata_store.release_inode(inode).await;
                self.api_metrics
                    .record_call("mkdir", _start.elapsed().as_secs_f64());
                return Err(Error::Internal(
                    "Unexpected Raft result for directory creation".into(),
                ));
            }
        };

        // Release the locally reserved inode since Raft generated a different one
        let _ = self.metadata_store.release_inode(inode).await;

        // Step 7: Directory is already created by Raft state machine on all nodes
        // Fetch the directory metadata to populate caches
        let file_record = self
            .metadata_store
            .get_file_by_inode(raft_inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 8: Create FileAttr for the response
        let now = SystemTime::now();
        let attr = FileAttr {
            ino: raft_inode,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: mode as u16,
            nlink: 1, // Always 1 for all files/directories (see docs/posix_compliance.md)
            uid,
            gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        };

        // Step 9: Cache the new directory
        let metadata = crate::metadata_store::FileMetadata {
            file_type: file_record.file_type,
            size: file_record.size,
            permissions: file_record.permissions,
            uid: file_record.uid,
            gid: file_record.gid,
            created_at: file_record.created_at,
            modified_at: file_record.modified_at,
            accessed_at: file_record.accessed_at,
            target: file_record.target.clone(),
        };
        self.inode_manager
            .cache()
            .insert(raft_inode, file_id, metadata);

        tracing::info!(
            "Created directory via Raft: path={:?}, inode={}, file_id={:?}",
            path,
            raft_inode,
            file_id
        );

        self.api_metrics
            .record_call("mkdir", _start.elapsed().as_secs_f64());
        Ok(attr)
    }

    async fn rmdir(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<(), Error> {
        let _start = Instant::now();
        tracing::debug!(
            "rmdir: parent={}, name={}, uid={}, gid={}",
            parent,
            name,
            uid,
            gid
        );

        // Step 1: Validate parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            self.api_metrics
                .record_call("rmdir", _start.elapsed().as_secs_f64());
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Check rmdir permission on parent directory (write + execute)
        crate::filesystem_service::permissions::check_rmdir_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(parent))?;

        // Step 3: Construct path and lookup target directory
        let path = parent_record.path.join(name);
        let dir_record = self
            .metadata_store
            .get_file_by_path(&path)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 4: Verify target is a directory (not a file or symlink)
        if dir_record.file_type != crate::metadata_store::FileType::Directory {
            self.api_metrics
                .record_call("rmdir", _start.elapsed().as_secs_f64());
            return Err(Error::NotADirectory(dir_record.inode));
        }

        // Step 5: Check if directory is empty
        // A directory is empty if it contains no entries (list_directory returns empty)
        let children = self
            .metadata_store
            .list_directory(&path)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if !children.is_empty() {
            self.api_metrics
                .record_call("rmdir", _start.elapsed().as_secs_f64());
            return Err(Error::DirectoryNotEmpty(dir_record.inode));
        }

        // Step 6: Propose directory deletion through Raft (handles metadata cleanup)
        use crate::filesystem_service::raft_commands::RaftCommand;
        let command = RaftCommand::DeleteFile {
            parent_inode: parent,
            name: name.to_string(),
        };

        let result = self
            .raft_client
            .propose_raft_command(command)
            .await
            .map_err(|e| {
                let error_msg = format!("{}", e);
                // Convert specific Raft errors to appropriate FileSystemService errors
                if error_msg.contains("File not found") || error_msg.contains("not found") {
                    Error::MetadataError(format!("Directory not found: {}", name))
                } else {
                    Error::RaftError(error_msg)
                }
            })?;

        // Step 7: Check Raft result
        match result {
            crate::filesystem_service::raft_commands::RaftCommandResult::FileDeleted => {
                // Success - file already deleted by Raft stub
            }
            crate::filesystem_service::raft_commands::RaftCommandResult::Error { message } => {
                self.api_metrics
                    .record_call("rmdir", _start.elapsed().as_secs_f64());
                return Err(Error::MetadataError(message));
            }
            _ => {
                self.api_metrics
                    .record_call("rmdir", _start.elapsed().as_secs_f64());
                return Err(Error::Internal(
                    "Unexpected Raft result for directory deletion".into(),
                ));
            }
        }

        // Step 8: Invalidate cache
        // Note: File is already deleted from metadata store by Raft stub
        self.inode_manager.cache().invalidate(dir_record.inode);

        tracing::info!(
            "Removed directory: path={:?}, inode={}",
            path,
            dir_record.inode
        );

        self.api_metrics
            .record_call("rmdir", _start.elapsed().as_secs_f64());
        Ok(())
    }

    async fn readdir(
        &self,
        inode: u64,
        _offset: i64,
        _client_id: ClientId,
    ) -> Result<Vec<DirEntry>, Error> {
        let _start = Instant::now();
        // Get the directory's file record
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Determine parent inode
        // For root directory ("/"), parent is itself
        // For all other directories, look up parent by parent_path
        let parent_inode = if inode == ROOT_INODE {
            ROOT_INODE
        } else {
            // Look up parent directory by parent_path
            match self
                .metadata_store
                .get_file_by_path(&record.parent_path)
                .await
            {
                Ok(parent_record) => parent_record.inode,
                Err(_) => {
                    // If parent lookup fails, fall back to root
                    // This shouldn't happen in a consistent filesystem, but provides safety
                    tracing::warn!(
                        "Failed to find parent directory for path {:?}, falling back to root",
                        record.path
                    );
                    ROOT_INODE
                }
            }
        };

        // List files in this directory
        let files = self
            .metadata_store
            .list_directory(&record.path)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Convert to DirEntry
        let mut entries = vec![
            // Add . and .. entries
            DirEntry {
                ino: inode,
                name: ".".to_string(),
                kind: FileType::Directory,
            },
            DirEntry {
                ino: parent_inode,
                name: "..".to_string(),
                kind: FileType::Directory,
            },
        ];

        for file in files {
            let name = file
                .path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();

            // Skip entries with empty names (e.g., root directory "/" in its own listing)
            if name.is_empty() {
                continue;
            }

            // Convert MetadataStore FileType to FileSystemService FileType
            let kind = match file.file_type {
                crate::metadata_store::FileType::Directory => FileType::Directory,
                crate::metadata_store::FileType::RegularFile => FileType::RegularFile,
                crate::metadata_store::FileType::Symlink => FileType::Symlink,
            };

            entries.push(DirEntry {
                ino: file.inode,
                name,
                kind,
            });
        }

        self.api_metrics
            .record_call("readdir", _start.elapsed().as_secs_f64());
        Ok(entries)
    }

    // ===== Metadata Operations =====

    async fn getattr(&self, inode: u64) -> Result<FileAttr, Error> {
        let _start = Instant::now();

        // Fast path: If file has an open BufferedFileHandle, use its cached metadata
        // BufferedFileHandle maintains up-to-date attributes, so no flush needed
        if let Some(buffered_handle) = self.get_buffered_handle_by_inode(inode) {
            let result = Ok(buffered_handle.get_file_by_inode());
            self.api_metrics
                .record_call("getattr", _start.elapsed().as_secs_f64());
            return result;
        }

        // Slow path: No open file handle, need to query MetadataStore
        // First check inode_manager cache
        if let Some(cached) = self.inode_manager.cache().get(inode) {
            let result = Ok(self.cached_metadata_to_attr(inode, &cached));
            self.api_metrics
                .record_call("getattr", _start.elapsed().as_secs_f64());
            return result;
        }

        // Cache miss - query MetadataStore
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Update cache with fresh data from database
        let metadata = FileMetadata {
            file_type: record.file_type,
            size: record.size,
            permissions: record.permissions,
            uid: record.uid,
            gid: record.gid,
            created_at: record.created_at,
            modified_at: record.modified_at,
            accessed_at: record.accessed_at,
            target: record.target.clone(), // Include target for symlinks
        };
        self.inode_manager
            .cache()
            .insert(record.inode, record.file_id, metadata);

        let result = Ok(self.file_record_to_attr(&record));
        self.api_metrics
            .record_call("getattr", _start.elapsed().as_secs_f64());
        result
    }

    async fn resolve_path(&self, path: &std::path::Path) -> Result<u64, Error> {
        let _start = Instant::now();

        // Query metadata store to resolve path to inode
        let record = self
            .metadata_store
            .get_file_by_path(path)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        self.api_metrics
            .record_call("resolve_path", _start.elapsed().as_secs_f64());

        Ok(record.inode)
    }

    async fn setattr(
        &self,
        inode: u64,
        file_handle: Option<u64>,
        mode: Option<u32>,
        new_uid: Option<u32>,
        new_gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        req_uid: u32,
        req_gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        let _start = Instant::now();
        tracing::debug!(
            "setattr: inode={}, file_handle={:?}, mode={:?}, size={:?}, req_uid={}, req_gid={}",
            inode,
            file_handle,
            mode,
            size,
            req_uid,
            req_gid
        );

        // Step 1: Get current metadata from MetadataStore
        // We need file_id and other fields not available in FileAttr
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 2: Check permissions
        // Changing ownership or permissions requires being the owner
        if mode.is_some() || new_uid.is_some() || new_gid.is_some() {
            crate::filesystem_service::permissions::check_owner_permission(
                req_uid, record.uid, inode,
            )?;
        }

        // Changing size (truncate) requires write permission
        if size.is_some() {
            crate::filesystem_service::permissions::check_write_permission(
                req_uid,
                req_gid,
                record.uid,
                record.gid,
                record.permissions,
            )
            .map_err(|_| Error::PermissionDenied(inode))?;
        }

        // Step 3: Validate new size against max_file_size
        if let Some(new_size) = size {
            if new_size > self.config.max_file_size {
                self.api_metrics
                    .record_call("setattr", _start.elapsed().as_secs_f64());
                return Err(Error::NoSpace); // ENOSPC - file would exceed maximum size
            }
        }

        // Step 3: Handle truncation if size is changing (DATA PLANE)
        if let Some(new_size) = size {
            // CRITICAL: Inform ALL BufferedFileHandles for this inode about truncation
            // This ensures pending writes are flushed BEFORE we delete stripe metadata
            // and that all handles update their cached file size
            let buffered_handles: Vec<std::sync::Arc<BufferedFileHandle>> = {
                let open_files = self
                    .open_files
                    .read()
                    .expect("open_files lock poisoned - indicates panic in file operation");

                if let Some(fh) = file_handle {
                    // Specific file handle provided - inform just that one
                    open_files
                        .get(&fh)
                        .and_then(|of| of.buffered_handle.clone())
                        .into_iter()
                        .collect()
                } else {
                    // No file handle provided - find ALL handles for this inode
                    // This handles external truncation (e.g., via truncate() syscall)
                    open_files
                        .values()
                        .filter(|of| of.inode == inode)
                        .filter_map(|of| of.buffered_handle.clone())
                        .collect()
                }
            };

            // Inform all matching handles about truncation
            for handle in &buffered_handles {
                handle
                    .inform(
                        crate::filesystem_service::buffered_file_handle::OperationType::Truncate,
                    )
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to inform BufferedFileHandle: {}", e))
                    })?;
            }

            // Get the current effective size (BufferedFileHandle knows about unflushed writes)
            let current_size = buffered_handles
                .first()
                .map(|handle| handle.attributes().size)
                .unwrap_or(record.size);

            if new_size < current_size {
                // Shrinking - need to delete/truncate stripes
                let stripe_size = self.stripe_size();

                let new_last_stripe_idx = if new_size == 0 {
                    0
                } else {
                    (new_size - 1) / stripe_size
                };

                let old_last_stripe_idx = if current_size == 0 {
                    0
                } else {
                    (current_size - 1) / stripe_size
                };

                tracing::debug!(
                    "setattr: truncating from {} to {} bytes (stripes {} to {})",
                    record.size,
                    new_size,
                    old_last_stripe_idx,
                    new_last_stripe_idx
                );

                // Get all stripes
                let stripes = self
                    .metadata_store
                    .get_file_stripes(record.file_id)
                    .await
                    .map_err(|e| self.convert_metadata_error(e))?;

                // Delete stripes beyond the new size
                for stripe in stripes {
                    let stripe_idx = stripe.offset / stripe_size;

                    if stripe_idx > new_last_stripe_idx {
                        // Delete entire stripe (metadata and chunks)
                        tracing::debug!(
                            "setattr: deleting stripe at index {} (stripe_id={:?})",
                            stripe_idx,
                            stripe.stripe_id
                        );

                        // Delete stripe metadata and associated chunks from database
                        // Physical chunk deletion is deferred to StorageWatchdog (Phase 1)
                        self.metadata_store
                            .delete_stripe(stripe.stripe_id)
                            .await
                            .map_err(|e| self.convert_metadata_error(e))?;

                        tracing::debug!(
                            "setattr: deleted stripe metadata for stripe {}",
                            stripe_idx
                        );
                    } else if stripe_idx == new_last_stripe_idx && new_size % stripe_size != 0 {
                        // Partial truncation of last stripe - leave as-is for Phase 1
                        // Proper implementation would truncate the stripe data, but that requires
                        // read-modify-write which is deferred to Phase 2
                        tracing::debug!(
                            "setattr: partial truncation of stripe {} (Phase 1: left as-is, wasted space)",
                            stripe_idx
                        );
                    }
                }
            }
            // Growing the file - no action needed (sparse file semantics)
        }

        // Step 4: Propose metadata update through Raft (CONTROL PLANE)
        // Note: We ignore atime parameter - WormFS doesn't track access time
        use crate::filesystem_service::raft_commands::{FileUpdateFields, RaftCommand};
        let command = RaftCommand::UpdateFile {
            inode,
            updates: FileUpdateFields {
                size,
                mode,
                uid: new_uid,
                gid: new_gid,
                atime: None, // Always None - we don't track access time
                mtime,
            },
        };

        let _result = self
            .raft_client
            .propose_raft_command(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        // Step 5: [TEMP Phase 1] Update metadata store directly
        let now = SystemTime::now();
        let updated_metadata = FileMetadata {
            file_type: record.file_type,
            size: size.unwrap_or(record.size),
            permissions: mode.unwrap_or(record.permissions),
            uid: new_uid.unwrap_or(record.uid),
            gid: new_gid.unwrap_or(record.gid),
            created_at: record.created_at,
            modified_at: mtime.unwrap_or(now),
            accessed_at: record.accessed_at, // Never update - preserved as-is
            target: record.target.clone(),   // Preserve target for symlinks
        };

        self.metadata_store
            .update_file(record.file_id, updated_metadata.clone())
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 6: Build updated FileAttr
        let attr = FileAttr {
            ino: inode,
            size: updated_metadata.size,
            blocks: (updated_metadata.size + 4095) / 4096,
            atime: updated_metadata.accessed_at,
            mtime: updated_metadata.modified_at,
            ctime: now, // ctime updated on metadata change
            crtime: record.created_at,
            kind: match record.file_type {
                crate::metadata_store::FileType::Directory => FileType::Directory,
                crate::metadata_store::FileType::RegularFile => FileType::RegularFile,
                crate::metadata_store::FileType::Symlink => FileType::Symlink,
            },
            perm: updated_metadata.permissions as u16,
            nlink: 1,
            uid: updated_metadata.uid,
            gid: updated_metadata.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        };

        // Step 7: Update BufferedFileHandle cache if file is open
        let buffered_handle = if let Some(fh) = file_handle {
            // Look up by file handle
            let open_files = self
                .open_files
                .read()
                .expect("open_files lock poisoned - indicates panic in file operation");
            open_files
                .get(&fh)
                .and_then(|of| of.buffered_handle.clone())
        } else {
            // Look up by inode
            self.get_buffered_handle_by_inode(inode)
        };

        if let Some(handle) = buffered_handle {
            // Update cached attributes in BufferedFileHandle
            handle.update_attributes(attr.clone());
        }

        // Also invalidate inode_manager cache for consistency
        self.inode_manager.cache().invalidate(inode);

        tracing::info!("Updated attributes for inode {}", inode);
        self.api_metrics
            .record_call("setattr", _start.elapsed().as_secs_f64());
        Ok(attr)
    }

    // ===== Lock Operations =====

    async fn acquire_lock(
        &self,
        _inode: u64,
        _lock_type: LockType,
        _expires_at: SystemTime,
        _client_id: ClientId,
    ) -> Result<u64, Error> {
        let _start = Instant::now();
        // Phase 1: Stub - locks will be implemented in Step 8
        let result = Err(Error::NotSupported(
            "locks not implemented in Step 7".into(),
        ));
        self.api_metrics
            .record_call("acquire_lock", _start.elapsed().as_secs_f64());
        result
    }

    async fn release_lock(&self, _inode: u64, _client_id: ClientId) -> Result<(), Error> {
        let _start = Instant::now();
        // Phase 1: Stub - locks will be implemented in Step 8
        let result = Err(Error::NotSupported(
            "locks not implemented in Step 7".into(),
        ));
        self.api_metrics
            .record_call("release_lock", _start.elapsed().as_secs_f64());
        result
    }

    async fn extend_lock(
        &self,
        _inode: u64,
        _new_expiry: SystemTime,
        _client_id: ClientId,
    ) -> Result<(), Error> {
        let _start = Instant::now();
        // Phase 1: Stub - locks will be implemented in Step 8
        let result = Err(Error::NotSupported(
            "locks not implemented in Step 7".into(),
        ));
        self.api_metrics
            .record_call("extend_lock", _start.elapsed().as_secs_f64());
        result
    }
}

// Tests will be added via FUSE integration tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_store::FileStore;
    use crate::metadata_store::{
        factory::MetadataStoreFactory, types::Config as MetadataConfig, types::IsolationLevel,
        types::SynchronousMode, MetadataStore,
    };
    use tempfile::TempDir;

    /// Test user ID (matches the uid used in create() calls in tests)
    const TEST_UID: u32 = 1000;
    /// Test group ID (matches the gid used in create() calls in tests)
    const TEST_GID: u32 = 1000;

    /// Create a test FileSystemService instance with temporary storage
    async fn create_test_service() -> Arc<FileSystemServiceImpl> {
        let metadata_config = MetadataConfig {
            database_path: ":memory:".into(),
            read_pool_size: 5,
            enable_wal: false,
            cache_size_mb: 64,
            enable_foreign_keys: false,
            synchronous: SynchronousMode::Normal,
            transaction_isolation: IsolationLevel::ReadCommitted,
            enable_prepared_statements: false,
            read_pool_timeout_secs: 5,
            stripe_cache_size_mb: 64,
            stripe_cache_ttl_secs: 10,
            stripe_cache_tti_secs: 5,
            chunk_cache_size_mb: 64,
            chunk_cache_ttl_secs: 10,
            chunk_cache_tti_secs: 5,
        };

        let metadata_store = MetadataStoreFactory::create_concrete(metadata_config)
            .await
            .unwrap();
        metadata_store.initialize_schema().await.unwrap();

        // Create temp dir for file store
        let temp_dir = TempDir::new().unwrap();

        let file_store_config = crate::file_store::types::Config {
            disk_paths: vec![temp_dir.path().to_path_buf()],
            max_chunk_size: 512,
            default_data_shards: 2,
            default_parity_shards: 1,
            max_concurrent_operations: 10,
            verification_interval: Duration::from_secs(3600),
            orphan_cleanup_age: Duration::from_secs(3600),
            stripe_cache_size_mb: 256,
            stripe_cache_ttl_secs: 3600,
            stripe_cache_tti_secs: 600,
        };

        let file_store = Arc::new(FileStore::new(file_store_config).unwrap());

        let fs_config = Config {
            uid: 1000,
            gid: 1000,
            lock_timeout: Duration::from_secs(2), // Short timeout for testing
            lock_extend_interval: Duration::from_millis(500), // Fast extension for testing
            ..Default::default()
        };

        Arc::new(FileSystemServiceImpl::new(
            fs_config,
            metadata_store,
            file_store,
            None, // Use default RaftClient stub
        ))
    }

    #[tokio::test]
    async fn test_lock_extension_keeps_lock_alive() {
        let service = create_test_service().await;

        // Create root directory
        service.initialize_root().await.unwrap();

        let client_id = ClientId::new(123);

        // Create a file using the FileSystemService trait
        let file_attr = service
            .create(
                ROOT_INODE, "test.txt", 0o644, // mode
                1000,  // uid
                1000,  // gid
                client_id,
            )
            .await
            .unwrap();

        // Open file for writing (acquires lock), flags=0x02 is O_RDWR
        let (fh, _attr) = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, client_id)
            .await
            .unwrap();

        // Start background tasks (lock extension)
        Arc::clone(&service).start_background_tasks();

        // Sleep for 5 seconds (longer than lock_timeout of 2 seconds)
        // If lock extension is working, the lock should still be held
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Verify file is still open with lock
        {
            let open_files = service
                .open_files
                .read()
                .expect("open_files lock poisoned in test");
            let open_file = open_files.get(&fh).expect("File handle should still exist");
            assert!(open_file.lock_id.is_some(), "Lock should still be held");
        }

        // Try to acquire the same lock from a different client (should fail)
        let other_client = ClientId::new(456);
        let result = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, other_client)
            .await;
        assert!(
            result.is_err(),
            "Should not be able to acquire lock held by another client"
        );

        // Release the file
        service.release(fh).await.unwrap();

        // Shutdown to clean up background tasks
        service.shutdown().await;
    }

    #[tokio::test]
    async fn test_lock_expires_without_heartbeat() {
        let service = create_test_service().await;

        // Create root directory
        service.initialize_root().await.unwrap();

        let client_id = ClientId::new(123);

        // Create a file
        let file_attr = service
            .create(
                ROOT_INODE, "test.txt", 0o644, // mode
                1000,  // uid
                1000,  // gid
                client_id,
            )
            .await
            .unwrap();

        // Open file for writing (acquires lock, registers heartbeat)
        let (fh, _attr) = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, client_id)
            .await
            .unwrap();

        // Start background tasks
        Arc::clone(&service).start_background_tasks();

        // Remove the client from sessions (simulate no heartbeat)
        {
            let mut sessions = service
                .client_sessions
                .write()
                .expect("client_sessions lock poisoned in test");
            sessions.remove(&client_id);
        }

        // Sleep for longer than lock_timeout
        tokio::time::sleep(Duration::from_secs(3)).await;

        // The lock extension task should NOT extend the lock because
        // the client has no heartbeat. However, in our current implementation,
        // the lock is registered once during open(). This test verifies the
        // infrastructure is in place for Phase 2 heartbeat tracking.

        // For now, just verify the heartbeat removal worked
        {
            let sessions = service
                .client_sessions
                .read()
                .expect("client_sessions lock poisoned in test");
            assert!(
                !sessions.contains_key(&client_id),
                "Client session should be removed"
            );
        }

        // Release and shutdown
        service.release(fh).await.unwrap();
        service.shutdown().await;
    }

    #[tokio::test]
    async fn test_heartbeat_registration() {
        let service = create_test_service().await;

        let client_id = ClientId::new(789);

        // Initially no session
        {
            let sessions = service
                .client_sessions
                .read()
                .expect("client_sessions lock poisoned in test");
            assert!(!sessions.contains_key(&client_id));
        }

        // Call heartbeat
        service.heartbeat(client_id);

        // Should now have session
        {
            let sessions = service
                .client_sessions
                .read()
                .expect("client_sessions lock poisoned in test");
            assert!(
                sessions.contains_key(&client_id),
                "Client session should be registered"
            );
            let last_heartbeat = sessions.get(&client_id).unwrap();

            // Heartbeat should be recent (within last second)
            let age = SystemTime::now().duration_since(*last_heartbeat).unwrap();
            assert!(age < Duration::from_secs(1), "Heartbeat should be recent");
        }
    }

    #[tokio::test]
    async fn test_write_overflow_detection() {
        let service = create_test_service().await;

        // Create root directory
        service.initialize_root().await.unwrap();

        let client_id = ClientId::new(1);

        // Create a test file
        let file_attr = service
            .create(ROOT_INODE, "test.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        let (fh, _) = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, client_id)
            .await
            .unwrap();

        // Try to write at near u64::MAX offset - should fail gracefully
        let data = vec![0u8; 1000];
        let result = service
            .write(
                file_attr.ino,
                fh,
                u64::MAX - 100,
                data,
                TEST_UID,
                TEST_GID,
                client_id,
            )
            .await;

        assert!(result.is_err(), "Should detect overflow");

        // Verify error message mentions overflow
        match result {
            Err(Error::InvalidArgument(msg)) => {
                assert!(
                    msg.contains("overflow"),
                    "Error should mention overflow: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidArgument error, got: {:?}", e),
            Ok(_) => panic!("Should have failed with overflow error"),
        }

        // Clean up
        service.release(fh).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_overflow_detection() {
        let service = create_test_service().await;

        // Create root directory
        service.initialize_root().await.unwrap();

        let client_id = ClientId::new(1);

        // Create a test file
        let file_attr = service
            .create(ROOT_INODE, "test.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        let (fh, _) = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, client_id)
            .await
            .unwrap();

        // Try to read at near u64::MAX offset - should fail gracefully or return empty
        // (read is clamped to file size, so it might succeed with empty result)
        let result = service
            .read(
                file_attr.ino,
                fh,
                u64::MAX - 100,
                1000,
                TEST_UID,
                TEST_GID,
                client_id,
            )
            .await;

        // Either succeeds with empty data (offset > file_size) or detects overflow
        match result {
            Ok(data) => {
                assert!(
                    data.is_empty(),
                    "Should return empty data for read beyond file size"
                );
            }
            Err(Error::InvalidArgument(msg)) if msg.contains("overflow") => {
                // Also acceptable - overflow detected
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }

        // Clean up
        service.release(fh).await.unwrap();
    }

    #[tokio::test]
    async fn test_permission_denied_read() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file owned by uid=1000 with mode 0o600 (rw-------)
        // Only the owner can read/write
        let file_attr = service
            .create(ROOT_INODE, "private.txt", 0o600, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to open for read as a different user (uid=2000) - should fail
        let result = service
            .open(file_attr.ino, libc::O_RDONLY as u32, 2000, 2000, client_id)
            .await;

        assert!(result.is_err(), "Should deny read access to non-owner");
        match result {
            Err(Error::PermissionDenied(inode)) => {
                assert_eq!(inode, file_attr.ino);
            }
            _ => panic!("Expected PermissionDenied error"),
        }
    }

    #[tokio::test]
    async fn test_permission_denied_write() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file owned by uid=1000 with mode 0o644 (rw-r--r--)
        // Owner can write, but group and others cannot
        let file_attr = service
            .create(ROOT_INODE, "readonly.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to open for writing as a different user (uid=2000) - should fail at open
        let result = service
            .open(file_attr.ino, 0x02, 2000, 2000, client_id)
            .await;

        assert!(result.is_err(), "Should deny write open to non-owner");
        match result {
            Err(Error::PermissionDenied(inode)) => {
                assert_eq!(inode, file_attr.ino);
            }
            _ => panic!("Expected PermissionDenied error, got: {:?}", result),
        }

        // Open the file as owner to get a valid file handle
        let (fh, _) = service
            .open(file_attr.ino, 0x02, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to write using owner's file handle but as a different user (uid=2000) - should fail permission check
        let data = vec![1u8; 100];
        let result = service
            .write(file_attr.ino, fh, 0, data, 2000, 2000, client_id)
            .await;

        assert!(result.is_err(), "Should deny write access to non-owner");
        match result {
            Err(Error::PermissionDenied(inode)) => {
                assert_eq!(inode, file_attr.ino);
            }
            _ => panic!("Expected PermissionDenied error"),
        }
    }

    #[tokio::test]
    async fn test_permission_owner_precedence() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file with mode 0o077 (---rwxrwx)
        // Owner has NO permissions, but group and others have full permissions
        // This tests POSIX precedence: owner permissions checked first
        let file_attr = service
            .create(ROOT_INODE, "weird.txt", 0o077, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to open for read as the owner (uid=1000) - should FAIL
        // Even though group has read permission, owner permissions take precedence
        let result = service
            .open(file_attr.ino, libc::O_RDONLY as u32, 1000, 1000, client_id)
            .await;

        assert!(result.is_err(), "Owner should be denied due to precedence");
        assert!(matches!(result, Err(Error::PermissionDenied(_))));

        // Try to open for read as a group member (uid=2000, gid=1000) - should SUCCEED
        let result = service
            .open(file_attr.ino, libc::O_RDONLY as u32, 2000, 1000, client_id)
            .await;

        assert!(result.is_ok(), "Group member should be able to read");

        // Clean up
        if let Ok((fh, _)) = result {
            service.release(fh).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_permission_group_access() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file with mode 0o640 (rw-r-----)
        // Owner can read/write, group can read, others have no access
        let file_attr = service
            .create(ROOT_INODE, "group.txt", 0o640, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to open for read as group member (uid=2000, gid=1000) - should succeed
        // Note: O_RDONLY = 0x00 (not 0x01 which is O_WRONLY!)
        let result = service
            .open(file_attr.ino, 0x00, 2000, 1000, client_id)
            .await;
        assert!(
            result.is_ok(),
            "Group member should be able to open for read. Error: {:?}",
            result.as_ref().err()
        );
        if let Ok((fh, _)) = result {
            service.release(fh).await.unwrap();
        }

        // Try to open for write as group member - should fail (group has no write permission)
        // O_WRONLY = 0x01
        let result = service
            .open(file_attr.ino, 0x01, 2000, 1000, client_id)
            .await;
        assert!(
            result.is_err(),
            "Group member should not be able to open for write"
        );

        // Try to open for read as other (uid=2000, gid=2000) - should fail
        let result = service
            .open(file_attr.ino, 0x00, 2000, 2000, client_id)
            .await;
        assert!(result.is_err(), "Other should not be able to open for read");
    }

    #[tokio::test]
    async fn test_permission_unlink_requires_parent_write() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file - the file's permissions don't matter for unlink
        let file_attr = service
            .create(ROOT_INODE, "deleteme.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to unlink as non-owner of parent directory (uid=2000)
        // Root is owned by uid=1000, so this should fail
        let result = service
            .unlink(ROOT_INODE, "deleteme.txt", 2000, 2000, client_id)
            .await;

        assert!(
            result.is_err(),
            "Should deny unlink without write permission on parent"
        );
        match result {
            Err(Error::PermissionDenied(inode)) => {
                assert_eq!(inode, ROOT_INODE);
            }
            _ => panic!("Expected PermissionDenied error on parent directory"),
        }

        // Unlink as owner of parent directory - should succeed
        let result = service
            .unlink(ROOT_INODE, "deleteme.txt", 1000, 1000, client_id)
            .await;
        assert!(result.is_ok(), "Owner of parent should be able to unlink");
    }

    #[tokio::test]
    async fn test_permission_setattr_requires_ownership() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file owned by uid=1000
        let file_attr = service
            .create(ROOT_INODE, "changeme.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to change permissions as non-owner (uid=2000) - should fail
        let result = service
            .setattr(
                file_attr.ino,
                None, // file_handle
                Some(0o600),
                None,
                None,
                None,
                None,
                None,
                2000,
                2000,
                client_id,
            )
            .await;

        assert!(
            result.is_err(),
            "Non-owner should not be able to change permissions"
        );
        assert!(matches!(result, Err(Error::PermissionDenied(_))));

        // Change permissions as owner - should succeed
        let result = service
            .setattr(
                file_attr.ino,
                None, // file_handle
                Some(0o600),
                None,
                None,
                None,
                None,
                None,
                1000,
                1000,
                client_id,
            )
            .await;
        assert!(result.is_ok(), "Owner should be able to change permissions");
    }

    /*
    use super::*;
    use crate::metadata_store::MetadataStore;
    use tempfile::TempDir;

    async fn create_test_service() -> (FileSystemServiceImpl, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("metadata.db");

        let metadata_config = crate::metadata_store::Config {
            database_path: db_path,
            ..Default::default()
        };

        let metadata_store = MetadataStoreImpl::new(metadata_config)
            .await
            .unwrap();
        metadata_store.initialize_schema().await.unwrap();

        let file_store_config = crate::file_store::types::Config {
            disk_paths: vec![temp_dir.path().to_path_buf()],
            max_chunk_size: 512,
            default_data_shards: 2,
            default_parity_shards: 1,
            max_concurrent_operations: 10,
            verification_interval: std::time::Duration::from_secs(3600),
            orphan_cleanup_age: std::time::Duration::from_secs(3600),
        };

        let file_store = Arc::new(FileStore::new(file_store_config).unwrap());

        let fs_config = Config {
            uid: 1000,
            gid: 1000,
            ..Default::default()
        };

        let service = FileSystemServiceImpl::new(fs_config, metadata_store, file_store);

        (service, temp_dir)
    }

    #[tokio::test]
    async fn test_initialize_root() {
        let (service, _temp_dir) = create_test_service().await;

        // Initialize root
        service.initialize_root().await.unwrap();

        // Should be able to get root attributes
        let attr = service.getattr(ROOT_INODE).await.unwrap();
        assert_eq!(attr.ino, ROOT_INODE);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[tokio::test]
    async fn test_initialize_root_idempotent() {
        let (service, _temp_dir) = create_test_service().await;

        // Initialize root twice - should not error
        service.initialize_root().await.unwrap();
        service.initialize_root().await.unwrap();

        // Root should still be accessible
        let attr = service.getattr(ROOT_INODE).await.unwrap();
        assert_eq!(attr.ino, ROOT_INODE);
    }

    #[tokio::test]
    async fn test_getattr_nonexistent() {
        let (service, _temp_dir) = create_test_service().await;
        service.initialize_root().await.unwrap();

        // Try to get attributes for non-existent inode
        let result = service.getattr(999).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_getattr_caching() {
        let (service, _temp_dir) = create_test_service().await;
        service.initialize_root().await.unwrap();

        // First getattr - cache miss
        let attr1 = service.getattr(ROOT_INODE).await.unwrap();
        assert_eq!(attr1.ino, ROOT_INODE);

        // Check cache has entry
        let cache = service.inode_cache();
        assert_eq!(cache.len(), 1);

        // Second getattr - should still work
        let attr2 = service.getattr(ROOT_INODE).await.unwrap();
        assert_eq!(attr2.ino, ROOT_INODE);
    }
    */

    #[tokio::test]
    async fn test_create_in_non_directory_fails() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(100);

        // Create a regular file
        let file_attr = service
            .create(
                ROOT_INODE,
                "regular.txt",
                0o644,
                TEST_UID,
                TEST_GID,
                client_id,
            )
            .await
            .unwrap();

        // Try to create a file inside the regular file (should fail - not a directory)
        let result = service
            .create(
                file_attr.ino,
                "child.txt",
                0o644,
                TEST_UID,
                TEST_GID,
                client_id,
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::NotADirectory(inode) => assert_eq!(inode, file_attr.ino),
            e => panic!("Expected NotADirectory error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_open_directory_for_writing_fails() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(100);

        // Try to open root directory for writing (should fail - is a directory)
        let result = service
            .open(ROOT_INODE, 0x02, TEST_UID, TEST_GID, client_id)
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::IsADirectory(inode) => assert_eq!(inode, ROOT_INODE),
            e => panic!("Expected IsADirectory error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_release_nonexistent_file_handle_succeeds() {
        let service = create_test_service().await;

        // Try to release a non-existent file handle
        // Note: Release is idempotent and doesn't error on invalid handles
        let invalid_fh = 99999;
        let result = service.release(invalid_fh).await;

        // Should succeed (idempotent operation)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_read_with_invalid_file_handle_returns_error() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(100);

        // Create a file
        let file_attr = service
            .create(ROOT_INODE, "test.txt", 0o644, TEST_UID, TEST_GID, client_id)
            .await
            .unwrap();

        // Try to read with an invalid file handle (non-zero but not open)
        // Should return InvalidFileHandle error (no fallback path anymore)
        let invalid_fh = 99999;
        let result = service
            .read(
                file_attr.ino,
                invalid_fh,
                0,
                1024,
                TEST_UID,
                TEST_GID,
                client_id,
            )
            .await;

        // Should fail with InvalidFileHandle error
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::InvalidFileHandle(_))));
    }

    #[tokio::test]
    async fn test_multiple_heartbeats_update_timestamp() {
        let service = create_test_service().await;
        let client_id = ClientId::new(999);

        // First heartbeat
        service.heartbeat(client_id);

        let first_timestamp = {
            let sessions = service
                .client_sessions
                .read()
                .expect("client_sessions lock poisoned in test");
            *sessions.get(&client_id).unwrap()
        };

        // Wait a bit
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second heartbeat
        service.heartbeat(client_id);

        let second_timestamp = {
            let sessions = service
                .client_sessions
                .read()
                .expect("client_sessions lock poisoned in test");
            *sessions.get(&client_id).unwrap()
        };

        // Second timestamp should be later than first
        assert!(
            second_timestamp > first_timestamp,
            "Second heartbeat should update timestamp"
        );
    }
}
