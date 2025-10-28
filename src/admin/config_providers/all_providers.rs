//! All configuration providers for WormFS components.
//!
//! This file contains config provider implementations for all major components.

use crate::admin::config_provider::{ConfigProvider, ConfigWithDescriptions};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Configuration provider for Admin component.
pub struct AdminConfigProvider {
    /// Optional admin configuration. None if admin server is disabled.
    pub config: Option<crate::admin::Config>,
}

impl ConfigProvider for AdminConfigProvider {
    fn name(&self) -> &'static str {
        "admin"
    }

    fn get_config_with_descriptions(&self) -> ConfigWithDescriptions {
        let values = if let Some(ref config) = self.config {
            json!({
                "enabled": config.enabled,
                "port": config.port,
                "bind_address": config.bind_address,
            })
        } else {
            json!({
                "enabled": false,
            })
        };

        let mut descriptions = HashMap::new();
        descriptions.insert(
            "enabled".to_string(),
            "Enable the admin HTTP server for monitoring and management".to_string(),
        );
        descriptions.insert(
            "port".to_string(),
            "Port to bind the admin server (default: 9090)".to_string(),
        );
        descriptions.insert(
            "bind_address".to_string(),
            "Bind address for admin server (127.0.0.1 for localhost only)".to_string(),
        );

        ConfigWithDescriptions::new(values, descriptions)
    }
}

/// Configuration provider for Metrics component.
pub struct MetricsConfigProvider {
    /// Optional metrics configuration. None if metrics collection is disabled.
    pub config: Option<crate::metric_service::Config>,
}

impl ConfigProvider for MetricsConfigProvider {
    fn name(&self) -> &'static str {
        "metrics"
    }

    fn get_config_with_descriptions(&self) -> ConfigWithDescriptions {
        let values = if let Some(ref config) = self.config {
            json!({
                "enabled": config.enabled,
                "aggregation_window_secs": config.aggregation_window_secs,
                "max_cardinality": config.max_cardinality,
                "channel_buffer_size": config.channel_buffer_size,
                "enable_prometheus": config.enable_prometheus,
                "prometheus_port": config.prometheus_port,
                "enable_otel": config.enable_otel,
                "otel_endpoint": config.otel_endpoint.clone().unwrap_or_else(|| "None".to_string()),
                "enable_time_series": config.enable_time_series,
                "time_series_retention_secs": config.time_series_retention_secs,
                "max_points_per_metric": config.max_points_per_metric,
                "time_series_sample_interval_secs": config.time_series_sample_interval_secs,
            })
        } else {
            json!({
                "enabled": false,
            })
        };

        let mut descriptions = HashMap::new();
        descriptions.insert(
            "enabled".to_string(),
            "Enable metrics collection system-wide".to_string(),
        );
        descriptions.insert(
            "aggregation_window_secs".to_string(),
            "Time window for aggregating metrics before snapshot (seconds)".to_string(),
        );
        descriptions.insert(
            "max_cardinality".to_string(),
            "Maximum unique metric label combinations to prevent memory exhaustion".to_string(),
        );
        descriptions.insert(
            "channel_buffer_size".to_string(),
            "Buffer size for metric events channel".to_string(),
        );
        descriptions.insert(
            "enable_prometheus".to_string(),
            "Enable Prometheus exporter for external scraping".to_string(),
        );
        descriptions.insert(
            "prometheus_port".to_string(),
            "Port for Prometheus exporter HTTP server".to_string(),
        );
        descriptions.insert(
            "enable_otel".to_string(),
            "Enable OpenTelemetry exporter for metrics".to_string(),
        );
        descriptions.insert(
            "otel_endpoint".to_string(),
            "OpenTelemetry endpoint URL (e.g., http://localhost:4317)".to_string(),
        );
        descriptions.insert(
            "enable_time_series".to_string(),
            "Enable time-series storage for graphing and historical data".to_string(),
        );
        descriptions.insert(
            "time_series_retention_secs".to_string(),
            "Time-series data retention window (seconds)".to_string(),
        );
        descriptions.insert(
            "max_points_per_metric".to_string(),
            "Maximum data points per metric to limit memory usage".to_string(),
        );
        descriptions.insert(
            "time_series_sample_interval_secs".to_string(),
            "Sample interval for downsampling high-frequency metrics (seconds)".to_string(),
        );

        ConfigWithDescriptions::new(values, descriptions)
    }
}

/// Configuration provider for Metadata component.
pub struct MetadataConfigProvider {
    /// Metadata store configuration. Always required as metadata is essential.
    pub config: crate::metadata_store::Config,
}

impl ConfigProvider for MetadataConfigProvider {
    fn name(&self) -> &'static str {
        "metadata"
    }

    fn get_config_with_descriptions(&self) -> ConfigWithDescriptions {
        let values = json!({
            "database_path": self.config.database_path.display().to_string(),
            "read_pool_size": self.config.read_pool_size,
            "enable_wal": self.config.enable_wal,
            "cache_size_mb": self.config.cache_size_mb,
            "enable_foreign_keys": self.config.enable_foreign_keys,
            "synchronous": format!("{:?}", self.config.synchronous),
            "transaction_isolation": format!("{:?}", self.config.transaction_isolation),
            "enable_prepared_statements": self.config.enable_prepared_statements,
            "read_pool_timeout_secs": self.config.read_pool_timeout_secs,
            "stripe_cache_size_mb": self.config.stripe_cache_size_mb,
            "stripe_cache_ttl_secs": self.config.stripe_cache_ttl_secs,
            "stripe_cache_tti_secs": self.config.stripe_cache_tti_secs,
            "chunk_cache_size_mb": self.config.chunk_cache_size_mb,
            "chunk_cache_ttl_secs": self.config.chunk_cache_ttl_secs,
            "chunk_cache_tti_secs": self.config.chunk_cache_tti_secs,
        });

        let mut descriptions = HashMap::new();
        descriptions.insert(
            "database_path".to_string(),
            "Path to SQLite database file storing filesystem metadata".to_string(),
        );
        descriptions.insert(
            "read_pool_size".to_string(),
            "Number of read connections in pool (4-8 recommended for concurrent access)"
                .to_string(),
        );
        descriptions.insert(
            "enable_wal".to_string(),
            "Enable Write-Ahead Logging mode for better concurrent read/write performance"
                .to_string(),
        );
        descriptions.insert(
            "cache_size_mb".to_string(),
            "SQLite page cache size in MB (max: 2047MB to prevent overflow)".to_string(),
        );
        descriptions.insert(
            "enable_foreign_keys".to_string(),
            "Enable foreign key constraints for referential integrity".to_string(),
        );
        descriptions.insert(
            "synchronous".to_string(),
            "Synchronous mode: Off (fastest), Normal (balanced), Full (safest)".to_string(),
        );
        descriptions.insert(
            "transaction_isolation".to_string(),
            "Transaction isolation level: ReadCommitted or Serializable".to_string(),
        );
        descriptions.insert(
            "enable_prepared_statements".to_string(),
            "Enable prepared statement caching for common queries".to_string(),
        );
        descriptions.insert(
            "read_pool_timeout_secs".to_string(),
            "Timeout for acquiring read connection from pool (seconds)".to_string(),
        );
        descriptions.insert(
            "stripe_cache_size_mb".to_string(),
            "In-memory cache size for stripe metadata in MB (reduces SQLite queries)".to_string(),
        );
        descriptions.insert(
            "stripe_cache_ttl_secs".to_string(),
            "Stripe metadata cache time-to-live (evict after this time)".to_string(),
        );
        descriptions.insert(
            "stripe_cache_tti_secs".to_string(),
            "Stripe metadata cache time-to-idle (evict if not accessed)".to_string(),
        );
        descriptions.insert(
            "chunk_cache_size_mb".to_string(),
            "In-memory cache size for chunk lists in MB (reduces SQLite queries)".to_string(),
        );
        descriptions.insert(
            "chunk_cache_ttl_secs".to_string(),
            "Chunk list cache time-to-live (evict after this time)".to_string(),
        );
        descriptions.insert(
            "chunk_cache_tti_secs".to_string(),
            "Chunk list cache time-to-idle (evict if not accessed)".to_string(),
        );

        ConfigWithDescriptions::new(values, descriptions)
    }
}

/// Configuration provider for FileStore component.
pub struct FileStoreConfigProvider {
    /// File store configuration for chunk storage and erasure coding.
    pub config: crate::file_store::types::Config,
}

impl ConfigProvider for FileStoreConfigProvider {
    fn name(&self) -> &'static str {
        "filestore"
    }

    fn get_config_with_descriptions(&self) -> ConfigWithDescriptions {
        let values = json!({
            "disk_paths": self.config.disk_paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
            "max_chunk_size": self.config.max_chunk_size,
            "default_data_shards": self.config.default_data_shards,
            "default_parity_shards": self.config.default_parity_shards,
            "max_concurrent_operations": self.config.max_concurrent_operations,
            "verification_interval": self.config.verification_interval.as_secs(),
            "orphan_cleanup_age": self.config.orphan_cleanup_age.as_secs(),
            "stripe_cache_size_mb": self.config.stripe_cache_size_mb,
            "stripe_cache_ttl_secs": self.config.stripe_cache_ttl_secs,
            "stripe_cache_tti_secs": self.config.stripe_cache_tti_secs,
        });

        let mut descriptions = HashMap::new();
        descriptions.insert(
            "disk_paths".to_string(),
            "Paths to disk mount points for chunk storage across multiple devices".to_string(),
        );
        descriptions.insert(
            "max_chunk_size".to_string(),
            "Maximum chunk size in bytes after erasure encoding (affects I/O granularity)"
                .to_string(),
        );
        descriptions.insert(
            "default_data_shards".to_string(),
            "Number of data shards for erasure coding (higher = more storage efficiency)"
                .to_string(),
        );
        descriptions.insert(
            "default_parity_shards".to_string(),
            "Number of parity shards for erasure coding (higher = more fault tolerance)"
                .to_string(),
        );
        descriptions.insert(
            "max_concurrent_operations".to_string(),
            "Maximum concurrent chunk read/write operations".to_string(),
        );
        descriptions.insert(
            "verification_interval".to_string(),
            "How often to verify chunk integrity (seconds)".to_string(),
        );
        descriptions.insert(
            "orphan_cleanup_age".to_string(),
            "Age threshold for cleaning up orphaned chunks (seconds)".to_string(),
        );
        descriptions.insert(
            "stripe_cache_size_mb".to_string(),
            "Maximum stripe cache size in megabytes".to_string(),
        );
        descriptions.insert(
            "stripe_cache_ttl_secs".to_string(),
            "Stripe cache time-to-live (evict after this time regardless of usage)".to_string(),
        );
        descriptions.insert(
            "stripe_cache_tti_secs".to_string(),
            "Stripe cache time-to-idle (evict if not accessed for this time)".to_string(),
        );

        ConfigWithDescriptions::new(values, descriptions)
    }
}

/// Configuration provider for Filesystem component.
pub struct FilesystemConfigProvider {
    /// Filesystem service configuration for FUSE operations and file handling.
    pub config: crate::filesystem_service::types::Config,
}

impl ConfigProvider for FilesystemConfigProvider {
    fn name(&self) -> &'static str {
        "filesystem"
    }

    fn get_config_with_descriptions(&self) -> ConfigWithDescriptions {
        let values = json!({
            "node_id": self.config.node_id,
            "client_heartbeat_timeout": self.config.client_heartbeat_timeout.as_secs(),
            "enable_read_locks": self.config.enable_read_locks,
            "lock_timeout": self.config.lock_timeout.as_secs(),
            "lock_extend_interval": self.config.lock_extend_interval.as_secs(),
            "max_file_handles": self.config.max_file_handles,
            "inode_cache_size": self.config.inode_cache_size,
            "inode_cache_ttl": self.config.inode_cache_ttl.as_secs(),
            "read_buffer_size": self.config.read_buffer_size,
            "write_buffer_size": self.config.write_buffer_size,
            "write_through": self.config.write_through,
            "default_file_mode": format!("{:o}", self.config.default_file_mode),
            "default_dir_mode": format!("{:o}", self.config.default_dir_mode),
            "max_file_size": self.config.max_file_size,
            "enable_xattr": self.config.enable_xattr,
            "uid": self.config.uid,
            "gid": self.config.gid,
        });

        let mut descriptions = HashMap::new();
        descriptions.insert(
            "node_id".to_string(),
            "Unique identifier for this storage node in distributed system".to_string(),
        );
        descriptions.insert(
            "client_heartbeat_timeout".to_string(),
            "Client heartbeat timeout before considered dead (seconds)".to_string(),
        );
        descriptions.insert(
            "enable_read_locks".to_string(),
            "Enable read lock enforcement for concurrent access control".to_string(),
        );
        descriptions.insert(
            "lock_timeout".to_string(),
            "Lock timeout duration for file operations (seconds)".to_string(),
        );
        descriptions.insert(
            "lock_extend_interval".to_string(),
            "Interval for extending locks during long operations (seconds)".to_string(),
        );
        descriptions.insert(
            "max_file_handles".to_string(),
            "Maximum file handles per client to prevent resource exhaustion".to_string(),
        );
        descriptions.insert(
            "inode_cache_size".to_string(),
            "Number of inode entries to cache in memory".to_string(),
        );
        descriptions.insert(
            "inode_cache_ttl".to_string(),
            "Time-to-live for cached inode entries (seconds)".to_string(),
        );
        descriptions.insert(
            "read_buffer_size".to_string(),
            "Read buffer size for stripe assembly (bytes)".to_string(),
        );
        descriptions.insert(
            "write_buffer_size".to_string(),
            "Write buffer size per file handle (bytes)".to_string(),
        );
        descriptions.insert(
            "write_through".to_string(),
            "Enable write-through mode (no buffering, immediate disk writes)".to_string(),
        );
        descriptions.insert(
            "default_file_mode".to_string(),
            "Default permissions for new files (octal)".to_string(),
        );
        descriptions.insert(
            "default_dir_mode".to_string(),
            "Default permissions for new directories (octal)".to_string(),
        );
        descriptions.insert(
            "max_file_size".to_string(),
            "Maximum allowed file size in bytes".to_string(),
        );
        descriptions.insert(
            "enable_xattr".to_string(),
            "Enable extended attributes support".to_string(),
        );
        descriptions.insert(
            "uid".to_string(),
            "Default user ID for filesystem operations".to_string(),
        );
        descriptions.insert(
            "gid".to_string(),
            "Default group ID for filesystem operations".to_string(),
        );

        ConfigWithDescriptions::new(values, descriptions)
    }
}

/// Configuration provider for BufferedFileHandle component.
pub struct BufferedFileHandleConfigProvider {
    /// Configuration for file handle buffering and write caching behavior.
    pub config: crate::filesystem_service::types::BufferedFileHandleConfig,
}

impl ConfigProvider for BufferedFileHandleConfigProvider {
    fn name(&self) -> &'static str {
        "buffered_file_handle"
    }

    fn get_config_with_descriptions(&self) -> ConfigWithDescriptions {
        let values = json!({
            "max_memory_bytes": self.config.max_memory_bytes,
            "max_flush_interval_secs": self.config.max_flush_interval.as_secs(),
            "max_writes_before_flush": self.config.max_writes_before_flush,
            "max_stripe_size": self.config.max_stripe_size,
        });

        let mut descriptions = HashMap::new();
        descriptions.insert(
            "max_memory_bytes".to_string(),
            "Maximum memory per file handle before triggering partial flush (bytes)".to_string(),
        );
        descriptions.insert(
            "max_flush_interval_secs".to_string(),
            "Maximum time between full flushes to disk (seconds)".to_string(),
        );
        descriptions.insert(
            "max_writes_before_flush".to_string(),
            "Maximum write operations before forcing full flush".to_string(),
        );
        descriptions.insert(
            "max_stripe_size".to_string(),
            "Maximum stripe size for buffered writes (bytes, from FileStore config)".to_string(),
        );

        ConfigWithDescriptions::new(values, descriptions)
    }
}

/// Configuration provider for Mount options.
pub struct MountConfigProvider {
    /// Path where the WormFS filesystem is mounted.
    pub mount_point: std::path::PathBuf,
    /// FUSE mount options controlling permissions and behavior.
    pub options: crate::filesystem_service::mount::MountOptions,
}

impl ConfigProvider for MountConfigProvider {
    fn name(&self) -> &'static str {
        "mount"
    }

    fn get_config_with_descriptions(&self) -> ConfigWithDescriptions {
        let values = json!({
            "mount_point": self.mount_point.display().to_string(),
            "allow_root": self.options.allow_root,
            "allow_other": self.options.allow_other,
            "foreground": self.options.foreground,
            "fsname": self.options.fsname,
            "auto_unmount": self.options.auto_unmount,
        });

        let mut descriptions = HashMap::new();
        descriptions.insert(
            "mount_point".to_string(),
            "Directory path where WormFS is mounted".to_string(),
        );
        descriptions.insert(
            "allow_root".to_string(),
            "Allow root user to access the mounted filesystem".to_string(),
        );
        descriptions.insert(
            "allow_other".to_string(),
            "Allow other users to access the mounted filesystem".to_string(),
        );
        descriptions.insert(
            "foreground".to_string(),
            "Run FUSE in foreground (don't daemonize process)".to_string(),
        );
        descriptions.insert(
            "fsname".to_string(),
            "Filesystem name shown in df and mount output".to_string(),
        );
        descriptions.insert(
            "auto_unmount".to_string(),
            "Automatically unmount on process exit (requires user_allow_other in fuse.conf)"
                .to_string(),
        );

        ConfigWithDescriptions::new(values, descriptions)
    }
}
