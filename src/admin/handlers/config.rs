//! Configuration handler for admin API endpoints.
//!
//! Provides handlers for configuration viewing and management.

use crate::filesystem_service::mount::MountConfig;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use std::sync::Arc;

/// Handler for `/api/config` endpoint.
///
/// Returns the current system configuration in JSON format with descriptions for each field.
pub async fn config_handler(State(mount_config): State<Arc<MountConfig>>) -> impl IntoResponse {
    let config = serde_json::json!({
        "admin": {
            "values": {
                "enabled": mount_config.admin_config.as_ref().map(|c| c.enabled).unwrap_or(false),
                "port": mount_config.admin_config.as_ref().map(|c| c.port).unwrap_or(9090),
                "bind_address": mount_config.admin_config.as_ref().map(|c| c.bind_address.clone()).unwrap_or_else(|| "127.0.0.1".to_string()),
            },
            "descriptions": {
                "enabled": "Enable the admin HTTP server for monitoring and management",
                "port": "Port to bind the admin server (default: 9090)",
                "bind_address": "Bind address for admin server (127.0.0.1 for localhost only)",
            }
        },
        "metrics": {
            "values": {
                "enabled": mount_config.metric_config.as_ref().map(|c| c.enabled).unwrap_or(false),
                "aggregation_window_secs": mount_config.metric_config.as_ref().map(|c| c.aggregation_window_secs).unwrap_or(60),
                "max_cardinality": mount_config.metric_config.as_ref().map(|c| c.max_cardinality).unwrap_or(10000),
                "channel_buffer_size": mount_config.metric_config.as_ref().map(|c| c.channel_buffer_size).unwrap_or(10000),
                "enable_prometheus": mount_config.metric_config.as_ref().map(|c| c.enable_prometheus).unwrap_or(false),
                "prometheus_port": mount_config.metric_config.as_ref().map(|c| c.prometheus_port).unwrap_or(9090),
                "enable_otel": mount_config.metric_config.as_ref().map(|c| c.enable_otel).unwrap_or(false),
                "otel_endpoint": mount_config.metric_config.as_ref().and_then(|c| c.otel_endpoint.clone()).unwrap_or_else(|| "None".to_string()),
                "enable_time_series": mount_config.metric_config.as_ref().map(|c| c.enable_time_series).unwrap_or(true),
                "time_series_retention_secs": mount_config.metric_config.as_ref().map(|c| c.time_series_retention_secs).unwrap_or(3600),
                "max_points_per_metric": mount_config.metric_config.as_ref().map(|c| c.max_points_per_metric).unwrap_or(1000),
                "time_series_sample_interval_secs": mount_config.metric_config.as_ref().map(|c| c.time_series_sample_interval_secs).unwrap_or(1),
            },
            "descriptions": {
                "enabled": "Enable metrics collection system-wide",
                "aggregation_window_secs": "Time window for aggregating metrics before snapshot (seconds)",
                "max_cardinality": "Maximum unique metric label combinations to prevent memory exhaustion",
                "channel_buffer_size": "Buffer size for metric events channel",
                "enable_prometheus": "Enable Prometheus exporter for external scraping",
                "prometheus_port": "Port for Prometheus exporter HTTP server",
                "enable_otel": "Enable OpenTelemetry exporter for metrics",
                "otel_endpoint": "OpenTelemetry endpoint URL (e.g., http://localhost:4317)",
                "enable_time_series": "Enable time-series storage for graphing and historical data",
                "time_series_retention_secs": "Time-series data retention window (seconds)",
                "max_points_per_metric": "Maximum data points per metric to limit memory usage",
                "time_series_sample_interval_secs": "Sample interval for downsampling high-frequency metrics (seconds)",
            }
        },
        "metadata": {
            "values": {
                "database_path": mount_config.metadata_config.database_path.display().to_string(),
                "read_pool_size": mount_config.metadata_config.read_pool_size,
                "enable_wal": mount_config.metadata_config.enable_wal,
                "cache_size_mb": mount_config.metadata_config.cache_size_mb,
                "enable_foreign_keys": mount_config.metadata_config.enable_foreign_keys,
                "synchronous": format!("{:?}", mount_config.metadata_config.synchronous),
                "transaction_isolation": format!("{:?}", mount_config.metadata_config.transaction_isolation),
                "enable_prepared_statements": mount_config.metadata_config.enable_prepared_statements,
                "read_pool_timeout_secs": mount_config.metadata_config.read_pool_timeout_secs,
            },
            "descriptions": {
                "database_path": "Path to SQLite database file storing filesystem metadata",
                "read_pool_size": "Number of read connections in pool (4-8 recommended for concurrent access)",
                "enable_wal": "Enable Write-Ahead Logging mode for better concurrent read/write performance",
                "cache_size_mb": "SQLite page cache size in MB (max: 2047MB to prevent overflow)",
                "enable_foreign_keys": "Enable foreign key constraints for referential integrity",
                "synchronous": "Synchronous mode: Off (fastest), Normal (balanced), Full (safest)",
                "transaction_isolation": "Transaction isolation level: ReadCommitted or Serializable",
                "enable_prepared_statements": "Enable prepared statement caching for common queries",
                "read_pool_timeout_secs": "Timeout for acquiring read connection from pool (seconds)",
            }
        },
        "filestore": {
            "values": {
                "disk_paths": mount_config.file_store_config.disk_paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
                "max_chunk_size": mount_config.file_store_config.max_chunk_size,
                "default_data_shards": mount_config.file_store_config.default_data_shards,
                "default_parity_shards": mount_config.file_store_config.default_parity_shards,
                "max_concurrent_operations": mount_config.file_store_config.max_concurrent_operations,
                "verification_interval": mount_config.file_store_config.verification_interval.as_secs(),
                "orphan_cleanup_age": mount_config.file_store_config.orphan_cleanup_age.as_secs(),
                "stripe_cache_size_mb": mount_config.file_store_config.stripe_cache_size_mb,
                "stripe_cache_ttl_secs": mount_config.file_store_config.stripe_cache_ttl_secs,
                "stripe_cache_tti_secs": mount_config.file_store_config.stripe_cache_tti_secs,
            },
            "descriptions": {
                "disk_paths": "Paths to disk mount points for chunk storage across multiple devices",
                "max_chunk_size": "Maximum chunk size in bytes after erasure encoding (affects I/O granularity)",
                "default_data_shards": "Number of data shards for erasure coding (higher = more storage efficiency)",
                "default_parity_shards": "Number of parity shards for erasure coding (higher = more fault tolerance)",
                "max_concurrent_operations": "Maximum concurrent chunk read/write operations",
                "verification_interval": "How often to verify chunk integrity (seconds)",
                "orphan_cleanup_age": "Age threshold for cleaning up orphaned chunks (seconds)",
                "stripe_cache_size_mb": "Maximum stripe cache size in megabytes",
                "stripe_cache_ttl_secs": "Stripe cache time-to-live (evict after this time regardless of usage)",
                "stripe_cache_tti_secs": "Stripe cache time-to-idle (evict if not accessed for this time)",
            }
        },
        "filesystem": {
            "values": {
                "node_id": mount_config.filesystem_config.node_id,
                "client_heartbeat_timeout": mount_config.filesystem_config.client_heartbeat_timeout.as_secs(),
                "enable_read_locks": mount_config.filesystem_config.enable_read_locks,
                "lock_timeout": mount_config.filesystem_config.lock_timeout.as_secs(),
                "lock_extend_interval": mount_config.filesystem_config.lock_extend_interval.as_secs(),
                "max_file_handles": mount_config.filesystem_config.max_file_handles,
                "inode_cache_size": mount_config.filesystem_config.inode_cache_size,
                "inode_cache_ttl": mount_config.filesystem_config.inode_cache_ttl.as_secs(),
                "read_buffer_size": mount_config.filesystem_config.read_buffer_size,
                "write_buffer_size": mount_config.filesystem_config.write_buffer_size,
                "write_through": mount_config.filesystem_config.write_through,
                "default_file_mode": format!("{:o}", mount_config.filesystem_config.default_file_mode),
                "default_dir_mode": format!("{:o}", mount_config.filesystem_config.default_dir_mode),
                "max_file_size": mount_config.filesystem_config.max_file_size,
                "enable_xattr": mount_config.filesystem_config.enable_xattr,
                "uid": mount_config.filesystem_config.uid,
                "gid": mount_config.filesystem_config.gid,
            },
            "descriptions": {
                "node_id": "Unique identifier for this storage node in distributed system",
                "client_heartbeat_timeout": "Client heartbeat timeout before considered dead (seconds)",
                "enable_read_locks": "Enable read lock enforcement for concurrent access control",
                "lock_timeout": "Lock timeout duration for file operations (seconds)",
                "lock_extend_interval": "Interval for extending locks during long operations (seconds)",
                "max_file_handles": "Maximum file handles per client to prevent resource exhaustion",
                "inode_cache_size": "Number of inode entries to cache in memory",
                "inode_cache_ttl": "Time-to-live for cached inode entries (seconds)",
                "read_buffer_size": "Read buffer size for stripe assembly (bytes)",
                "write_buffer_size": "Write buffer size per file handle (bytes)",
                "write_through": "Enable write-through mode (no buffering, immediate disk writes)",
                "default_file_mode": "Default permissions for new files (octal)",
                "default_dir_mode": "Default permissions for new directories (octal)",
                "max_file_size": "Maximum allowed file size in bytes",
                "enable_xattr": "Enable extended attributes support",
                "uid": "Default user ID for filesystem operations",
                "gid": "Default group ID for filesystem operations",
            }
        },
        "buffered_file_handle": {
            "values": {
                "max_memory_bytes": mount_config.filesystem_config.buffered_file_handle_config.max_memory_bytes,
                "max_flush_interval_secs": mount_config.filesystem_config.buffered_file_handle_config.max_flush_interval.as_secs(),
                "max_writes_before_flush": mount_config.filesystem_config.buffered_file_handle_config.max_writes_before_flush,
                "max_stripe_size": mount_config.filesystem_config.buffered_file_handle_config.max_stripe_size,
            },
            "descriptions": {
                "max_memory_bytes": "Maximum memory per file handle before triggering partial flush (bytes)",
                "max_flush_interval_secs": "Maximum time between full flushes to disk (seconds)",
                "max_writes_before_flush": "Maximum write operations before forcing full flush",
                "max_stripe_size": "Maximum stripe size for buffered writes (bytes, from FileStore config)",
            }
        },
        "mount": {
            "values": {
                "mount_point": mount_config.mount_point.display().to_string(),
                "allow_root": mount_config.mount_options.allow_root,
                "allow_other": mount_config.mount_options.allow_other,
                "foreground": mount_config.mount_options.foreground,
                "fsname": mount_config.mount_options.fsname.clone(),
                "auto_unmount": mount_config.mount_options.auto_unmount,
            },
            "descriptions": {
                "mount_point": "Directory path where WormFS is mounted",
                "allow_root": "Allow root user to access the mounted filesystem",
                "allow_other": "Allow other users to access the mounted filesystem",
                "foreground": "Run FUSE in foreground (don't daemonize process)",
                "fsname": "Filesystem name shown in df and mount output",
                "auto_unmount": "Automatically unmount on process exit (requires user_allow_other in fuse.conf)",
            }
        }
    });

    (StatusCode::OK, Json(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;

    #[tokio::test]
    async fn test_config_handler() {
        // Create a test MountConfig
        let mount_config = Arc::new(MountConfig {
            filesystem_config: crate::filesystem_service::types::Config::default(),
            metadata_config: crate::metadata_store::Config::default(),
            file_store_config: crate::file_store::types::Config::default(),
            metric_config: Some(crate::metric_service::Config::default()),
            admin_config: Some(crate::admin::Config::default()),
            mount_point: std::path::PathBuf::from("/tmp/test"),
            mount_options: crate::filesystem_service::mount::MountOptions::default(),
        });

        let response = config_handler(State(mount_config)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
