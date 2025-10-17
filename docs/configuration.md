# WormFS Configuration Reference

## Overview

WormFS uses TOML format for configuration files. This document provides a comprehensive reference for all available configuration options in Phase 1.

## Table of Contents

1. [File Format](#file-format)
2. [Global Settings](#global-settings)
3. [Metadata Store Configuration](#metadata-store-configuration)
4. [File Store Configuration](#file-store-configuration)
5. [Filesystem Configuration](#filesystem-configuration)
6. [Configuration Examples](#configuration-examples)
7. [Environment-Specific Configurations](#environment-specific-configurations)

## File Format

Configuration files use the [TOML](https://toml.io/) format, organized into sections:

```toml
# Global settings (outside any section)
mount_point = "/path/to/mount"

[metadata]
# Metadata store settings

[file_store]
# File store settings

[filesystem]
# Filesystem behavior settings
```

## Global Settings

### mount_point

**Type**: String
**Required**: Yes
**Description**: Directory where WormFS will be mounted
**Example**:
```toml
mount_point = "/mnt/wormfs"
```

**Notes**:
- Must be an absolute path
- Directory must exist before mounting
- Must be empty
- User must have write permissions

## Metadata Store Configuration

The `[metadata]` section configures the SQLite metadata database.

### database_path

**Type**: String
**Required**: Yes
**Default**: None
**Description**: Path to the SQLite database file
**Example**:
```toml
[metadata]
database_path = "/var/lib/wormfs/metadata.db"
```

**Notes**:
- Parent directory must exist
- File will be created if it doesn't exist
- Stores file metadata, directory structure, and inode information

### read_pool_size

**Type**: Integer
**Required**: No
**Default**: `4`
**Range**: 1-100
**Description**: Number of read-only database connections in the pool
**Example**:
```toml
[metadata]
read_pool_size = 8
```

**Tuning**:
- Increase for read-heavy workloads
- Each connection consumes memory
- Recommended: 2× number of CPU cores

### enable_wal

**Type**: Boolean
**Required**: No
**Default**: `true`
**Description**: Enable SQLite Write-Ahead Logging (WAL) mode
**Example**:
```toml
[metadata]
enable_wal = true
```

**Notes**:
- WAL mode improves concurrency
- Better write performance
- Recommended to keep enabled

### cache_size_mb

**Type**: Integer
**Required**: No
**Default**: `100`
**Range**: 1-10000
**Description**: SQLite page cache size in megabytes
**Example**:
```toml
[metadata]
cache_size_mb = 500
```

**Tuning**:
- Larger cache = better performance for frequently accessed metadata
- Consumes RAM
- Recommended: 5-10% of available RAM

### enable_foreign_keys

**Type**: Boolean
**Required**: No
**Default**: `true`
**Description**: Enable SQLite foreign key constraints
**Example**:
```toml
[metadata]
enable_foreign_keys = true
```

**Notes**:
- Ensures referential integrity
- Recommended to keep enabled
- Required for Phase 1

### synchronous

**Type**: String
**Required**: No
**Default**: `"Normal"`
**Valid Values**: `"Off"`, `"Normal"`, `"Full"`
**Description**: SQLite synchronous mode
**Example**:
```toml
[metadata]
synchronous = "Normal"
```

**Options**:
- `"Off"`: Fastest, risk of database corruption on crash
- `"Normal"`: Good balance of speed and safety (recommended)
- `"Full"`: Slowest, maximum durability

### transaction_isolation

**Type**: String
**Required**: No
**Default**: `"Serializable"`
**Valid Values**: `"Serializable"`, `"ReadCommitted"`
**Description**: Transaction isolation level
**Example**:
```toml
[metadata]
transaction_isolation = "Serializable"
```

**Notes**:
- `"Serializable"`: Strictest isolation (recommended)
- `"ReadCommitted"`: Better performance, less isolation

### enable_prepared_statements

**Type**: Boolean
**Required**: No
**Default**: `true`
**Description**: Enable prepared statement caching
**Example**:
```toml
[metadata]
enable_prepared_statements = true
```

**Notes**:
- Improves performance
- Recommended to keep enabled

### read_pool_timeout_secs

**Type**: Integer
**Required**: No
**Default**: `30`
**Range**: 1-300
**Description**: Timeout in seconds for acquiring a database connection
**Example**:
```toml
[metadata]
read_pool_timeout_secs = 60
```

## File Store Configuration

The `[file_store]` section configures erasure coding and chunk storage.

### disk_paths

**Type**: Array of Strings
**Required**: Yes
**Default**: None
**Description**: List of directories where chunks will be stored
**Example**:
```toml
[file_store]
disk_paths = ["/var/lib/wormfs/chunks1", "/var/lib/wormfs/chunks2"]
```

**Notes**:
- All directories must exist
- Can span multiple disks/filesystems
- Phase 1: Only first path is used

### max_chunk_size

**Type**: Integer
**Required**: No
**Default**: `1048576` (1 MB)
**Range**: 4096-16777216 (4 KB - 16 MB)
**Description**: Maximum size of individual data chunks in bytes
**Example**:
```toml
[file_store]
max_chunk_size = 2097152  # 2 MB
```

**Tuning**:
- Smaller chunks: Better for small files, more metadata overhead
- Larger chunks: Better for large files, less metadata overhead
- Recommended: 256 KB - 4 MB depending on typical file sizes

### default_data_shards

**Type**: Integer
**Required**: No
**Default**: `2`
**Range**: 1-16
**Description**: Number of data shards in erasure coding
**Example**:
```toml
[file_store]
default_data_shards = 4
```

**Notes**:
- More shards = better parallelism
- Must have: `data_shards + parity_shards >= 2`

### default_parity_shards

**Type**: Integer
**Required**: No
**Default**: `1`
**Range**: 1-16
**Description**: Number of parity (redundancy) shards in erasure coding
**Example**:
```toml
[file_store]
default_parity_shards = 2
```

**Storage Overhead**: `parity_shards / data_shards`
**Examples**:
- 2 data + 1 parity = 50% overhead, can lose 1 shard
- 4 data + 2 parity = 50% overhead, can lose 2 shards
- 8 data + 3 parity = 37.5% overhead, can lose 3 shards

### max_concurrent_operations

**Type**: Integer
**Required**: No
**Default**: `100`
**Range**: 1-10000
**Description**: Maximum number of concurrent file operations
**Example**:
```toml
[file_store]
max_concurrent_operations = 500
```

**Tuning**:
- Higher values: Better throughput under load
- Consumes more memory and file descriptors
- Recommended: 10× number of CPU cores

### verification_interval

**Type**: Integer
**Required**: No
**Default**: `3600` (1 hour)
**Range**: 60-86400 (1 minute - 1 day)
**Description**: Interval in seconds between chunk verification scans
**Example**:
```toml
[file_store]
verification_interval = 7200  # 2 hours
```

**Notes**:
- Phase 1: Not fully implemented
- Future: Periodic integrity checks

### orphan_cleanup_age

**Type**: Integer
**Required**: No
**Default**: `3600` (1 hour)
**Range**: 60-86400
**Description**: Age in seconds before orphaned chunks are cleaned up
**Example**:
```toml
[file_store]
orphan_cleanup_age = 86400  # 1 day
```

**Notes**:
- Phase 1: Not fully implemented
- Prevents premature deletion of chunks

## Filesystem Configuration

The `[filesystem]` section configures filesystem behavior.

### node_id

**Type**: Integer
**Required**: Yes
**Default**: None
**Range**: 0-65535
**Description**: Unique identifier for this filesystem node
**Example**:
```toml
[filesystem]
node_id = 1
```

**Notes**:
- Phase 1: Can be any value
- Future (multi-node): Must be unique across cluster

### client_heartbeat_timeout

**Type**: Integer
**Required**: No
**Default**: `86400` (24 hours)
**Range**: 60-2592000
**Description**: Client inactivity timeout in seconds
**Example**:
```toml
[filesystem]
client_heartbeat_timeout = 3600
```

### enable_read_locks

**Type**: Boolean
**Required**: No
**Default**: `true`
**Description**: Enable read lock support
**Example**:
```toml
[filesystem]
enable_read_locks = true
```

### lock_timeout

**Type**: Integer
**Required**: No
**Default**: `10`
**Range**: 1-300
**Description**: Lock acquisition timeout in seconds
**Example**:
```toml
[filesystem]
lock_timeout = 30
```

### lock_extend_interval

**Type**: Integer
**Required**: No
**Default**: `5`
**Range**: 1-60
**Description**: Interval in seconds for automatic lock extension
**Example**:
```toml
[filesystem]
lock_extend_interval = 10
```

### max_file_handles

**Type**: Integer
**Required**: No
**Default**: `10000`
**Range**: 100-1000000
**Description**: Maximum number of open file handles
**Example**:
```toml
[filesystem]
max_file_handles = 50000
```

**Notes**:
- Must be less than system `ulimit -n`
- Each open file consumes memory

### inode_cache_size

**Type**: Integer
**Required**: No
**Default**: `10000`
**Range**: 100-1000000
**Description**: Maximum number of cached inode entries
**Example**:
```toml
[filesystem]
inode_cache_size = 100000
```

**Tuning**:
- Larger cache = fewer database lookups
- Each entry consumes ~1 KB of memory

### inode_cache_ttl

**Type**: Integer
**Required**: No
**Default**: `60` (1 minute)
**Range**: 1-3600
**Description**: Time-to-live for cached inodes in seconds
**Example**:
```toml
[filesystem]
inode_cache_ttl = 300  # 5 minutes
```

### read_buffer_size

**Type**: Integer
**Required**: No
**Default**: `1048576` (1 MB)
**Range**: 4096-16777216
**Description**: Read buffer size in bytes
**Example**:
```toml
[filesystem]
read_buffer_size = 4194304  # 4 MB
```

**Tuning**:
- Larger buffers: Better performance for large sequential reads
- Consumes more memory per operation

### write_buffer_size

**Type**: Integer
**Required**: No
**Default**: `1048576` (1 MB)
**Range**: 4096-16777216
**Description**: Write buffer size in bytes
**Example**:
```toml
[filesystem]
write_buffer_size = 4194304  # 4 MB
```

### write_through

**Type**: Boolean
**Required**: No
**Default**: `false`
**Description**: Enable write-through caching
**Example**:
```toml
[filesystem]
write_through = true
```

**Options**:
- `false`: Better performance, data cached
- `true`: Slower writes, immediate durability

### default_file_mode

**Type**: String or Integer
**Required**: No
**Default**: `"0644"` or `420`
**Description**: Default permissions for new files
**Example**:
```toml
[filesystem]
default_file_mode = "0644"  # Octal string (recommended)
# or
default_file_mode = 420     # Decimal integer
```

**Common Values**:
- `"0644"` (420): Owner read/write, others read
- `"0600"` (384): Owner read/write only
- `"0666"` (438): Everyone read/write

### default_dir_mode

**Type**: String or Integer
**Required**: No
**Default**: `"0755"` or `493`
**Description**: Default permissions for new directories
**Example**:
```toml
[filesystem]
default_dir_mode = "0755"  # Octal string (recommended)
# or
default_dir_mode = 493     # Decimal integer
```

**Common Values**:
- `"0755"` (493): Owner all, others read/execute
- `"0700"` (448): Owner only
- `"0775"` (509): Owner/group all, others read/execute

### max_file_size

**Type**: Integer
**Required**: No
**Default**: `1099511627776` (1 TB)
**Range**: 1048576-unlimited
**Description**: Maximum file size in bytes
**Example**:
```toml
[filesystem]
max_file_size = 10737418240  # 10 GB
```

### enable_xattr

**Type**: Boolean
**Required**: No
**Default**: `false`
**Description**: Enable extended attributes (xattr) support
**Example**:
```toml
[filesystem]
enable_xattr = false
```

**Notes**:
- Phase 1: Not fully implemented
- Future: Full xattr support

### uid

**Type**: Integer
**Required**: Yes
**Default**: None
**Range**: 0-65535
**Description**: User ID for filesystem ownership
**Example**:
```toml
[filesystem]
uid = 1000
```

**Notes**:
- Use your user ID: `id -u`
- Files will be owned by this UID

### gid

**Type**: Integer
**Required**: Yes
**Default**: None
**Range**: 0-65535
**Description**: Group ID for filesystem ownership
**Example**:
```toml
[filesystem]
gid = 1000
```

**Notes**:
- Use your group ID: `id -g`
- Files will be owned by this GID

## Configuration Examples

### Minimal Configuration

```toml
mount_point = "/mnt/wormfs"

[metadata]
database_path = "/var/lib/wormfs/metadata.db"
enable_foreign_keys = true

[file_store]
disk_paths = ["/var/lib/wormfs/chunks"]

[filesystem]
node_id = 1
uid = 1000
gid = 1000
```

### Development/Testing Configuration

```toml
mount_point = "/tmp/wormfs-test"

[metadata]
database_path = "/tmp/wormfs-data/metadata.db"
cache_size_mb = 10
enable_wal = true
synchronous = "Normal"
enable_foreign_keys = true
transaction_isolation = "Serializable"
enable_prepared_statements = true

[file_store]
disk_paths = ["/tmp/wormfs-data/chunks"]
max_chunk_size = 262144  # 256 KB for testing
default_data_shards = 2
default_parity_shards = 1
max_concurrent_operations = 50

[filesystem]
node_id = 1
enable_read_locks = true
max_file_handles = 1000
inode_cache_size = 1000
inode_cache_ttl = 30
read_buffer_size = 524288   # 512 KB
write_buffer_size = 524288  # 512 KB
write_through = false
default_file_mode = "0644"
default_dir_mode = "0755"
max_file_size = 10737418240  # 10 GB
enable_xattr = false
uid = 1000
gid = 1000
```

### Production Configuration

```toml
mount_point = "/mnt/wormfs"

[metadata]
database_path = "/var/lib/wormfs/metadata.db"
read_pool_size = 16
cache_size_mb = 1000
enable_wal = true
synchronous = "Normal"
enable_foreign_keys = true
transaction_isolation = "Serializable"
enable_prepared_statements = true
read_pool_timeout_secs = 60

[file_store]
disk_paths = [
    "/mnt/disk1/wormfs/chunks",
    "/mnt/disk2/wormfs/chunks",
    "/mnt/disk3/wormfs/chunks"
]
max_chunk_size = 1048576  # 1 MB
default_data_shards = 4
default_parity_shards = 2
max_concurrent_operations = 1000
verification_interval = 3600
orphan_cleanup_age = 86400

[filesystem]
node_id = 1
client_heartbeat_timeout = 86400
enable_read_locks = true
lock_timeout = 30
lock_extend_interval = 10
max_file_handles = 100000
inode_cache_size = 100000
inode_cache_ttl = 300
read_buffer_size = 4194304   # 4 MB
write_buffer_size = 4194304  # 4 MB
write_through = false
default_file_mode = "0644"
default_dir_mode = "0755"
max_file_size = 1099511627776  # 1 TB
enable_xattr = false
uid = 1000
gid = 1000
```

### High-Performance Configuration

```toml
mount_point = "/mnt/wormfs-fast"

[metadata]
database_path = "/nvme/wormfs/metadata.db"
read_pool_size = 32
cache_size_mb = 2000
enable_wal = true
synchronous = "Normal"
enable_foreign_keys = true
transaction_isolation = "ReadCommitted"  # Less strict for performance
enable_prepared_statements = true
read_pool_timeout_secs = 120

[file_store]
disk_paths = [
    "/nvme1/wormfs/chunks",
    "/nvme2/wormfs/chunks"
]
max_chunk_size = 4194304  # 4 MB chunks
default_data_shards = 8
default_parity_shards = 3
max_concurrent_operations = 5000
verification_interval = 7200
orphan_cleanup_age = 86400

[filesystem]
node_id = 1
client_heartbeat_timeout = 86400
enable_read_locks = true
lock_timeout = 60
lock_extend_interval = 20
max_file_handles = 500000
inode_cache_size = 500000
inode_cache_ttl = 600  # 10 minutes
read_buffer_size = 16777216   # 16 MB
write_buffer_size = 16777216  # 16 MB
write_through = false
default_file_mode = "0644"
default_dir_mode = "0755"
max_file_size = 5497558138880  # 5 TB
enable_xattr = false
uid = 1000
gid = 1000
```

## Environment-Specific Configurations

### Small Files Workload

For many small files (< 1 MB):
- Smaller `max_chunk_size` (256 KB)
- Larger `inode_cache_size`
- Moderate buffer sizes

### Large Files Workload

For large files (> 100 MB):
- Larger `max_chunk_size` (2-4 MB)
- Larger buffer sizes (4-16 MB)
- More `data_shards` for parallelism

### Write-Heavy Workload

- `write_through = false`
- `synchronous = "Normal"` (not "Full")
- Larger `write_buffer_size`
- More `max_concurrent_operations`

### Read-Heavy Workload

- Larger `cache_size_mb`
- More `read_pool_size`
- Larger `inode_cache_size`
- Longer `inode_cache_ttl`

## See Also

- [User Guide](user_guide_phase1.md) - Getting started with WormFS
- [Troubleshooting](troubleshooting.md) - Common configuration issues
- [Design Document](design.md) - Architecture and internals
