# WormFS Phase 1 User Guide

## Overview

WormFS Phase 1 provides a functional single-node filesystem with erasure coding. This guide will help you get started with mounting, configuring, and using WormFS for basic file operations.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Building WormFS](#building-wormfs)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [Mounting the Filesystem](#mounting-the-filesystem)
6. [Basic Operations](#basic-operations)
7. [Understanding Erasure Coding](#understanding-erasure-coding)
8. [Performance Considerations](#performance-considerations)
9. [Unmounting](#unmounting)

## Prerequisites

- **Operating System**: Linux (FUSE support required)
- **Rust**: Version 1.70 or later
- **FUSE**: libfuse 2.9 or later
  ```bash
  # Ubuntu/Debian
  sudo apt-get install fuse libfuse-dev

  # Fedora/RHEL
  sudo dnf install fuse fuse-devel
  ```
- **User Permissions**: Your user must be in the `fuse` group
  ```bash
  sudo usermod -a -G fuse $USER
  # Log out and back in for group changes to take effect
  ```

## Building WormFS

### Build from Source

```bash
# Clone the repository
git clone https://github.com/your-org/wormfs.git
cd wormfs

# Build in release mode (recommended for performance)
cargo build --release --features fuser --bin wormfs

# The binary will be at: target/release/wormfs
```

### Verify the Build

```bash
./target/release/wormfs --version
./target/release/wormfs --help
```

## Quick Start

The fastest way to try WormFS is using the demo script:

```bash
./scripts/demo_wormfs.sh
```

This script will:
1. Build WormFS
2. Create test directories
3. Mount the filesystem
4. Perform basic operations
5. Run data integrity tests
6. Display results

## Configuration

### Configuration File Format

WormFS uses TOML for configuration. Here's a minimal configuration file:

```toml
# Mount point for the filesystem
mount_point = "/mnt/wormfs"

[metadata]
database_path = "/var/lib/wormfs/metadata.db"
read_pool_size = 4
enable_wal = true
cache_size_mb = 100
enable_foreign_keys = true
synchronous = "Normal"
transaction_isolation = "Serializable"
enable_prepared_statements = true
read_pool_timeout_secs = 30

[file_store]
disk_paths = ["/var/lib/wormfs/chunks"]
max_chunk_size = 1048576  # 1 MB
default_data_shards = 2
default_parity_shards = 1
max_concurrent_operations = 100
verification_interval = 3600
orphan_cleanup_age = 3600

[filesystem]
node_id = 1
client_heartbeat_timeout = 86400
enable_read_locks = true
lock_timeout = 10
lock_extend_interval = 5
max_file_handles = 10000
inode_cache_size = 10000
inode_cache_ttl = 60
read_buffer_size = 1048576
write_buffer_size = 1048576
write_through = false
default_file_mode = "0644"  # or 420 (decimal)
default_dir_mode = "0755"   # or 493 (decimal)
max_file_size = 1099511627776  # 1 TB
enable_xattr = false
uid = 1000
gid = 1000
```

### Key Configuration Options

See [configuration.md](configuration.md) for a complete reference of all configuration options.

### Creating a Configuration File

```bash
# Create data directories
mkdir -p /var/lib/wormfs/chunks
mkdir -p /mnt/wormfs

# Create config file
cat > wormfs.toml <<EOF
mount_point = "/mnt/wormfs"

[metadata]
database_path = "/var/lib/wormfs/metadata.db"
enable_wal = true
cache_size_mb = 100
enable_foreign_keys = true
synchronous = "Normal"
transaction_isolation = "Serializable"
enable_prepared_statements = true

[file_store]
disk_paths = ["/var/lib/wormfs/chunks"]
max_chunk_size = 1048576
default_data_shards = 2
default_parity_shards = 1

[filesystem]
node_id = 1
default_file_mode = "0644"
default_dir_mode = "0755"
uid = $(id -u)
gid = $(id -g)
EOF
```

## Mounting the Filesystem

### Using a Configuration File

```bash
wormfs mount --config wormfs.toml
```

### Using Command-Line Arguments

```bash
wormfs mount \
  --mount-point /mnt/wormfs \
  --metadata-db /var/lib/wormfs/metadata.db \
  --data-dir /var/lib/wormfs/chunks
```

### Foreground Mode (for debugging)

```bash
wormfs mount --config wormfs.toml --foreground --debug
```

### Verifying the Mount

```bash
# Check if mounted
mountpoint /mnt/wormfs

# Check mount details
mount | grep wormfs

# List the mount point
ls -la /mnt/wormfs
```

## Basic Operations

Once mounted, WormFS behaves like a regular filesystem:

### Creating Files

```bash
# Create a simple text file
echo "Hello, WormFS!" > /mnt/wormfs/hello.txt

# Create a file with dd
dd if=/dev/urandom of=/mnt/wormfs/random.dat bs=1M count=10
```

### Reading Files

```bash
# Read a text file
cat /mnt/wormfs/hello.txt

# Copy a file out
cp /mnt/wormfs/random.dat /tmp/backup.dat
```

### Creating Directories

```bash
# Create a directory
mkdir /mnt/wormfs/mydir

# Create nested directories
mkdir -p /mnt/wormfs/a/b/c
```

### Listing Files

```bash
# List files
ls -la /mnt/wormfs

# Recursive listing
find /mnt/wormfs -ls
```

### Deleting Files

```bash
# Remove a file
rm /mnt/wormfs/hello.txt

# Remove a directory (must be empty)
rmdir /mnt/wormfs/mydir

# Remove directory and contents
rm -rf /mnt/wormfs/mydir
```

### Moving/Renaming Files

```bash
# Rename a file
mv /mnt/wormfs/old.txt /mnt/wormfs/new.txt

# Move to different directory
mv /mnt/wormfs/file.txt /mnt/wormfs/mydir/
```

## Understanding Erasure Coding

WormFS uses erasure coding to provide data redundancy:

### How It Works

- **Data Shards**: Original file chunks (default: 2)
- **Parity Shards**: Redundancy chunks (default: 1)
- **Total Shards**: 3 (2 data + 1 parity)

With 2 data shards and 1 parity shard, WormFS can recover from the loss of 1 shard.

### Storage Overhead

Storage overhead = parity_shards / data_shards
- Default (2+1): 50% overhead
- Example: A 10 MB file consumes 15 MB of storage

### Chunk Layout

Files are split into stripes (max 1 MB by default):
```
Original File: [Data........................]
Stripe 0:      [Chunk 0][Chunk 1][Parity 0]
Stripe 1:      [Chunk 2][Chunk 3][Parity 1]
...
```

Each stripe is erasure coded independently.

### Verifying Erasure Coding

```bash
# Write a file
dd if=/dev/urandom of=/mnt/wormfs/test.dat bs=1M count=5

# Check chunk directory
ls -lh /var/lib/wormfs/chunks/

# You should see multiple chunk files (data + parity shards)
```

## Performance Considerations

### Write Performance

- **Write-through disabled** (default): Better write performance, data cached
- **Write-through enabled**: Slower writes, immediate durability
- **Buffer size**: Larger buffers = better throughput for large files

### Read Performance

- **Inode cache**: Frequently accessed files benefit from caching
- **Read buffer size**: Affects read performance for large files
- **Concurrent operations**: Multiple clients can read simultaneously

### Tuning for Your Workload

**For small files (< 1 MB)**:
```toml
[file_store]
max_chunk_size = 262144  # 256 KB

[filesystem]
inode_cache_size = 50000  # More cache entries
```

**For large files (> 10 MB)**:
```toml
[filesystem]
read_buffer_size = 4194304   # 4 MB
write_buffer_size = 4194304  # 4 MB
```

**For write-heavy workloads**:
```toml
[metadata]
synchronous = "Normal"  # vs "Full"
enable_wal = true

[filesystem]
write_through = false
```

## Unmounting

### Graceful Unmount

```bash
# Unmount using fusermount
fusermount -u /mnt/wormfs

# Or using umount (may require sudo)
sudo umount /mnt/wormfs
```

### Force Unmount

If normal unmount fails:

```bash
# Force unmount
sudo umount -f /mnt/wormfs

# If still stuck, lazy unmount
sudo umount -l /mnt/wormfs
```

### Cleaning Up Processes

```bash
# Find WormFS processes
pgrep -a wormfs

# Kill gracefully
pkill wormfs

# Force kill if needed
pkill -9 wormfs
```

## Next Steps

- See [configuration.md](configuration.md) for detailed configuration options
- See [troubleshooting.md](troubleshooting.md) for common issues and solutions
- See the [design document](design.md) for architecture details
- See [posix_compliance.md](posix_compliance.md) for POSIX feature support

## Phase 1 Limitations

Current limitations (to be addressed in future phases):

1. **Single Node**: No multi-node support yet
2. **No Replication**: Erasure coding only within a single node
3. **Limited Recovery**: Cannot recover from corrupted chunks yet
4. **No Dynamic Reconfiguration**: Erasure coding parameters are set at mount time
5. **No Snapshots**: No filesystem snapshot support
6. **No Quotas**: No per-user or per-directory quotas

See the [implementation plan](implementation_plan/phase1_minimal_data_path.md) for planned features.
