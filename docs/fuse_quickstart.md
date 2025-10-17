# WormFS FUSE Integration - Quick Start

## Overview

WormFS Phase 1, Step 7 implements basic FUSE filesystem integration, allowing WormFS to be mounted as a local filesystem.

**Current Capabilities (Step 7):**
- Mount/unmount filesystem
- Query root directory (`stat /mnt/wormfs`)
- List directory contents (`readdir`)
- Get file attributes (`getattr`)

**Not Yet Implemented (Steps 8-9):**
- File creation/deletion
- File read/write
- Directory creation/deletion

## Prerequisites

### Linux
```bash
sudo apt-get install fuse3 libfuse3-dev
# OR
sudo yum install fuse3 fuse3-devel
```

### macOS
```bash
brew install macfuse
```

## Building

```bash
# Build with FUSE support (default)
cargo build --release

# Binary location
./target/release/wormfs
```

## Usage

### Mount Filesystem

**Basic mount with defaults:**
```bash
# Create mount point
mkdir /tmp/wormfs

# Mount filesystem
./target/release/wormfs mount --mount-point /tmp/wormfs --foreground

# In another terminal, test it
stat /tmp/wormfs
ls -la /tmp/wormfs
```

**Mount with custom paths:**
```bash
./target/release/wormfs mount \
    --mount-point /mnt/wormfs \
    --metadata-db /var/lib/wormfs/metadata.db \
    --data-dir /var/lib/wormfs/chunks \
    --foreground
```

**Mount with additional options:**
```bash
./target/release/wormfs mount \
    --mount-point /tmp/wormfs \
    --allow-other \              # Allow other users to access
    --fuse-debug \               # Enable FUSE debug logging
    --foreground
```

### Unmount Filesystem

**Using wormfs CLI:**
```bash
./target/release/wormfs unmount /tmp/wormfs
```

**Using system commands:**
```bash
# Linux
fusermount -u /tmp/wormfs

# macOS
umount /tmp/wormfs
```

## CLI Reference

### `wormfs mount`

Mount a WormFS filesystem via FUSE.

**Options:**
- `-m, --mount-point <PATH>` - Mount point directory (required)
- `-c, --config <FILE>` - Path to configuration file (optional)
- `-f, --foreground` - Run in foreground (don't daemonize)
- `--allow-root` - Allow root user to access filesystem
- `--allow-other` - Allow other users to access filesystem
- `--fuse-debug` - Enable FUSE kernel debug logging
- `--metadata-db <PATH>` - Override metadata database path
- `--data-dir <PATH>` - Override chunk storage directory
- `-v, --verbose` - Enable verbose logging
- `-d, --debug` - Enable debug logging

**Examples:**
```bash
# Simple mount
wormfs mount -m /tmp/wormfs -f

# Mount with custom paths
wormfs mount -m /mnt/wormfs \
    --metadata-db /opt/wormfs/metadata.db \
    --data-dir /opt/wormfs/chunks \
    -f

# Mount with debug logging
wormfs mount -m /tmp/wormfs -f -d --fuse-debug
```

### `wormfs unmount`

Unmount a WormFS filesystem.

**Usage:**
```bash
wormfs unmount <MOUNT_POINT>
```

## Testing

### Verify Mount
```bash
# Check if mounted
mount | grep wormfs

# Should show:
# wormfs on /tmp/wormfs type fuse (rw,nosuid,nodev,relatime,user_id=1000,group_id=1000)
```

### Test Root Directory
```bash
# Stat root directory
stat /tmp/wormfs

# Should show:
#   File: /tmp/wormfs
#   Size: 0         Blocks: 0          IO Block: 4096   directory
#   ...

# List directory
ls -la /tmp/wormfs

# Should show:
# drwxr-xr-x  2 user user    0 Jan  1 12:00 .
# drwxr-xr-x  2 user user    0 Jan  1 12:00 ..
```

### Test Readdir
```bash
# Currently returns only . and .. since no files exist yet
ls /tmp/wormfs
# (empty output)

# With -a flag
ls -a /tmp/wormfs
# .  ..
```

## Troubleshooting

### Mount fails with "Transport endpoint is not connected"
```bash
# The mount point may have a stale mount
fusermount -u /tmp/wormfs
# OR
umount /tmp/wormfs

# Then try mounting again
```

### Mount fails with "Permission denied"
```bash
# Check if you have permission to access FUSE
groups | grep fuse

# If not in fuse group, add yourself:
sudo usermod -a -G fuse $USER
# Then log out and log back in
```

### "FUSE support not compiled" error
```bash
# Rebuild with FUSE feature
cargo build --release --features fuser
```

## Architecture

### Data Flow
```
User Process
    │
    ├─ open("/tmp/wormfs/file")
    │
    ▼
FUSE Kernel Module
    │
    ├─ lookup(parent=1, name="file")
    │
    ▼
wormfs (user space)
    │
    ├─ FuseAdapter::lookup()
    │
    ▼
FileSystemServiceImpl
    │
    ├─ getattr(inode)
    │
    ▼
MetadataStore (SQLite)
    │
    └─ Query file metadata
```

### Components

1. **wormfs CLI** - Command-line interface (src/bin/wormfs.rs)
2. **Mount Utilities** - Mount/unmount functions (src/filesystem_service/mount.rs)
3. **FuseAdapter** - FUSE trait implementation (src/filesystem_service/fuse_adapter.rs)
4. **FileSystemServiceImpl** - Core filesystem logic (src/filesystem_service/implementation.rs)
5. **InodeCache** - LRU cache for metadata (src/filesystem_service/inode.rs)
6. **MetadataStore** - SQLite backend (src/metadata_store/)

## Next Steps (Phase 1, Steps 8-9)

Step 7 provides the foundation. Next steps will add:

**Step 8 - File Operations:**
- `create()` - Create new files
- `open()` - Open files for reading/writing
- `read()` - Read file data
- `write()` - Write file data
- `unlink()` - Delete files
- `truncate()` - Change file size

**Step 9 - Directory Operations:**
- `mkdir()` - Create directories
- `rmdir()` - Remove directories
- Full `readdir()` - List actual directory contents
- `rename()` - Move/rename files and directories

## Configuration

Default configuration (when no config file provided):
```toml
[filesystem]
uid = <current user>
gid = <current group>
inode_cache_size = 10000
inode_cache_ttl_secs = 60

[metadata]
path = "<mount_point>/.wormfs/metadata.db"
read_pool_size = 8
enable_wal = true
cache_size_mb = 10

[file_store]
data_path = "<mount_point>/.wormfs/chunks"
stripe_size = 1048576  # 1MB
data_shards = 2
parity_shards = 1
```

## Performance

Phase 1 (single-node) performance targets:
- Mount time: <1 second
- getattr (cached): <0.1ms
- getattr (uncached): <5ms
- readdir: <10ms for empty directory
- Cache hit rate: >90% for repeated operations

## Limitations (Phase 1, Step 7)

- **Read-only**: No file/directory creation yet
- **Single-node**: No distributed operation
- **No persistence**: Metadata lost on unmount (will be fixed in Step 8)
- **Root only**: Only root directory exists initially
- **Blocking I/O**: FUSE operations block (acceptable for Phase 1)

## POSIX Compliance

⚠️ **Important:** WormFS has deliberate deviations from strict POSIX compliance:

- **nlink always returns 1** for all files and directories
- **Hard links are not supported** and will never be supported
- This design follows the Btrfs precedent and simplifies distributed systems implementation
- 99% of applications work without modification
- `find` command may be 5-15% slower (workaround: use `find -noleaf`)

For detailed information about POSIX compliance, rationale, and application compatibility, see [`docs/posix_compliance.md`](posix_compliance.md).

## References

- [Implementation Plan](../docs/implementation_plan/phase1_minimal_data_path.md)
- [GitHub Issue #64](https://github.com/avirtuos/wormfs/issues/64)
- [FUSE Protocol Documentation](https://www.kernel.org/doc/html/latest/filesystems/fuse.html)
