# WormFS Troubleshooting Guide

## Overview

This guide covers common issues you may encounter when using WormFS Phase 1 and their solutions.

## Table of Contents

1. [Mount Issues](#mount-issues)
2. [Permission Problems](#permission-problems)
3. [Performance Issues](#performance-issues)
4. [Data Integrity Issues](#data-integrity-issues)
5. [Configuration Errors](#configuration-errors)
6. [Database Issues](#database-issues)
7. [Unmount Issues](#unmount-issues)
8. [Debugging Tips](#debugging-tips)

## Mount Issues

### Error: "Failed to mount: Device or resource busy"

**Symptoms**: Mount command fails with EBUSY error

**Causes**:
- Mount point is already in use
- Previous WormFS instance didn't unmount cleanly
- Another process has files open in the mount point

**Solutions**:

1. Check if already mounted:
   ```bash
   mountpoint /mnt/wormfs
   mount | grep wormfs
   ```

2. Unmount existing instance:
   ```bash
   fusermount -u /mnt/wormfs
   # or
   sudo umount /mnt/wormfs
   ```

3. Check for processes using the mount point:
   ```bash
   lsof | grep /mnt/wormfs
   fuser -m /mnt/wormfs
   ```

4. Force unmount if needed:
   ```bash
   sudo umount -l /mnt/wormfs  # Lazy unmount
   ```

### Error: "mount_point must be specified in config file or via --mount-point CLI flag"

**Symptoms**: WormFS fails to start with this error message

**Cause**: Missing `mount_point` in configuration file

**Solution**:

Add `mount_point` to your config file before any sections:
```toml
mount_point = "/mnt/wormfs"

[metadata]
# ...
```

### Error: "Permission denied" when mounting

**Symptoms**: Mount command fails with EPERM

**Causes**:
- User not in `fuse` group
- Mount point doesn't have correct permissions
- SELinux or AppArmor blocking FUSE

**Solutions**:

1. Add user to fuse group:
   ```bash
   sudo usermod -a -G fuse $USER
   # Log out and back in
   ```

2. Check mount point permissions:
   ```bash
   ls -ld /mnt/wormfs
   sudo chmod 755 /mnt/wormfs
   sudo chown $USER:$USER /mnt/wormfs
   ```

3. Check SELinux (if applicable):
   ```bash
   # Temporarily disable
   sudo setenforce 0

   # Or add exception
   sudo ausearch -c 'wormfs' --raw | audit2allow -M my-wormfs
   sudo semodule -i my-wormfs.pp
   ```

### Error: "Mount point directory doesn't exist"

**Symptoms**: Mount fails because directory is missing

**Solution**:
```bash
mkdir -p /mnt/wormfs
```

### Error: "FUSE library not found"

**Symptoms**: Binary won't run or compile fails

**Solution**:

Install FUSE development libraries:
```bash
# Ubuntu/Debian
sudo apt-get install fuse libfuse-dev

# Fedora/RHEL
sudo dnf install fuse fuse-devel

# Rebuild WormFS
cargo build --release --features fuser --bin wormfs
```

## Permission Problems

### Error: "Operation not permitted" when creating files

**Symptoms**: Cannot create files or directories

**Causes**:
- UID/GID mismatch in configuration
- Mount point permissions
- File mode restrictions

**Solutions**:

1. Check your UID and GID:
   ```bash
   id -u  # Your UID
   id -g  # Your GID
   ```

2. Update config to match:
   ```toml
   [filesystem]
   uid = 1000  # Your UID
   gid = 1000  # Your GID
   ```

3. Verify default modes:
   ```toml
   [filesystem]
   default_file_mode = "0644"
   default_dir_mode = "0755"
   ```

### Files have wrong permissions

**Symptoms**: Created files don't have expected permissions

**Cause**: Incorrect `default_file_mode` or `default_dir_mode` configuration

**Solution**:

Use octal string format (recommended):
```toml
[filesystem]
default_file_mode = "0644"  # Octal string
default_dir_mode = "0755"   # Octal string
```

Or decimal format:
```toml
[filesystem]
default_file_mode = 420  # Decimal (equals 0644 octal)
default_dir_mode = 493   # Decimal (equals 0755 octal)
```

**Verify**:
```bash
stat /mnt/wormfs/testfile
# Should show expected permissions
```

## Performance Issues

### Slow write performance

**Symptoms**: Writing files is slower than expected

**Causes**:
- Write-through mode enabled
- Small buffer sizes
- Database synchronous mode set to "Full"
- Slow disk for metadata or chunks

**Solutions**:

1. Disable write-through (if acceptable):
   ```toml
   [filesystem]
   write_through = false
   ```

2. Increase buffer sizes:
   ```toml
   [filesystem]
   write_buffer_size = 4194304  # 4 MB
   ```

3. Adjust database synchronous mode:
   ```toml
   [metadata]
   synchronous = "Normal"  # Not "Full"
   ```

4. Use faster storage:
   - Put metadata.db on SSD/NVMe
   - Put chunks on fast disks

### Slow read performance

**Symptoms**: Reading files is slower than expected

**Causes**:
- Small cache sizes
- Small buffer sizes
- Cold cache (first access)

**Solutions**:

1. Increase cache sizes:
   ```toml
   [metadata]
   cache_size_mb = 1000

   [filesystem]
   inode_cache_size = 100000
   inode_cache_ttl = 300
   ```

2. Increase buffer sizes:
   ```toml
   [filesystem]
   read_buffer_size = 4194304  # 4 MB
   ```

3. Increase read pool:
   ```toml
   [metadata]
   read_pool_size = 16
   ```

### High memory usage

**Symptoms**: WormFS consumes too much RAM

**Causes**:
- Large cache sizes
- Too many file handles
- Large buffer sizes

**Solutions**:

1. Reduce cache sizes:
   ```toml
   [metadata]
   cache_size_mb = 100  # Reduce from 1000

   [filesystem]
   inode_cache_size = 10000  # Reduce from 100000
   ```

2. Reduce max file handles:
   ```toml
   [filesystem]
   max_file_handles = 10000  # Reduce from 100000
   ```

## Data Integrity Issues

### Error: "Checksum mismatch"

**Symptoms**: Data read doesn't match data written

**Causes**:
- Disk corruption
- Bug in erasure coding
- Incomplete write

**Diagnostic Steps**:

1. Run data integrity test:
   ```bash
   # Create test file
   dd if=/dev/urandom of=/tmp/test.dat bs=1M count=10
   md5sum /tmp/test.dat > /tmp/original.md5

   # Copy to WormFS
   cp /tmp/test.dat /mnt/wormfs/test.dat

   # Copy back and verify
   cp /mnt/wormfs/test.dat /tmp/test-verify.dat
   md5sum /tmp/test-verify.dat
   # Should match original.md5
   ```

2. Check disk health:
   ```bash
   sudo smartctl -a /dev/sda  # Check for errors
   ```

3. Enable debug logging and check for errors:
   ```bash
   RUST_LOG=wormfs=debug wormfs mount --config wormfs.toml --foreground
   ```

### Corrupted chunks

**Symptoms**: Cannot read file after writing

**Causes**:
- Disk failure
- Power loss during write
- Filesystem full

**Solutions**:

1. Check disk space:
   ```bash
   df -h /var/lib/wormfs/chunks
   ```

2. Check chunk directory:
   ```bash
   ls -lh /var/lib/wormfs/chunks/
   # Look for zero-byte or corrupted files
   ```

3. Restore from backup if available

## Configuration Errors

### Error: "Failed to parse config file"

**Symptoms**: TOML parsing error

**Causes**:
- Invalid TOML syntax
- Missing required fields
- Type mismatch

**Solutions**:

1. Validate TOML syntax:
   ```bash
   # Use an online TOML validator
   # Or check with Python
   python3 -c "import toml; toml.load('wormfs.toml')"
   ```

2. Check for common mistakes:
   - Missing quotes around strings
   - Wrong data types (string vs integer)
   - Missing section headers
   - Typos in field names

3. Example of common error:
   ```toml
   # WRONG - missing quotes
   mount_point = /mnt/wormfs

   # CORRECT
   mount_point = "/mnt/wormfs"
   ```

### Error: "missing field `enable_foreign_keys`"

**Symptoms**: Config validation fails

**Cause**: Required field missing from metadata section

**Solution**:

Add the missing field:
```toml
[metadata]
database_path = "/var/lib/wormfs/metadata.db"
enable_foreign_keys = true  # Add this line
```

### File mode format errors

**Symptoms**: Invalid default_file_mode or default_dir_mode value

**Solution**:

Use either octal string OR decimal integer:
```toml
# Option 1: Octal string (recommended)
default_file_mode = "0644"
default_dir_mode = "0755"

# Option 2: Decimal integer
default_file_mode = 420
default_dir_mode = 493
```

## Database Issues

### Error: "Database is locked"

**Symptoms**: Operations fail with SQLITE_BUSY error

**Causes**:
- Too many concurrent operations
- Read pool exhausted
- Long-running transaction

**Solutions**:

1. Increase read pool:
   ```toml
   [metadata]
   read_pool_size = 16  # Increase from 4
   read_pool_timeout_secs = 60  # Increase timeout
   ```

2. Check for stuck processes:
   ```bash
   lsof | grep metadata.db
   ```

3. Restart WormFS

### Error: "Database corruption detected"

**Symptoms**: SQLite reports corruption

**Solutions**:

1. Try to recover:
   ```bash
   # Stop WormFS
   fusermount -u /mnt/wormfs

   # Backup database
   cp /var/lib/wormfs/metadata.db /var/lib/wormfs/metadata.db.backup

   # Try to repair
   sqlite3 /var/lib/wormfs/metadata.db "PRAGMA integrity_check;"
   ```

2. Restore from backup if available

3. If unrecoverable, reinitialize (DATA LOSS):
   ```bash
   # WARNING: This deletes all metadata
   rm /var/lib/wormfs/metadata.db
   # Remount will create fresh database
   ```

### WAL file growing too large

**Symptoms**: `metadata.db-wal` file is very large

**Cause**: WAL checkpointing not occurring

**Solution**:

1. Checkpoint manually:
   ```bash
   sqlite3 /var/lib/wormfs/metadata.db "PRAGMA wal_checkpoint(TRUNCATE);"
   ```

2. Ensure WAL mode is enabled:
   ```toml
   [metadata]
   enable_wal = true
   ```

## Unmount Issues

### Error: "Device is busy" when unmounting

**Symptoms**: Cannot unmount filesystem

**Cause**: Processes still using the filesystem

**Solutions**:

1. Find processes using the mount:
   ```bash
   lsof | grep /mnt/wormfs
   fuser -m /mnt/wormfs
   ```

2. Kill processes if safe:
   ```bash
   fuser -k /mnt/wormfs
   ```

3. Change to different directory:
   ```bash
   cd ~  # Don't be in /mnt/wormfs when unmounting
   ```

4. Force unmount:
   ```bash
   sudo umount -f /mnt/wormfs
   # or
   sudo umount -l /mnt/wormfs  # Lazy unmount
   ```

### WormFS process won't die

**Symptoms**: Process remains after unmount

**Solutions**:

```bash
# Find the process
pgrep -a wormfs

# Try graceful kill
pkill wormfs

# Wait a few seconds
sleep 2

# Force kill if needed
pkill -9 wormfs
```

## Debugging Tips

### Enable debug logging

```bash
RUST_LOG=wormfs=debug wormfs mount --config wormfs.toml --foreground
```

Logging levels:
- `error`: Errors only
- `warn`: Warnings and errors
- `info`: General information
- `debug`: Detailed debugging
- `trace`: Very detailed (verbose)

### Component-specific logging

```bash
# Log only file_store module
RUST_LOG=wormfs::file_store=debug wormfs mount ...

# Multiple modules
RUST_LOG=wormfs::file_store=debug,wormfs::metadata_store=trace wormfs mount ...
```

### Run in foreground for debugging

```bash
wormfs mount --config wormfs.toml --foreground --debug
```

This shows all log output in the terminal.

### Check FUSE operations

```bash
# Mount with FUSE debug output
wormfs mount --config wormfs.toml --foreground --debug 2>&1 | grep FUSE
```

### Inspect metadata database

```bash
sqlite3 /var/lib/wormfs/metadata.db

# List tables
.tables

# Check files
SELECT * FROM files LIMIT 10;

# Check inodes
SELECT * FROM inodes LIMIT 10;

# Check integrity
PRAGMA integrity_check;
```

### Inspect chunk files

```bash
# List chunks
ls -lh /var/lib/wormfs/chunks/

# Count chunks
find /var/lib/wormfs/chunks/ -type f | wc -l

# Check for zero-byte files (corruption indicator)
find /var/lib/wormfs/chunks/ -type f -size 0
```

### Monitor system resources

```bash
# Watch memory usage
watch -n 1 'ps aux | grep wormfs'

# Watch file descriptors
watch -n 1 'lsof -p $(pgrep wormfs) | wc -l'

# Watch disk I/O
sudo iotop -p $(pgrep wormfs)
```

### Test data integrity

See the demo script for comprehensive data integrity testing:
```bash
./scripts/demo_wormfs.sh
```

Or manual test:
```bash
# Create test file with known checksum
dd if=/dev/urandom of=/tmp/test.dat bs=1M count=10
ORIGINAL=$(md5sum /tmp/test.dat | awk '{print $1}')

# Copy through WormFS
cp /tmp/test.dat /mnt/wormfs/test.dat
cp /mnt/wormfs/test.dat /tmp/test-verify.dat

# Verify checksum
VERIFY=$(md5sum /tmp/test-verify.dat | awk '{print $1}')

if [ "$ORIGINAL" = "$VERIFY" ]; then
    echo "✓ Data integrity verified"
else
    echo "✗ CHECKSUM MISMATCH!"
fi
```

## Getting Help

If you continue to experience issues:

1. **Check logs**: Always enable debug logging first
2. **Check GitHub issues**: https://github.com/your-org/wormfs/issues
3. **File a bug report**: Include:
   - WormFS version
   - Operating system and version
   - Configuration file (sanitized)
   - Debug logs
   - Steps to reproduce

## Known Limitations (Phase 1)

- **Single node only**: No multi-node support
- **No chunk recovery**: Cannot recover from corrupted chunks
- **Limited error recovery**: Some error conditions require remount
- **No dynamic reconfiguration**: Must unmount to change settings
- **No xattr support**: Extended attributes not fully implemented

See the [user guide](user_guide_phase1.md) for more information on Phase 1 features and limitations.
