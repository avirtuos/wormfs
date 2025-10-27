# WormFS Scripts

This directory contains utility scripts for WormFS development and demonstration.

## Available Scripts

### 🌐 `demo_wormfs.sh` - Complete WormFS Demo with Networking

Demonstrates all Phase 1 WormFS capabilities PLUS peer-to-peer networking with two FUSE mount instances discovering each other via libp2p and displaying real-time network status via Admin UI.

**Usage:**
```bash
./scripts/demo_wormfs.sh [OPTIONS]
```

**Options:**
- `-v, --verbose` - Enable verbose output
- `--skip-tests` - Skip file/directory operation tests (mount only)
- `-h, --help` - Show help message

**What it demonstrates:**
1. ✅ Configuration Management (TOML + CLI overrides)
2. ✅ Metadata Persistence (MetadataStore + SQLite)
3. ✅ Erasure Coding (FileStore + Reed-Solomon 2+1)
4. ✅ StripeCache (Write Buffering & I/O Amplification Reduction)
5. ✅ FUSE Filesystem (FileSystemService)
6. ✅ **StorageNetwork (libp2p peer-to-peer networking)**
7. ✅ **Two Networked Mount Points (connected via libp2p)**
8. ✅ File & Directory Operations
9. ✅ Metrics Collection (I/O Amplification Tracking)
10. ✅ Graceful Shutdown

**Example:**
```bash
# Run the complete demo with networking
./scripts/demo_wormfs.sh

# The script will:
# 1. Build wormfs binary with FUSE support
# 2. Create two mount instances with unique configs
# 3. Mount both filesystems with StorageNetwork enabled
# 4. Display Admin UI URLs:
#    - Node 1: http://127.0.0.1:9090 (Mount 1)
#    - Node 2: http://127.0.0.1:9091 (Mount 2)
# 5. Show peer connectivity status
# 6. Run complete Phase 1 tests on Node 1
# 7. Demonstrate file operations, erasure coding, and data integrity

# Open both Admin UIs in your browser
# Click the "Network" tab to see peer connectivity!

# Quick test without file operations
./scripts/demo_wormfs.sh --skip-tests
```

**Features:**
- 🎨 Colored, readable output
- 🧹 Automatic cleanup on exit or Ctrl+C
- 🌐 Embedded Admin UI in each mount instance
- 📊 Live metrics and heartbeat monitoring
- 🔄 Real-time network status updates with actual peer data
- 📁 Complete Phase 1 FUSE filesystem testing
- 🔐 Erasure coding and data integrity verification

**What you'll see:**
- **In Terminal:**
  - Mount status for both nodes
  - Peer connectivity verification
  - File operations on Node 1
  - Performance metrics
  - Data integrity checks (2GB file with MD5 verification)
- **In Admin UI:** Network tab showing:
  - Connected peers with actual node IDs
  - Last heartbeat timestamps (auto-updating)
  - Heartbeat sequence numbers
  - Connection states
  - Peer addresses

**Requirements:**
- Rust toolchain for building
- FUSE3 (Linux) or macFUSE (macOS)
- Two available ports (9090, 9091) for Admin UIs
- Two available ports (7101, 7102) for libp2p

---

### ✅ `validate.sh` - Quality Validation

Runs comprehensive quality checks including build, tests, formatting, and linting.

**Usage:**
```bash
./scripts/validate.sh
```

**What it checks:**
1. Cargo build (no errors or warnings)
2. Unit tests
3. Integration tests
4. Test utilities feature
5. Code formatting (`cargo fmt`)
6. Linter (`cargo clippy`)

**Use this before:**
- Opening a pull request
- Committing major changes
- Running CI/CD pipelines

---

### 🧪 `run_fuse_integration_tests.sh` - FUSE Integration Tests

Runs complete end-to-end FUSE integration tests that mount actual filesystems and verify all Phase 1 functionality.

**Usage:**
```bash
./scripts/run_fuse_integration_tests.sh [OPTIONS]
```

**Options:**
- `-v, --verbose` - Enable verbose test output
- `-b, --build-only` - Build binary but don't run tests
- `-t, --test NAME` - Run only the specified test (e.g., `test_basic_operations`)
- `--keep-artifacts` - Don't clean up temp directories (useful for debugging)
- `-h, --help` - Show help message

**What it tests:**
1. ✅ Basic file operations (create, read, write, delete)
2. ✅ Nested directory structures
3. ✅ File size variants (empty, KB, MB ranges)
4. ✅ Erasure coding verification
5. ✅ Performance with large files (100MB)
6. ✅ Stress testing (1000 files)
7. ✅ Data integrity with MD5 checksums

**Example:**
```bash
# Run all FUSE integration tests
./scripts/run_fuse_integration_tests.sh

# Run with verbose output
./scripts/run_fuse_integration_tests.sh --verbose

# Run specific test
./scripts/run_fuse_integration_tests.sh -t test_basic_operations

# Build only (no tests)
./scripts/run_fuse_integration_tests.sh --build-only

# Keep artifacts for debugging
./scripts/run_fuse_integration_tests.sh --keep-artifacts
```

**Features:**
- 🧹 Comprehensive cleanup of mounts and processes
- 🔒 Sequential test execution (no mount conflicts)
- 📊 Performance timing and reporting
- 🛡️ Robust error handling with automatic cleanup
- 🔍 Detects and cleans orphaned FUSE mounts

**Requirements:**
- FUSE3 (Linux) or macFUSE (macOS)
- Permission to mount filesystems
- No other wormfs processes running

**When to run:**
- Before major releases
- After FUSE layer changes
- When debugging user-reported FUSE issues
- As part of pre-release QA

**Note:** These tests take 20-30 seconds and require actual FUSE mounts. Unit tests in `cargo test` are faster for regular development.

---

## Installation & Setup

### Linux (Ubuntu/Debian)
```bash
# Install FUSE3
sudo apt-get install fuse3 libfuse3-dev

# Add yourself to fuse group (optional)
sudo usermod -a -G fuse $USER
# Log out and log back in
```

### Linux (RHEL/CentOS/Fedora)
```bash
# Install FUSE3
sudo yum install fuse3 fuse3-devel
```

### macOS
```bash
# Install macFUSE
brew install macfuse
```

---

## Development Workflow

### Quick Demo
```bash
# Run multi-node networking demo with Admin UI
./scripts/demo_wormfs.sh
```

### Before Committing
```bash
# Run validation to ensure code quality
./scripts/validate.sh
```

### Integration Testing
```bash
# Run FUSE integration tests (pre-release QA)
./scripts/run_fuse_integration_tests.sh

# Run specific integration test
./scripts/run_fuse_integration_tests.sh -t test_basic_operations
```

### Manual Testing
```bash
# Build the binary
cargo build --release

# Mount manually
mkdir /tmp/wormfs
./target/release/wormfs mount --mount-point /tmp/wormfs --foreground

# In another terminal, test
stat /tmp/wormfs
ls -la /tmp/wormfs

# Unmount
./target/release/wormfs unmount /tmp/wormfs
```

---

## Troubleshooting

### Demo script fails with "FUSE not found"
Install FUSE3 or macFUSE (see Installation section above).

### Demo script fails with "Permission denied"
You may need to be in the `fuse` group:
```bash
sudo usermod -a -G fuse $USER
# Log out and log back in
```

### Mount fails with "Transport endpoint is not connected"
Clean up stale mount:
```bash
fusermount -u /tmp/wormfs  # Linux
umount /tmp/wormfs         # macOS
```

### "Binary not found" error
The demo script will automatically build the binary. If manual build fails:
```bash
cargo clean
cargo build --release
```

---

## Contributing

When adding new scripts:
1. Make them executable: `chmod +x scripts/your_script.sh`
2. Add a header comment explaining purpose
3. Use colored output for readability
4. Implement `--help` flag
5. Add cleanup/error handling
6. Document in this README

---

## Script Conventions

All scripts in this directory follow these conventions:

- **Colored Output:**
  - 🔴 Red: Errors
  - 🟢 Green: Success
  - 🟡 Yellow: Info/Warnings
  - 🔵 Blue: Commands/Steps
  - 🔷 Cyan: Debug/Verbose

- **Exit Codes:**
  - `0`: Success
  - `1`: Error/Failure

- **Cleanup:**
  - All scripts implement cleanup on exit
  - Use `trap` for signal handling
  - Clean up temporary files/directories

- **Help:**
  - All scripts support `--help` flag
  - Display usage, options, and examples
