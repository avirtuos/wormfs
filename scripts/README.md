# WormFS Scripts

This directory contains utility scripts for WormFS development and demonstration.

## Available Scripts

### 🎬 `demo_wormfs.sh` - FUSE Filesystem Demo

Demonstrates WormFS Phase 1, Step 7 capabilities by mounting the filesystem and running various operations.

**Usage:**
```bash
./scripts/demo_wormfs.sh [OPTIONS]
```

**Options:**
- `-v, --verbose` - Enable verbose debug output
- `-h, --help` - Show help message

**What it demonstrates:**
1. ✅ Pre-flight checks (FUSE availability, binary existence)
2. ✅ Automatic build if binary doesn't exist
3. ✅ Mount WormFS filesystem
4. ✅ Verify mount in system mount table
5. ✅ Query root directory attributes (`stat`)
6. ✅ List directory contents (`ls`, `ls -la`)
7. ✅ Navigate into filesystem (`cd`)
8. ✅ Check filesystem statistics (`df`)
9. ✅ Show current limitations (file/directory creation)
10. ✅ Performance test (inode caching)
11. ✅ Automatic cleanup and unmount

**Example:**
```bash
# Run the demo
./scripts/demo_wormfs.sh

# Run with verbose output
./scripts/demo_wormfs.sh --verbose
```

**Features:**
- 🎨 Colored, readable output
- 🧹 Automatic cleanup on exit or Ctrl+C
- ⚡ Background mount with health checking
- 📊 Performance benchmarking
- 🔍 Shows what works and what's coming

**Requirements:**
- FUSE3 (Linux) or macFUSE (macOS)
- Rust toolchain for building
- `bc` command for calculations

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

### 🧪 `run_integration_tests.sh` - FUSE Integration Tests

Runs complete end-to-end FUSE integration tests that mount actual filesystems and verify all Phase 1 functionality.

**Usage:**
```bash
./scripts/run_integration_tests.sh [OPTIONS]
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
# Run all integration tests
./scripts/run_integration_tests.sh

# Run with verbose output
./scripts/run_integration_tests.sh --verbose

# Run specific test
./scripts/run_integration_tests.sh -t test_basic_operations

# Build only (no tests)
./scripts/run_integration_tests.sh --build-only

# Keep artifacts for debugging
./scripts/run_integration_tests.sh --keep-artifacts
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
# Run the demo to see WormFS in action
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
./scripts/run_integration_tests.sh

# Run specific integration test
./scripts/run_integration_tests.sh -t test_basic_operations
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
