<p align="center">
<img src="docs/images/logo.png" width="400">
</p>

# WormFS

WormFS, short for write-once-read-many file system, is a user-space distributed file system that uses erasure encoding to spread files across multiple storage devices, each running their own commodity filesystems. This allows great flexibility with respect to configuring device failure tolerance at a file or directory level. I envision this being extremely useful for media storage and deep archive use-cases.

Much of the architecture of this project is inspired by LizardFS' simplicity with a goal of offering greater control and visibility over how chunks are stored, replicated, and recovered.

## 📊 Project Status

**Current Phase:** Phase 1 - Minimal Data Path (Steps 1-10 Complete ✅)

WormFS is a work in progress that's being developed iteratively following a phased implementation plan. **Phase 1 is now functional** with all core components wired together!

### What's Working Now (Phase 1, Steps 1-10)
- ✅ **MetadataStore**: SQLite-based metadata persistence with WAL mode
- ✅ **FileStore**: Local chunk storage with Reed-Solomon erasure coding
- ✅ **FileSystemService**: FUSE integration for filesystem operations
- ✅ **StorageNode**: Component orchestrator that wires everything together
- ✅ **Configuration**: TOML-based config with environment variable overrides
- ✅ **File Operations**: Create, read, write, delete files
- ✅ **Directory Operations**: Create, list, remove directories
- ✅ **Integration Tests**: Comprehensive test suite (All tests passing)

### What's Coming Next
- 🚧 Phase 1, Step 11-12: CLI refinement and final integration testing
- 📋 Phase 2: Raft consensus and distributed operation
- 📋 Phase 3: Multi-node storage with distributed erasure coding
- 📋 Phase 4: Robustness and recovery features
- 📋 Phase 5: Observability and production testing

---

## 🚀 Quick Start

### Prerequisites

**Required:**
- Rust 1.70+ (with Cargo)
- FUSE3 (Linux) or macFUSE (macOS)

**Install FUSE:**
```bash
# Ubuntu/Debian
sudo apt-get install fuse3 libfuse3-dev

# RHEL/CentOS/Fedora
sudo yum install fuse3 fuse3-devel

# macOS
brew install macfuse
```

### Building WormFS

```bash
# Clone the repository
git clone <repository-url>
cd wormfs_v2

# Build with default features (includes FUSE)
cargo build --release

# Or build with all features
cargo build --release --all-features
```

### Running the Demo

The quickest way to see WormFS in action:

```bash
# Run the interactive demo (handles everything automatically)
./scripts/demo_wormfs.sh

# Or run with verbose output
./scripts/demo_wormfs.sh --verbose
```

### Manual Usage

#### 1. Run the Storage Node

```bash
# Run with default configuration
cargo run --bin wormfs-storage-node -- \
  --data-dir /tmp/wormfs-data \
  --verbose

# Or use a config file
cargo run --bin wormfs-storage-node -- \
  --config examples/config.toml \
  --verbose
```

#### 2. Mount the Filesystem (in another terminal)

```bash
# Create mount point
mkdir -p /tmp/wormfs-mount

# Mount the filesystem
cargo run --bin wormfs -- mount \
  --mount-point /tmp/wormfs-mount \
  --metadata-db /tmp/wormfs-data/metadata.db \
  --data-dir /tmp/wormfs-data/chunks \
  --foreground
```

#### 3. Use the Filesystem

```bash
# In another terminal
cd /tmp/wormfs-mount

# Create files
echo "Hello WormFS!" > hello.txt
cat hello.txt

# Create directories
mkdir test_dir
ls -la

# Copy files
cp /etc/hosts test_dir/
cat test_dir/hosts
```

#### 4. Unmount

```bash
# Linux
fusermount -u /tmp/wormfs-mount

# macOS
umount /tmp/wormfs-mount
```

---

## ⚙️ Configuration

WormFS supports three levels of configuration (in order of precedence):

### 1. TOML Configuration File

Create a `config.toml` file (see `examples/config.toml` for full example):

```toml
# Node configuration
node_id = "wormfs-node-001"
listen_address = "127.0.0.1:7000"
data_dir = "/var/lib/wormfs"

# Metadata database
metadata_db_path = "/var/lib/wormfs/metadata.db"

# Erasure coding settings
default_stripe_size = 1048576  # 1MB
default_data_shards = 2
default_parity_shards = 1

# Filesystem settings
default_uid = 1000
default_gid = 1000
lock_timeout = 30  # seconds
```

### 2. Environment Variables

Override specific settings:

```bash
export WORMFS_NODE_ID="my-custom-node"
export WORMFS_DATA_DIR="/custom/path"
export WORMFS_LISTEN_ADDRESS="0.0.0.0:8000"
```

### 3. CLI Arguments

Highest priority - overrides everything:

```bash
wormfs-storage-node \
  --config config.toml \
  --node-id override-node \
  --data-dir /tmp/override \
  --verbose
```

---

## 🏗️ Architecture (Phase 1)

WormFS Phase 1 consists of three main components wired together by the StorageNode orchestrator:

```
┌─────────────────────────────────────────┐
│         StorageNode (Orchestrator)      │
├─────────────────────────────────────────┤
│                                         │
│  ┌───────────────────────────────────┐ │
│  │   MetadataStore (SQLite + WAL)    │ │
│  │   • File/directory metadata       │ │
│  │   • Chunk location tracking       │ │
│  │   • Distributed locks             │ │
│  │   • Inode management              │ │
│  └───────────────────────────────────┘ │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │   FileStore (Erasure Coding)      │ │
│  │   • Reed-Solomon encoding         │ │
│  │   • Local chunk storage           │ │
│  │   • Chunk verification            │ │
│  │   • Two-phase commit staging      │ │
│  └───────────────────────────────────┘ │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │   FileSystemService (FUSE)        │ │
│  │   • POSIX filesystem API          │ │
│  │   • File operations               │ │
│  │   • Directory operations          │ │
│  │   • Lock management               │ │
│  └───────────────────────────────────┘ │
│                                         │
└─────────────────────────────────────────┘
```

### Data Flow

**Write Operation:**
1. Client writes data via FUSE
2. FileSystemService validates and splits into stripes
3. FileStore applies Reed-Solomon erasure coding
4. Chunks written to local disk
5. MetadataStore updated with chunk locations

**Read Operation:**
1. Client reads data via FUSE
2. FileSystemService queries MetadataStore for chunk locations
3. FileStore retrieves and reconstructs stripes from chunks
4. Data returned to client

---

## 🧪 Testing

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --test phase1_component_wiring
cargo test --lib storage_node
cargo test --lib metadata_store
cargo test --lib file_store
cargo test --lib filesystem_service

# Run with logging
RUST_LOG=debug cargo test -- --nocapture
```

---

## 📝 Development

This project uses a structured, spec-driven development approach:

1. **Design Documentation** (`docs/design.md`)
   - Overall architecture
   - Component specifications
   - Data flow diagrams

2. **Implementation Plans** (`docs/implementation_plan/`)
   - Phase 1: Minimal Data Path
   - Phase 2: Consensus Layer (Raft)
   - Phase 3: Distributed Storage
   - Phase 4: Robustness & Recovery
   - Phase 5: Observability & Testing

3. **Component Specifications** (`docs/components/`)
   - Detailed specs for each component
   - Interface definitions
   - Implementation notes

### Development Workflow

```bash
# 1. Make changes
vim src/...

# 2. Run tests
cargo test

# 3. Check formatting
cargo fmt --check

# 4. Run linter
cargo clippy -- -D warnings

# 5. Run comprehensive validation
./scripts/validate.sh

# 6. Commit changes
git add .
git commit -m "Description"
```

---

## 🎯 Current Capabilities & Limitations

### ✅ What Works (Phase 1)

**Filesystem Operations:**
- ✅ Create, read, write, delete files
- ✅ Create, list, remove directories
- ✅ File attributes (permissions, timestamps, size)
- ✅ Symbolic links

**Data Management:**
- ✅ Erasure coding (configurable data/parity shards)
- ✅ Local chunk storage
- ✅ Stripe-based file organization
- ✅ Chunk verification

**Metadata:**
- ✅ SQLite-based persistence
- ✅ Concurrent reads (connection pool)
- ✅ WAL mode for performance
- ✅ ACID transactions

### 🚧 Current Limitations (Single-Node Phase 1)

- ⚠️ **Single Node Only**: No distributed operation yet
- ⚠️ **Local Storage Only**: Chunks stored on one machine
- ⚠️ **No Replication**: Each chunk stored once (within erasure coding)
- ⚠️ **No Consensus**: No Raft coordination (Phase 2)
- ⚠️ **No Network Protocol**: No node-to-node communication
- ⚠️ **Limited Durability**: Single point of failure

These limitations will be addressed in future phases!

---

## 🐛 Troubleshooting

### Build Issues

**Problem:** `error: linker 'cc' not found`
```bash
# Install build essentials
sudo apt-get install build-essential  # Ubuntu/Debian
sudo yum groupinstall "Development Tools"  # RHEL/CentOS
```

**Problem:** `error: failed to run custom build command for 'fuser'`
```bash
# Install FUSE development headers
sudo apt-get install libfuse3-dev  # Ubuntu/Debian
```

### Runtime Issues

**Problem:** `Permission denied` when mounting
```bash
# Add user to fuse group
sudo usermod -a -G fuse $USER
# Log out and log back in
```

**Problem:** `Transport endpoint is not connected`
```bash
# Clean up stale mount
fusermount -u /tmp/wormfs-mount  # Linux
umount /tmp/wormfs-mount         # macOS
```

**Problem:** Tests fail with database locked
```bash
# Clean test artifacts
cargo clean
rm -rf /tmp/wormfs-test-*
```

**Problem:** `fusermount3: option allow_other only allowed if 'user_allow_other' is set`

This occurs when auto_unmount is enabled without proper FUSE configuration. WormFS now disables auto_unmount by default to avoid this issue.

**Solution A - Use defaults (recommended for development):**
```bash
# The demo script and binaries now work without any configuration
./scripts/demo_wormfs.sh
```

**Solution B - Enable user_allow_other system-wide (if you need auto_unmount):**
```bash
# Edit FUSE config (requires sudo)
sudo nano /etc/fuse.conf
# Uncomment or add the line: user_allow_other

# Then you can use auto_unmount
cargo run --bin wormfs -- mount \
  --mount-point /tmp/wormfs-mount \
  --metadata-db /tmp/wormfs-data/metadata.db \
  --data-dir /tmp/wormfs-data/chunks \
  --auto-unmount \
  --foreground
```

**Solution C - Manual cleanup (if process crashes):**
```bash
# Clean up stale mount manually
fusermount -u /tmp/wormfs-mount  # Linux
umount /tmp/wormfs-mount         # macOS

# Or use the demo script which handles cleanup automatically
```

---

## 📚 Documentation

- [Design Overview](docs/design.md)
- [Phase 1 Implementation Plan](docs/implementation_plan/phase1_minimal_data_path.md)
- [Component Specifications](docs/components/)
- [FUSE Quickstart](docs/fuse_quickstart.md)
- [POSIX Compliance](docs/posix_compliance.md)

---

## 🤝 Contributing

This project is currently in active development as a learning exercise. While it's not yet ready for external contributions, you're welcome to:

- Open issues for bugs or suggestions
- Star the repository if you find it interesting
- Follow along with the development progress

---

## 📄 License

Apache-2.0

---

## 🙏 Acknowledgments

- Inspired by [LizardFS](https://lizardfs.com/) for its architectural simplicity
- Built using [OpenRaft](https://github.com/datafuselabs/openraft) (Phase 2+)
- FUSE integration via [fuser](https://github.com/cberner/fuser)
- Erasure coding with [reed-solomon-erasure](https://github.com/darrenldl/reed-solomon-erasure)

---

**Note:** This is a work-in-progress learning project. I'm using Claude (AI assistant) to help me learn how best to integrate GenAI Tools into my SDLC workflow. Expect rough edges and evolving architecture as the project matures!
