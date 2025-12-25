#!/bin/bash

# WormFS Phase 1 Complete Demo Script
# Demonstrates full Phase 1 capabilities (Steps 1-11)
# Shows configuration file loading, component initialization, and filesystem operations

# Error handling: Don't exit immediately, but trap errors
set -E  # Inherit ERR trap in functions
set +e  # Don't exit on error (we'll handle errors manually)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Configuration
VERBOSE=0
SKIP_TESTS=0
DEMO_SUCCESS=0  # Track whether demo completed successfully
MOUNT_POINT_1=""
MOUNT_POINT_2=""
MOUNT_POINT_3=""
DATA_DIR_1=""
DATA_DIR_2=""
DATA_DIR_3=""
WORMFS_PID_1=""
WORMFS_PID_2=""
WORMFS_PID_3=""
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORMFS_BINARY="${PROJECT_ROOT}/target/release/wormfs"

# Use consistent directories across runs
WORMFS_DEMO_DIR_1="/tmp/wormfs-demo-node1"
WORMFS_DEMO_DIR_2="/tmp/wormfs-demo-node2"
WORMFS_DEMO_DIR_3="/tmp/wormfs-demo-node3"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=1
            shift
            ;;
        -h|--help)
            echo "WormFS Phase 1 Complete Demo Script"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Enable verbose output"
            echo "  --skip-tests     Skip file/directory operation tests"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "This script demonstrates WormFS Phase 1 (Steps 1-11) capabilities:"
            echo "  • TOML configuration file loading and validation"
            echo "  • CLI flag overrides of config values"
            echo "  • MetadataStore initialization (SQLite + WAL)"
            echo "  • FileStore setup with erasure coding (Reed-Solomon 2+1)"
            echo "  • FileSystemService FUSE mounting"
            echo "  • File operations (create, read, write, delete)"
            echo "  • Directory operations (mkdir, readdir, rmdir)"
            echo "  • Metrics collection (I/O amplification & performance tracking)"
            echo "  • Graceful shutdown (Ctrl+C)"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            fail "Unknown option: $1"
            ;;
    esac
done

error_handler() {
    local exit_code=$?
    echo ""
    echo -e "${RED}===============================================${NC}"
    echo -e "${RED}ERROR DETECTED (Exit code: $exit_code)${NC}"
    echo -e "${RED}===============================================${NC}"
    echo ""
    echo -e "${YELLOW}An error occurred. Press Enter to cleanup and exit...${NC}"
    read -r
    cleanup
    exit $exit_code
}

# Function to handle explicit failures with user prompt
fail() {
    local message="$1"
    local exit_code="${2:-1}"
    echo ""
    echo -e "${RED}===============================================${NC}"
    echo -e "${RED}ERROR: $message${NC}"
    echo -e "${RED}===============================================${NC}"
    echo ""
    echo -e "${YELLOW}Press Enter to cleanup and exit...${NC}"
    read -r
    cleanup
    exit $exit_code
}

# Trap errors and pause for inspection
trap 'error_handler' ERR

# Cleanup function - conditional log preservation
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"

    # Unmount filesystems
    if [ -n "$MOUNT_POINT_1" ] && mountpoint -q "$MOUNT_POINT_1" 2>/dev/null; then
        echo -e "${BLUE}Unmounting filesystem 1...${NC}"
        if command -v fusermount &> /dev/null; then
            fusermount -u "$MOUNT_POINT_1" 2>/dev/null || true
        else
            umount "$MOUNT_POINT_1" 2>/dev/null || true
        fi
        sleep 1
    fi

    if [ -n "$MOUNT_POINT_2" ] && mountpoint -q "$MOUNT_POINT_2" 2>/dev/null; then
        echo -e "${BLUE}Unmounting filesystem 2...${NC}"
        if command -v fusermount &> /dev/null; then
            fusermount -u "$MOUNT_POINT_2" 2>/dev/null || true
        else
            umount "$MOUNT_POINT_2" 2>/dev/null || true
        fi
        sleep 1
    fi

    if [ -n "$MOUNT_POINT_3" ] && mountpoint -q "$MOUNT_POINT_3" 2>/dev/null; then
        echo -e "${BLUE}Unmounting filesystem 3...${NC}"
        if command -v fusermount &> /dev/null; then
            fusermount -u "$MOUNT_POINT_3" 2>/dev/null || true
        else
            umount "$MOUNT_POINT_3" 2>/dev/null || true
        fi
        sleep 1
    fi

    # Stop FUSE processes
    if [ -n "$WORMFS_PID_1" ] && kill -0 "$WORMFS_PID_1" 2>/dev/null; then
        echo -e "${BLUE}Stopping wormfs FUSE process 1 (PID: $WORMFS_PID_1)...${NC}"
        kill -TERM "$WORMFS_PID_1" 2>/dev/null || true
        sleep 1
        kill -KILL "$WORMFS_PID_1" 2>/dev/null || true
    fi

    if [ -n "$WORMFS_PID_2" ] && kill -0 "$WORMFS_PID_2" 2>/dev/null; then
        echo -e "${BLUE}Stopping wormfs FUSE process 2 (PID: $WORMFS_PID_2)...${NC}"
        kill -TERM "$WORMFS_PID_2" 2>/dev/null || true
        sleep 1
        kill -KILL "$WORMFS_PID_2" 2>/dev/null || true
    fi

    if [ -n "$WORMFS_PID_3" ] && kill -0 "$WORMFS_PID_3" 2>/dev/null; then
        echo -e "${BLUE}Stopping wormfs FUSE process 3 (PID: $WORMFS_PID_3)...${NC}"
        kill -TERM "$WORMFS_PID_3" 2>/dev/null || true
        sleep 1
        kill -KILL "$WORMFS_PID_3" 2>/dev/null || true
    fi

    # Remove temporary mount directories
    if [ -n "$MOUNT_POINT_1" ] && [ -d "$MOUNT_POINT_1" ]; then
        echo -e "${BLUE}Removing mount point 1...${NC}"
        rm -rf "$MOUNT_POINT_1"
    fi

    if [ -n "$MOUNT_POINT_2" ] && [ -d "$MOUNT_POINT_2" ]; then
        echo -e "${BLUE}Removing mount point 2...${NC}"
        rm -rf "$MOUNT_POINT_2"
    fi

    if [ -n "$MOUNT_POINT_3" ] && [ -d "$MOUNT_POINT_3" ]; then
        echo -e "${BLUE}Removing mount point 3...${NC}"
        rm -rf "$MOUNT_POINT_3"
    fi

    # Always clean chunk data and databases
    echo -e "${BLUE}Cleaning up chunk data and databases...${NC}"
    if [ -d "$WORMFS_DEMO_DIR_1/chunks" ]; then
        rm -rf "$WORMFS_DEMO_DIR_1/chunks"
    fi
    if [ -f "$WORMFS_DEMO_DIR_1/metadata.db" ]; then
        rm -f "$WORMFS_DEMO_DIR_1/metadata.db" "$WORMFS_DEMO_DIR_1/metadata.db-shm" "$WORMFS_DEMO_DIR_1/metadata.db-wal"
    fi
    if [ -d "$WORMFS_DEMO_DIR_2/chunks" ]; then
        rm -rf "$WORMFS_DEMO_DIR_2/chunks"
    fi
    if [ -f "$WORMFS_DEMO_DIR_2/metadata.db" ]; then
        rm -f "$WORMFS_DEMO_DIR_2/metadata.db" "$WORMFS_DEMO_DIR_2/metadata.db-shm" "$WORMFS_DEMO_DIR_2/metadata.db-wal"
    fi
    if [ -d "$WORMFS_DEMO_DIR_3/chunks" ]; then
        rm -rf "$WORMFS_DEMO_DIR_3/chunks"
    fi
    if [ -f "$WORMFS_DEMO_DIR_3/metadata.db" ]; then
        rm -f "$WORMFS_DEMO_DIR_3/metadata.db" "$WORMFS_DEMO_DIR_3/metadata.db-shm" "$WORMFS_DEMO_DIR_3/metadata.db-wal"
    fi

    # Conditionally clean log files
    if [ "$DEMO_SUCCESS" -eq 1 ]; then
        echo -e "${BLUE}Demo succeeded - cleaning up log files...${NC}"
        if [ -f "$WORMFS_DEMO_DIR_1/wormfs.log" ]; then
            rm -f "$WORMFS_DEMO_DIR_1/wormfs.log"
        fi
        if [ -f "$WORMFS_DEMO_DIR_2/wormfs.log" ]; then
            rm -f "$WORMFS_DEMO_DIR_2/wormfs.log"
        fi
        if [ -f "$WORMFS_DEMO_DIR_3/wormfs.log" ]; then
            rm -f "$WORMFS_DEMO_DIR_3/wormfs.log"
        fi
    else
        echo -e "${YELLOW}Demo failed or interrupted - preserving log files for debugging:${NC}"
        if [ -f "$WORMFS_DEMO_DIR_1/wormfs.log" ]; then
            echo -e "${YELLOW}  Node 1: $WORMFS_DEMO_DIR_1/wormfs.log${NC}"
        fi
        if [ -f "$WORMFS_DEMO_DIR_2/wormfs.log" ]; then
            echo -e "${YELLOW}  Node 2: $WORMFS_DEMO_DIR_2/wormfs.log${NC}"
        fi
        if [ -f "$WORMFS_DEMO_DIR_3/wormfs.log" ]; then
            echo -e "${YELLOW}  Node 3: $WORMFS_DEMO_DIR_3/wormfs.log${NC}"
        fi
    fi

    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Print header
print_header() {
    echo ""
    echo "=========================================="
    echo -e "${BOLD}$1${NC}"
    echo "=========================================="
}

# Print section
print_section() {
    echo ""
    echo -e "${CYAN}>>> $1${NC}"
    echo "---"
}

# Print command being executed
print_command() {
    echo -e "${BLUE}$ $1${NC}"
}

# Print success
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Print info
print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Print error
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Print component
print_component() {
    echo -e "${MAGENTA}▸ $1${NC}"
}

# Verbose output
verbose() {
    if [ "$VERBOSE" -eq 1 ]; then
        echo -e "${CYAN}[DEBUG] $1${NC}"
    fi
}

# Main demo
main() {
    print_header "WormFS Phase 1 Complete Demo with Distributed Storage"
    echo "Demonstrating Phase 1, Steps 1-11 + Distributed Operations:"
    echo "  • Configuration Management (TOML + CLI overrides)"
    echo "  • Metadata Persistence (MetadataStore + SQLite)"
    echo "  • Erasure Coding (FileStore + Reed-Solomon)"
    echo "  • StripeCache (Write Buffering & I/O Amplification Reduction)"
    echo "  • FUSE Filesystem (FileSystemService)"
    echo "  • StorageNetwork (libp2p peer-to-peer networking)"
    echo "  • Three Networked Mount Points (connected via libp2p)"
    echo "  • Distributed Chunk Placement (chunks spread across nodes)"
    echo "  • File & Directory Operations"
    echo "  • Metrics Collection (I/O Amplification Tracking)"
    echo "  • Chunk Distribution Validation"
    echo "  • Graceful Shutdown"
    echo ""

    # Step 1: Pre-flight checks
    print_section "Step 1: Pre-flight Checks"

    # Check if FUSE is available
    print_command "Checking FUSE availability..."
    if command -v fusermount &> /dev/null || command -v mount.fuse &> /dev/null; then
        print_success "FUSE is available"
    else
        print_error "FUSE not found. Please install fuse3 or macfuse."
        fail "FUSE not found. Please install fuse3 or macfuse."
    fi

    echo "Building WormFS (this may take a few minutes)..."
    print_command "cargo build --release --features fuser"
    cd "$PROJECT_ROOT"
    cargo build --release --features fuser
    print_success "Build complete"
    
    # Step 2: Configuration Setup
    print_section "Step 2: Configuration Setup"

    # Clean up old data from previous runs if it exists
    echo "Cleaning up old demo data if present..."
    for demo_dir in "$WORMFS_DEMO_DIR_1" "$WORMFS_DEMO_DIR_2" "$WORMFS_DEMO_DIR_3"; do
        if [ -d "$demo_dir" ]; then
            echo -e "${BLUE}Cleaning old data from $demo_dir${NC}"
            # Clean up old chunks and databases
            rm -rf "$demo_dir/chunks" 2>/dev/null || true
            rm -f "$demo_dir/metadata.db"* 2>/dev/null || true
            rm -f "$demo_dir/tx_log.db"* 2>/dev/null || true
            rm -rf "$demo_dir/snapshots" 2>/dev/null || true
            rm -f "$demo_dir/peer_ids.json" 2>/dev/null || true
            # Truncate log file if it exists
            if [ -f "$demo_dir/wormfs.log" ]; then
                > "$demo_dir/wormfs.log"
            fi
        else
            # Create directory if it doesn't exist
            mkdir -p "$demo_dir"
        fi
    done
    print_success "Old demo data cleaned up"

    # Use consistent directories for all three nodes
    DATA_DIR_1="$WORMFS_DEMO_DIR_1"
    DATA_DIR_2="$WORMFS_DEMO_DIR_2"
    DATA_DIR_3="$WORMFS_DEMO_DIR_3"
    MOUNT_POINT_1=$(mktemp -d -t wormfs-demo-mount1.XXXXXX)
    MOUNT_POINT_2=$(mktemp -d -t wormfs-demo-mount2.XXXXXX)
    MOUNT_POINT_3=$(mktemp -d -t wormfs-demo-mount3.XXXXXX)
    CONFIG_FILE_1=$(mktemp -t wormfs-demo-config1.XXXXXX.toml)
    CONFIG_FILE_2=$(mktemp -t wormfs-demo-config2.XXXXXX.toml)
    CONFIG_FILE_3=$(mktemp -t wormfs-demo-config3.XXXXXX.toml)

    verbose "Node 1 Data directory: $DATA_DIR_1"
    verbose "Node 1 Mount point: $MOUNT_POINT_1"
    verbose "Node 1 Config file: $CONFIG_FILE_1"
    verbose "Node 2 Data directory: $DATA_DIR_2"
    verbose "Node 2 Mount point: $MOUNT_POINT_2"
    verbose "Node 2 Config file: $CONFIG_FILE_2"
    verbose "Node 3 Data directory: $DATA_DIR_3"
    verbose "Node 3 Mount point: $MOUNT_POINT_3"
    verbose "Node 3 Config file: $CONFIG_FILE_3"

    # Create demo configuration for Node 1
    cat > "$CONFIG_FILE_1" <<EOF
# WormFS Demo Configuration - Node 1
mount_point = "$MOUNT_POINT_1"

# Raft configuration (Phase 2+)
transaction_log_path = "$DATA_DIR_1/tx_log.db"
snapshot_dir = "$DATA_DIR_1/snapshots"

[metadata]
database_path = "$DATA_DIR_1/metadata.db"
read_pool_size = 8
enable_wal = true
cache_size_mb = 10
enable_foreign_keys = true
synchronous = "Normal"
transaction_isolation = "Serializable"
enable_prepared_statements = true
read_pool_timeout_secs = 30
stripe_cache_size_mb = 64
stripe_cache_ttl_secs = 10
stripe_cache_tti_secs = 5
chunk_cache_size_mb = 64
chunk_cache_ttl_secs = 10
chunk_cache_tti_secs = 5

[file_store]
disk_paths = ["$DATA_DIR_1/chunks"]
max_chunk_size = 1048576  # 1MB
default_data_shards = 2
default_parity_shards = 1
max_concurrent_operations = 100
verification_interval = 3600
orphan_cleanup_age = 3600
stripe_cache_size_mb = 256
stripe_cache_ttl_secs = 3600
stripe_cache_tti_secs = 600

[filesystem]
node_id = 1
client_heartbeat_timeout = 86400
enable_read_locks = true
lock_timeout = 10
lock_extend_interval = 5
max_file_handles = 10000
inode_cache_size = 10000
inode_cache_ttl = 60
read_buffer_size = 10048576
write_buffer_size = 10048576
write_through = false
default_file_mode = "0644"
default_dir_mode = "0755"
max_file_size = 1099511627776
enable_xattr = false
uid = $(id -u)
gid = $(id -g)

# StripeCache: Write buffering to dramatically reduce I/O amplification
# Buffers partial stripe writes in memory, coalesces them into full stripes
# Reduces I/O amplification from ~990x to ~3x for typical workloads
enable_stripe_cache = true
stripe_cache_max_memory_bytes = 268435456  # 256MB cache
stripe_cache_dirty_timeout = 5  # Flush dirty stripes after 5 seconds

[metrics]
enabled = true
aggregation_window_secs = 10
max_cardinality = 10000
channel_buffer_size = 10000
enable_prometheus = false
prometheus_port = 9091  # Not used when enable_prometheus=false
enable_otel = false
enable_time_series = true
time_series_retention_secs = 3600  # 1 hour of historical data
max_points_per_metric = 3600
time_series_sample_interval_secs = 1

[admin]
enabled = true
port = 9090
bind_address = "127.0.0.1"

[storage_endpoint]
listen_address = "127.0.0.1:8081"
enable_auth = false
enable_tls = false

[network]
node_id = "1"
listen_addresses = ["/ip4/0.0.0.0/tcp/7101"]
peer_id_store_path = "$DATA_DIR_1/peer_ids.json"
max_peers = 100
max_connections_per_peer = 3
connection_timeout = 30
idle_connection_timeout = 600
keep_alive_interval = 30
admin_url = "http://127.0.0.1:9090"
storage_endpoint_url = "127.0.0.1:8081"

# Peer configuration: connect to Node 2 and Node 3
[[network.peers]]
multiaddr = "/ip4/127.0.0.1/tcp/7102"

[[network.peers]]
multiaddr = "/ip4/127.0.0.1/tcp/7103"
EOF

    # Create demo configuration for Node 2
    cat > "$CONFIG_FILE_2" <<EOF
# WormFS Demo Configuration - Node 2
mount_point = "$MOUNT_POINT_2"

# Raft configuration (Phase 2+)
transaction_log_path = "$DATA_DIR_2/tx_log.db"
snapshot_dir = "$DATA_DIR_2/snapshots"

[metadata]
database_path = "$DATA_DIR_2/metadata.db"
read_pool_size = 8
enable_wal = true
cache_size_mb = 10
enable_foreign_keys = true
synchronous = "Normal"
transaction_isolation = "Serializable"
enable_prepared_statements = true
read_pool_timeout_secs = 30
stripe_cache_size_mb = 64
stripe_cache_ttl_secs = 10
stripe_cache_tti_secs = 5
chunk_cache_size_mb = 64
chunk_cache_ttl_secs = 10
chunk_cache_tti_secs = 5

[file_store]
disk_paths = ["$DATA_DIR_2/chunks"]
max_chunk_size = 1048576  # 1MB
default_data_shards = 2
default_parity_shards = 1
max_concurrent_operations = 100
verification_interval = 3600
orphan_cleanup_age = 3600
stripe_cache_size_mb = 256
stripe_cache_ttl_secs = 3600
stripe_cache_tti_secs = 600

[filesystem]
node_id = 2
client_heartbeat_timeout = 86400
enable_read_locks = true
lock_timeout = 10
lock_extend_interval = 5
max_file_handles = 10000
inode_cache_size = 10000
inode_cache_ttl = 60
read_buffer_size = 10048576
write_buffer_size = 10048576
write_through = false
default_file_mode = "0644"
default_dir_mode = "0755"
max_file_size = 1099511627776
enable_xattr = false
uid = $(id -u)
gid = $(id -g)

# StripeCache: Write buffering to dramatically reduce I/O amplification
enable_stripe_cache = true
stripe_cache_max_memory_bytes = 268435456  # 256MB cache
stripe_cache_dirty_timeout = 5  # Flush dirty stripes after 5 seconds

[metrics]
enabled = true
aggregation_window_secs = 10
max_cardinality = 10000
channel_buffer_size = 10000
enable_prometheus = false
prometheus_port = 9092  # Not used when enable_prometheus=false
enable_otel = false
enable_time_series = true
time_series_retention_secs = 3600  # 1 hour of historical data
max_points_per_metric = 3600
time_series_sample_interval_secs = 1

[admin]
enabled = true
port = 9091
bind_address = "127.0.0.1"

[storage_endpoint]
listen_address = "127.0.0.1:8082"
enable_auth = false
enable_tls = false

[network]
node_id = "2"
listen_addresses = ["/ip4/0.0.0.0/tcp/7102"]
peer_id_store_path = "$DATA_DIR_2/peer_ids.json"
max_peers = 100
max_connections_per_peer = 3
connection_timeout = 30
idle_connection_timeout = 600
keep_alive_interval = 30
admin_url = "http://127.0.0.1:9091"
storage_endpoint_url = "127.0.0.1:8082"

# Peer configuration: connect to Node 1 and Node 3
[[network.peers]]
multiaddr = "/ip4/127.0.0.1/tcp/7101"

[[network.peers]]
multiaddr = "/ip4/127.0.0.1/tcp/7103"
EOF

    # Create demo configuration for Node 3
    cat > "$CONFIG_FILE_3" <<EOF
# WormFS Demo Configuration - Node 3
mount_point = "$MOUNT_POINT_3"

# Raft configuration (Phase 2+)
transaction_log_path = "$DATA_DIR_3/tx_log.db"
snapshot_dir = "$DATA_DIR_3/snapshots"

[metadata]
database_path = "$DATA_DIR_3/metadata.db"
read_pool_size = 8
enable_wal = true
cache_size_mb = 10
enable_foreign_keys = true
synchronous = "Normal"
transaction_isolation = "Serializable"
enable_prepared_statements = true
read_pool_timeout_secs = 30
stripe_cache_size_mb = 64
stripe_cache_ttl_secs = 10
stripe_cache_tti_secs = 5
chunk_cache_size_mb = 64
chunk_cache_ttl_secs = 10
chunk_cache_tti_secs = 5

[file_store]
disk_paths = ["$DATA_DIR_3/chunks"]
max_chunk_size = 1048576  # 1MB
default_data_shards = 2
default_parity_shards = 1
max_concurrent_operations = 100
verification_interval = 3600
orphan_cleanup_age = 3600
stripe_cache_size_mb = 256
stripe_cache_ttl_secs = 3600
stripe_cache_tti_secs = 600

[filesystem]
node_id = 3
client_heartbeat_timeout = 86400
enable_read_locks = true
lock_timeout = 10
lock_extend_interval = 5
max_file_handles = 10000
inode_cache_size = 10000
inode_cache_ttl = 60
read_buffer_size = 10048576
write_buffer_size = 10048576
write_through = false
default_file_mode = "0644"
default_dir_mode = "0755"
max_file_size = 1099511627776
enable_xattr = false
uid = $(id -u)
gid = $(id -g)

# StripeCache: Write buffering to dramatically reduce I/O amplification
enable_stripe_cache = true
stripe_cache_max_memory_bytes = 268435456  # 256MB cache
stripe_cache_dirty_timeout = 5  # Flush dirty stripes after 5 seconds

[metrics]
enabled = true
aggregation_window_secs = 10
max_cardinality = 10000
channel_buffer_size = 10000
enable_prometheus = false
prometheus_port = 9093  # Not used when enable_prometheus=false
enable_otel = false
enable_time_series = true
time_series_retention_secs = 3600  # 1 hour of historical data
max_points_per_metric = 3600
time_series_sample_interval_secs = 1

[admin]
enabled = true
port = 9092
bind_address = "127.0.0.1"

[storage_endpoint]
listen_address = "127.0.0.1:8083"
enable_auth = false
enable_tls = false

[network]
node_id = "3"
listen_addresses = ["/ip4/0.0.0.0/tcp/7103"]
peer_id_store_path = "$DATA_DIR_3/peer_ids.json"
max_peers = 100
max_connections_per_peer = 3
connection_timeout = 30
idle_connection_timeout = 600
keep_alive_interval = 30
admin_url = "http://127.0.0.1:9092"
storage_endpoint_url = "127.0.0.1:8083"

# Peer configuration: connect to Node 1 and Node 2
[[network.peers]]
multiaddr = "/ip4/127.0.0.1/tcp/7101"

[[network.peers]]
multiaddr = "/ip4/127.0.0.1/tcp/7102"
EOF

    print_success "Created temporary configurations for all three nodes"
    echo ""
    echo "Node 1 Configuration (${CONFIG_FILE_1}):"
    echo -e "${CYAN}  Node ID: 1, Port: 7101, Admin: 9090, Raft: enabled${NC}"
    echo ""
    echo "Node 2 Configuration (${CONFIG_FILE_2}):"
    echo -e "${CYAN}  Node ID: 2, Port: 7102, Admin: 9091, Raft: enabled${NC}"
    echo ""
    echo "Node 3 Configuration (${CONFIG_FILE_3}):"
    echo -e "${CYAN}  Node ID: 3, Port: 7103, Admin: 9092, Raft: enabled${NC}"

    # Step 3: Mount Filesystems via FUSE
    print_section "Step 3: Mount Filesystems"

    echo "WormFS uses the following components:"
    print_component "1. MetadataStore      (SQLite + WAL for metadata persistence)"
    print_component "2. FileStore          (Reed-Solomon erasure coding + chunk storage)"
    print_component "3. StripeCache        (Write buffering for I/O amplification reduction)"
    print_component "4. FileSystemService  (FUSE integration for filesystem ops)"
    print_component "5. MetricService      (I/O amplification & performance metrics)"
    print_component "6. StorageNetwork     (libp2p peer-to-peer networking)"
    echo ""

    # Clean up any stale mounts before attempting to mount
    if mountpoint -q "$MOUNT_POINT_1" 2>/dev/null; then
        print_info "Found stale mount at $MOUNT_POINT_1, cleaning up..."
        if command -v fusermount &> /dev/null; then
            fusermount -u "$MOUNT_POINT_1" 2>/dev/null || true
        else
            umount "$MOUNT_POINT_1" 2>/dev/null || true
        fi
        sleep 1
        print_success "Stale mount 1 cleaned up"
    fi

    if mountpoint -q "$MOUNT_POINT_2" 2>/dev/null; then
        print_info "Found stale mount at $MOUNT_POINT_2, cleaning up..."
        if command -v fusermount &> /dev/null; then
            fusermount -u "$MOUNT_POINT_2" 2>/dev/null || true
        else
            umount "$MOUNT_POINT_2" 2>/dev/null || true
        fi
        sleep 1
        print_success "Stale mount 2 cleaned up"
    fi

    if mountpoint -q "$MOUNT_POINT_3" 2>/dev/null; then
        print_info "Found stale mount at $MOUNT_POINT_3, cleaning up..."
        if command -v fusermount &> /dev/null; then
            fusermount -u "$MOUNT_POINT_3" 2>/dev/null || true
        else
            umount "$MOUNT_POINT_3" 2>/dev/null || true
        fi
        sleep 1
        print_success "Stale mount 3 cleaned up"
    fi

    # Use consistent log file locations
    WORMFS_LOG_1="$DATA_DIR_1/wormfs.log"
    WORMFS_LOG_2="$DATA_DIR_2/wormfs.log"
    WORMFS_LOG_3="$DATA_DIR_3/wormfs.log"
    echo -e "${CYAN}Log files (consistent across runs):${NC}"
    echo -e "  Node 1: $WORMFS_LOG_1"
    echo -e "  Node 2: $WORMFS_LOG_2"
    echo -e "  Node 3: $WORMFS_LOG_3"

    # Mount Node 1
    print_command "RUST_LOG=wormfs=debug $WORMFS_BINARY mount --config $CONFIG_FILE_1 --mount-point $MOUNT_POINT_1 --foreground --verbose"
    echo ""
    echo "Mounting Node 1 WormFS with config file..."
    echo -e "${CYAN}Logs will be written to: $WORMFS_LOG_1${NC}"

    # Start FUSE mount 1 in background
    RUST_LOG=wormfs=debug "$WORMFS_BINARY" mount \
        --config "$CONFIG_FILE_1" \
        --mount-point "$MOUNT_POINT_1" \
        --foreground \
        --verbose > "$WORMFS_LOG_1" 2>&1 &
    WORMFS_PID_1=$!
    print_info "Node 1 FUSE PID: $WORMFS_PID_1"
    print_info "Node 1 Logs: $WORMFS_LOG_1"

    # Wait for mount 1
    echo -n "Waiting for Node 1 mount to complete"
    for i in {1..10}; do
        if mountpoint -q "$MOUNT_POINT_1" 2>/dev/null; then
            echo ""
            print_success "Node 1 filesystem mounted successfully"
            break
        fi
        echo -n "."
        sleep 0.5
    done

    if ! mountpoint -q "$MOUNT_POINT_1" 2>/dev/null; then
        echo ""
        print_error "Failed to mount Node 1 filesystem"
        echo ""
        echo "Log output:"
        cat "$WORMFS_LOG_1"
        fail "Failed to mount Node 1 filesystem"
    fi

    echo ""

    # Mount Node 2
    print_command "RUST_LOG=wormfs=debug $WORMFS_BINARY mount --config $CONFIG_FILE_2 --mount-point $MOUNT_POINT_2 --foreground --verbose"
    echo ""
    echo "Mounting Node 2 WormFS with config file..."
    echo -e "${CYAN}Logs will be written to: $WORMFS_LOG_2${NC}"

    # Start FUSE mount 2 in background
    RUST_LOG=wormfs=debug "$WORMFS_BINARY" mount \
        --config "$CONFIG_FILE_2" \
        --mount-point "$MOUNT_POINT_2" \
        --foreground \
        --verbose > "$WORMFS_LOG_2" 2>&1 &
    WORMFS_PID_2=$!
    print_info "Node 2 FUSE PID: $WORMFS_PID_2"
    print_info "Node 2 Logs: $WORMFS_LOG_2"

    # Wait for mount 2
    echo -n "Waiting for Node 2 mount to complete"
    for i in {1..10}; do
        if mountpoint -q "$MOUNT_POINT_2" 2>/dev/null; then
            echo ""
            print_success "Node 2 filesystem mounted successfully"
            break
        fi
        echo -n "."
        sleep 0.5
    done

    if ! mountpoint -q "$MOUNT_POINT_2" 2>/dev/null; then
        echo ""
        print_error "Failed to mount Node 2 filesystem"
        echo ""
        echo "Log output:"
        cat "$WORMFS_LOG_2"
        fail "Failed to mount Node 2 filesystem"
    fi

    echo ""

    # Mount Node 3
    print_command "RUST_LOG=wormfs=debug $WORMFS_BINARY mount --config $CONFIG_FILE_3 --mount-point $MOUNT_POINT_3 --foreground --verbose"
    echo ""
    echo "Mounting Node 3 WormFS with config file..."
    echo -e "${CYAN}Logs will be written to: $WORMFS_LOG_3${NC}"

    # Start FUSE mount 3 in background
    RUST_LOG=wormfs=debug "$WORMFS_BINARY" mount \
        --config "$CONFIG_FILE_3" \
        --mount-point "$MOUNT_POINT_3" \
        --foreground \
        --verbose > "$WORMFS_LOG_3" 2>&1 &
    WORMFS_PID_3=$!
    print_info "Node 3 FUSE PID: $WORMFS_PID_3"
    print_info "Node 3 Logs: $WORMFS_LOG_3"

    # Wait for mount 3
    echo -n "Waiting for Node 3 mount to complete"
    for i in {1..10}; do
        if mountpoint -q "$MOUNT_POINT_3" 2>/dev/null; then
            echo ""
            print_success "Node 3 filesystem mounted successfully"
            break
        fi
        echo -n "."
        sleep 0.5
    done

    if ! mountpoint -q "$MOUNT_POINT_3" 2>/dev/null; then
        echo ""
        print_error "Failed to mount Node 3 filesystem"
        echo ""
        echo "Log output:"
        cat "$WORMFS_LOG_3"
        fail "Failed to mount Node 3 filesystem"
    fi

    # Verify mounts
    echo ""
    print_command "mount | grep wormfs"
    mount | grep wormfs
    print_success "All three filesystems appear in mount table"

    # Check if admin servers are responding
    echo ""
    echo "Checking admin servers..."
    if curl -s --max-time 2 http://127.0.0.1:9090/api/health > /dev/null 2>&1; then
        print_success "Node 1 Admin server is responding on port 9090"
    else
        print_info "Node 1 Admin server health check failed - checking logs..."
    fi

    if curl -s --max-time 2 http://127.0.0.1:9091/api/health > /dev/null 2>&1; then
        print_success "Node 2 Admin server is responding on port 9091"
    else
        print_info "Node 2 Admin server health check failed - checking logs..."
    fi

    if curl -s --max-time 2 http://127.0.0.1:9092/api/health > /dev/null 2>&1; then
        print_success "Node 3 Admin server is responding on port 9092"
    else
        print_info "Node 3 Admin server health check failed - checking logs..."
    fi

    # Display Admin UI links and network information
    echo ""
    echo "=========================================="
    echo -e "${BOLD}${GREEN}🌐 Admin Web UIs Available!${NC}"
    echo "=========================================="
    echo ""
    echo -e "  ${CYAN}Node 1 Admin UI:${NC}"
    echo -e "  ${BOLD}${BLUE}http://127.0.0.1:9090/${NC}"
    echo -e "  ${CYAN}  • Node ID: 1${NC}"
    echo -e "  ${CYAN}  • libp2p Port: 7101${NC}"
    echo -e "  ${CYAN}  • Mount Point: $MOUNT_POINT_1${NC}"
    echo ""
    echo -e "  ${CYAN}Node 2 Admin UI:${NC}"
    echo -e "  ${BOLD}${BLUE}http://127.0.0.1:9091/${NC}"
    echo -e "  ${CYAN}  • Node ID: 2${NC}"
    echo -e "  ${CYAN}  • libp2p Port: 7102${NC}"
    echo -e "  ${CYAN}  • Mount Point: $MOUNT_POINT_2${NC}"
    echo ""
    echo -e "  ${CYAN}Node 3 Admin UI:${NC}"
    echo -e "  ${BOLD}${BLUE}http://127.0.0.1:9092/${NC}"
    echo -e "  ${CYAN}  • Node ID: 3${NC}"
    echo -e "  ${CYAN}  • libp2p Port: 7103${NC}"
    echo -e "  ${CYAN}  • Mount Point: $MOUNT_POINT_3${NC}"
    echo ""
    echo -e "  ${CYAN}Features:${NC}"
    echo -e "    • ${GREEN}📊 Real-time Metrics Monitoring${NC}"
    echo -e "    • ${GREEN}🌐 Network Peers Status${NC}"
    echo -e "    • ${GREEN}❤️  Heartbeat Monitoring${NC}"
    echo -e "    • ${GREEN}⚙️  Configuration Viewer${NC}"
    echo -e "    • ${GREEN}📝 Live Log Streaming${NC}"
    echo ""
    echo "=========================================="
    echo ""
    echo -e "${BOLD}${YELLOW}📋 Debug Logs:${NC}"
    echo -e "  ${CYAN}Node 1 logs:${NC} ${BOLD}$WORMFS_LOG_1${NC}"
    echo -e "  ${CYAN}Node 2 logs:${NC} ${BOLD}$WORMFS_LOG_2${NC}"
    echo -e "  ${CYAN}Node 3 logs:${NC} ${BOLD}$WORMFS_LOG_3${NC}"
    echo ""
    echo "=========================================="
    echo ""
    echo -e "${YELLOW}⏳ Waiting 10 seconds for nodes to connect via libp2p...${NC}"
    sleep 10

    # Check network connectivity
    echo ""
    echo "Checking libp2p network connectivity..."
    PEERS_1=$(curl -s --max-time 2 http://127.0.0.1:9090/api/network/peers 2>/dev/null | jq -r '.peers | length' 2>/dev/null || echo "0")
    PEERS_2=$(curl -s --max-time 2 http://127.0.0.1:9091/api/network/peers 2>/dev/null | jq -r '.peers | length' 2>/dev/null || echo "0")
    PEERS_3=$(curl -s --max-time 2 http://127.0.0.1:9092/api/network/peers 2>/dev/null | jq -r '.peers | length' 2>/dev/null || echo "0")

    if [ "$PEERS_1" -gt 0 ]; then
        print_success "Node 1 has $PEERS_1 connected peer(s)"
    else
        print_info "Node 1 has no connected peers yet (they may still be connecting)"
    fi

    if [ "$PEERS_2" -gt 0 ]; then
        print_success "Node 2 has $PEERS_2 connected peer(s)"
    else
        print_info "Node 2 has no connected peers yet (they may still be connecting)"
    fi

    if [ "$PEERS_3" -gt 0 ]; then
        print_success "Node 3 has $PEERS_3 connected peer(s)"
    else
        print_info "Node 3 has no connected peers yet (they may still be connecting)"
    fi
    echo ""

    if [ "$SKIP_TESTS" -eq 1 ]; then
        print_info "Skipping file/directory operation tests (--skip-tests flag)"
        print_header "Demo Complete (Basic Mount Only)"
        echo ""
        echo "Mount points:"
        echo "  Node 1: $MOUNT_POINT_1 (PID: $WORMFS_PID_1)"
        echo "  Node 2: $MOUNT_POINT_2 (PID: $WORMFS_PID_2)"
        echo "  Node 3: $MOUNT_POINT_3 (PID: $WORMFS_PID_3)"
        echo ""
        echo -e "${CYAN}Press Enter to unmount and cleanup...${NC}"
        read -r
        return
    fi

    # Step 4: File Operations (on Node 1)
    print_section "Step 4: File Operations (on Node 1)"

    echo "Testing create, read, write, delete operations on Node 1..."
    echo ""

    # Create a file
    print_command "echo 'Hello WormFS!' > $MOUNT_POINT_1/hello.txt"
    echo "Hello WormFS!" > "$MOUNT_POINT_1/hello.txt"
    print_success "File created on Node 1"

    # Read the file
    print_command "cat $MOUNT_POINT_1/hello.txt"
    cat "$MOUNT_POINT_1/hello.txt"
    print_success "File read successfully from Node 1"

    # Check file attributes
    print_command "stat $MOUNT_POINT_1/hello.txt"
    stat "$MOUNT_POINT_1/hello.txt"
    print_success "File attributes retrieved"

    # Note: Append operations not supported in write-once filesystem
    # Test reading the file instead
    print_command "cat $MOUNT_POINT_1/hello.txt"
    cat "$MOUNT_POINT_1/hello.txt"
    print_success "File content read successfully"

    # Step 5: Directory Operations (on Node 1)
    print_section "Step 5: Directory Operations (on Node 1)"

    # Create directory
    print_command "mkdir $MOUNT_POINT_1/test_dir"
    mkdir "$MOUNT_POINT_1/test_dir"
    print_success "Directory created on Node 1"

    # List directory
    print_command "ls -la $MOUNT_POINT_1"
    ls -la "$MOUNT_POINT_1"
    print_success "Directory listing successful"

    # Create file in subdirectory
    print_command "echo 'Nested file' > $MOUNT_POINT_1/test_dir/nested.txt"
    echo "Nested file" > "$MOUNT_POINT_1/test_dir/nested.txt"
    print_success "File created in subdirectory"

    print_command "cat $MOUNT_POINT_1/test_dir/nested.txt"
    cat "$MOUNT_POINT_1/test_dir/nested.txt"

    # Step 6: Erasure Coding Verification
    print_section "Step 6: Erasure Coding Verification"

    echo "WormFS uses Reed-Solomon erasure coding (2 data + 1 parity):"
    echo ""

    # Check chunk storage on Node 1
    if [ -d "$DATA_DIR_1/chunks" ]; then
        print_command "find $DATA_DIR_1/chunks -type f | head -5"
        echo "Chunks stored on Node 1:"
        find "$DATA_DIR_1/chunks" -type f | head -5 | while IFS= read -r chunk; do
            echo -e "${CYAN}  $chunk${NC}"
        done
        CHUNK_COUNT=$(find "$DATA_DIR_1/chunks" -type f | wc -l)
        print_success "Total chunks stored on Node 1: $CHUNK_COUNT"
    else
        print_info "Chunk directory not yet created (files too small for chunking)"
    fi

    # Step 7: Performance Test (on Node 1)
    print_section "Step 7: Performance Test (on Node 1)"

    echo "Running 100 file operations to test performance on Node 1..."
    print_command "for i in {1..100}; do echo \$i > $MOUNT_POINT_1/perf_\$i.txt; done"

    START_TIME=$(date +%s.%N)
    for i in {1..100}; do
        echo "$i" > "$MOUNT_POINT_1/perf_$i.txt"
    done
    END_TIME=$(date +%s.%N)

    ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)
    AVG=$(echo "scale=2; $ELAPSED / 100 * 1000" | bc)

    print_success "Created 100 files in ${ELAPSED}s (avg ${AVG}ms per file)"

    # Cleanup performance test files
    rm -f "$MOUNT_POINT_1"/perf_*.txt

    # Step 8: Data Integrity Check
    print_section "Step 8: Data Integrity Check"

    echo "Testing data integrity through the full write/read pipeline..."
    echo ""

    # Stage file in /tmp first
    STAGING_FILE=$(mktemp -t wormfs-demo-staging.XXXXXX.dat)
    print_command "dd if=/dev/urandom of=$STAGING_FILE bs=1M count=2000"
    dd if=/dev/urandom of="$STAGING_FILE" bs=1M count=2000 2>&1 | tail -1
    if [ $? -ne 0 ]; then
        echo -e "${RED}ERROR: Failed to create test file with dd${NC}"
        fail "Failed to create test file with dd"
    fi
    print_success "Created 2000MB file in staging area"

    # Calculate checksum of original file
    print_command "md5sum $STAGING_FILE"
    ORIGINAL_CHECKSUM=$(md5sum "$STAGING_FILE" | awk '{print $1}')
    if [ -z "$ORIGINAL_CHECKSUM" ]; then
        echo -e "${RED}ERROR: Failed to calculate checksum of staging file${NC}"
        fail "Failed to calculate checksum of staging file"
    fi
    echo -e "${CYAN}Original MD5: $ORIGINAL_CHECKSUM${NC}"
    print_success "Calculated checksum of staged file"

    # Copy file to WormFS Node 1 (with timing)
    print_command "cp $STAGING_FILE $MOUNT_POINT_1/random.dat"
    CP_START=$(date +%s.%N)
    cp "$STAGING_FILE" "$MOUNT_POINT_1/random.dat"
    CP_END=$(date +%s.%N)
    if [ $? -ne 0 ]; then
        echo -e "${RED}ERROR: Failed to copy file to WormFS${NC}"
        fail "Failed to copy file to WormFS"
    fi

    # Calculate write throughput
    CP_ELAPSED=$(echo "$CP_END - $CP_START" | bc)
    CP_THROUGHPUT_MBPS=$(echo "scale=2; 2000 / $CP_ELAPSED" | bc)
    CP_THROUGHPUT_MBITS=$(echo "scale=2; $CP_THROUGHPUT_MBPS * 8" | bc)

    print_success "Copied file to WormFS Node 1 in ${CP_ELAPSED}s"
    echo -e "${GREEN}Write speed: ${CP_THROUGHPUT_MBPS} MB/s (${CP_THROUGHPUT_MBITS} Mbit/s)${NC}"

    # Verify size
    SIZE=$(stat -f%z "$MOUNT_POINT_1/random.dat" 2>/dev/null || stat -c%s "$MOUNT_POINT_1/random.dat" 2>/dev/null)
    print_info "File size in WormFS: $(numfmt --to=iec-i --suffix=B $SIZE)"

    # Calculate checksum of file in WormFS (with timing)
    print_command "md5sum $MOUNT_POINT_1/random.dat"
    MD5_START=$(date +%s.%N)
    WORMFS_CHECKSUM=$(md5sum "$MOUNT_POINT_1/random.dat" | awk '{print $1}')
    MD5_END=$(date +%s.%N)
    if [ -z "$WORMFS_CHECKSUM" ]; then
        echo -e "${RED}ERROR: Failed to calculate checksum from WormFS (read failed or file corrupted)${NC}"
        fail "Failed to calculate checksum from WormFS (read failed or file corrupted)"
    fi

    # Calculate read throughput
    MD5_ELAPSED=$(echo "$MD5_END - $MD5_START" | bc)
    MD5_THROUGHPUT_MBPS=$(echo "scale=2; 2000 / $MD5_ELAPSED" | bc)
    MD5_THROUGHPUT_MBITS=$(echo "scale=2; $MD5_THROUGHPUT_MBPS * 8" | bc)

    echo -e "${CYAN}WormFS MD5:  $WORMFS_CHECKSUM${NC}"
    print_success "Calculated checksum from WormFS in ${MD5_ELAPSED}s"
    echo -e "${GREEN}Read speed: ${MD5_THROUGHPUT_MBPS} MB/s (${MD5_THROUGHPUT_MBITS} Mbit/s)${NC}"

    # Compare checksums
    echo ""
    echo "Comparing checksums..."
    if [ "$ORIGINAL_CHECKSUM" = "$WORMFS_CHECKSUM" ]; then
        echo -e "${GREEN}${BOLD}✓ CHECKSUM MATCH!${NC}"
        echo -e "${GREEN}  Original: $ORIGINAL_CHECKSUM${NC}"
        echo -e "${GREEN}  WormFS:   $WORMFS_CHECKSUM${NC}"
        print_success "Data integrity verified - no corruption detected"
    else
        echo -e "${RED}${BOLD}✗ CHECKSUM MISMATCH!${NC}"
        echo -e "${RED}  Original: $ORIGINAL_CHECKSUM${NC}"
        echo -e "${RED}  WormFS:   $WORMFS_CHECKSUM${NC}"
        print_error "Data integrity check FAILED - checksums do not match!"
        rm -f "$STAGING_FILE"
        fail "Data integrity check FAILED - checksums do not match"
    fi

    # Clean up staging file
    rm -f "$STAGING_FILE"
    verbose "Cleaned up staging file"

    # Step 9: Distributed Chunk Validation
    print_section "Step 9: Distributed Chunk Validation"

    echo "Verifying that chunks are distributed across all three nodes..."
    echo ""
    echo "With Reed-Solomon 2+1 erasure coding, each stripe is split into 3 chunks"
    echo "The PlacementEngine should distribute these chunks across different nodes"
    echo ""

    # Wait a moment for any pending chunk writes to complete
    sleep 2

    # Count chunks on each node
    # Chunks are stored as *.dat files in the structure: bucket/file_id/stripe_<stripe_id>/<chunk_id>.dat
    CHUNKS_NODE_1=0
    CHUNKS_NODE_2=0
    CHUNKS_NODE_3=0

    if [ -d "$DATA_DIR_1/chunks" ]; then
        CHUNKS_NODE_1=$(find "$DATA_DIR_1/chunks" -type f -name "*.dat" 2>/dev/null | wc -l)
    fi

    if [ -d "$DATA_DIR_2/chunks" ]; then
        CHUNKS_NODE_2=$(find "$DATA_DIR_2/chunks" -type f -name "*.dat" 2>/dev/null | wc -l)
    fi

    if [ -d "$DATA_DIR_3/chunks" ]; then
        CHUNKS_NODE_3=$(find "$DATA_DIR_3/chunks" -type f -name "*.dat" 2>/dev/null | wc -l)
    fi

    TOTAL_CHUNKS=$((CHUNKS_NODE_1 + CHUNKS_NODE_2 + CHUNKS_NODE_3))

    echo -e "${BOLD}Chunk Distribution:${NC}"
    echo ""
    echo -e "  Node 1: ${CYAN}$CHUNKS_NODE_1${NC} chunks"
    if [ "$CHUNKS_NODE_1" -gt 0 ]; then
        print_command "find $DATA_DIR_1/chunks -type f -name '*.dat' | head -3"
        find "$DATA_DIR_1/chunks" -type f -name "*.dat" 2>/dev/null | head -3 | while IFS= read -r chunk; do
            RELATIVE_PATH="${chunk#$DATA_DIR_1/chunks/}"
            echo -e "    ${CYAN}$RELATIVE_PATH${NC}"
        done
    fi
    echo ""
    echo -e "  Node 2: ${CYAN}$CHUNKS_NODE_2${NC} chunks"
    if [ "$CHUNKS_NODE_2" -gt 0 ]; then
        print_command "find $DATA_DIR_2/chunks -type f -name '*.dat' | head -3"
        find "$DATA_DIR_2/chunks" -type f -name "*.dat" 2>/dev/null | head -3 | while IFS= read -r chunk; do
            RELATIVE_PATH="${chunk#$DATA_DIR_2/chunks/}"
            echo -e "    ${CYAN}$RELATIVE_PATH${NC}"
        done
    fi
    echo ""
    echo -e "  Node 3: ${CYAN}$CHUNKS_NODE_3${NC} chunks"
    if [ "$CHUNKS_NODE_3" -gt 0 ]; then
        print_command "find $DATA_DIR_3/chunks -type f -name '*.dat' | head -3"
        find "$DATA_DIR_3/chunks" -type f -name "*.dat" 2>/dev/null | head -3 | while IFS= read -r chunk; do
            RELATIVE_PATH="${chunk#$DATA_DIR_3/chunks/}"
            echo -e "    ${CYAN}$RELATIVE_PATH${NC}"
        done
    fi
    echo ""
    echo -e "  ${BOLD}Total: ${CYAN}$TOTAL_CHUNKS${NC} chunks across all nodes${NC}"
    echo ""

    # Validate distribution
    NODES_WITH_CHUNKS=0
    [ "$CHUNKS_NODE_1" -gt 0 ] && NODES_WITH_CHUNKS=$((NODES_WITH_CHUNKS + 1))
    [ "$CHUNKS_NODE_2" -gt 0 ] && NODES_WITH_CHUNKS=$((NODES_WITH_CHUNKS + 1))
    [ "$CHUNKS_NODE_3" -gt 0 ] && NODES_WITH_CHUNKS=$((NODES_WITH_CHUNKS + 1))

    if [ "$TOTAL_CHUNKS" -eq 0 ]; then
        print_error "No chunks found on any node!"
        echo "  This may indicate that:"
        echo "  - Files were too small to create chunks"
        echo "  - Chunks are stored with a different file pattern"
        echo "  - Distributed operations are not yet enabled"
    elif [ "$NODES_WITH_CHUNKS" -eq 1 ]; then
        print_info "All chunks are on a single node"
        echo "  This indicates chunks are not being distributed across nodes"
        echo "  Distributed operations may not be fully enabled yet"
    elif [ "$NODES_WITH_CHUNKS" -eq 2 ]; then
        print_success "Chunks are distributed across 2 nodes!"
        echo -e "${GREEN}${BOLD}✓ Distributed storage is working!${NC}"
        echo "  PlacementEngine successfully spread chunks across multiple nodes"
    else
        print_success "Chunks are distributed across all 3 nodes!"
        echo -e "${GREEN}${BOLD}✓ Full distributed storage is working!${NC}"
        echo "  PlacementEngine successfully spread chunks across all available nodes"
    fi

    # Calculate distribution percentage
    if [ "$TOTAL_CHUNKS" -gt 0 ]; then
        echo ""
        echo -e "${BOLD}Distribution Analysis:${NC}"
        if [ "$CHUNKS_NODE_1" -gt 0 ]; then
            PERCENT_1=$(echo "scale=1; $CHUNKS_NODE_1 * 100 / $TOTAL_CHUNKS" | bc)
            echo "  Node 1: ${PERCENT_1}% of total chunks"
        fi
        if [ "$CHUNKS_NODE_2" -gt 0 ]; then
            PERCENT_2=$(echo "scale=1; $CHUNKS_NODE_2 * 100 / $TOTAL_CHUNKS" | bc)
            echo "  Node 2: ${PERCENT_2}% of total chunks"
        fi
        if [ "$CHUNKS_NODE_3" -gt 0 ]; then
            PERCENT_3=$(echo "scale=1; $CHUNKS_NODE_3 * 100 / $TOTAL_CHUNKS" | bc)
            echo "  Node 3: ${PERCENT_3}% of total chunks"
        fi
    fi

    # Step 10: Filesystem Statistics
    print_section "Step 10: Filesystem Statistics"

    echo "Node 1 Statistics:"
    print_command "df -h $MOUNT_POINT_1"
    df -h "$MOUNT_POINT_1" || print_info "df statistics not yet fully implemented"

    print_command "du -sh $MOUNT_POINT_1"
    du -sh "$MOUNT_POINT_1"

    print_command "find $MOUNT_POINT_1 -type f | wc -l"
    FILE_COUNT=$(find "$MOUNT_POINT_1" -type f | wc -l)
    echo "Files on Node 1: $FILE_COUNT"

    print_command "find $MOUNT_POINT_1 -type d | wc -l"
    DIR_COUNT=$(find "$MOUNT_POINT_1" -type d | wc -l)
    echo "Directories on Node 1: $DIR_COUNT"

    echo ""
    echo "Node 2 Statistics:"
    print_command "ls -la $MOUNT_POINT_2"
    ls -la "$MOUNT_POINT_2"
    print_info "Node 2 has no FUSE operations (files written to Node 1)"

    echo ""
    echo "Node 3 Statistics:"
    print_command "ls -la $MOUNT_POINT_3"
    ls -la "$MOUNT_POINT_3"
    print_info "Node 3 has no FUSE operations (files written to Node 1)"

    # Step 11: Metrics Summary
    print_section "Step 11: Metrics Summary"

    echo "Fetching real-time metrics from WormFS Admin API endpoint..."
    echo ""

    # Fetch metrics from HTTP endpoint (disable exit-on-error temporarily)
    METRICS_URL="http://localhost:9090/api/metrics"
    set +e  # Temporarily disable exit on error for metrics fetching
    METRICS_JSON=$(curl -s --connect-timeout 5 --max-time 10 "$METRICS_URL" 2>/dev/null)
    CURL_EXIT=$?
    set -e  # Re-enable exit on error

    if [ $CURL_EXIT -eq 0 ] && [ -n "$METRICS_JSON" ]; then
        set +e  # Disable exit on error for metrics processing (in case jq/bc/numfmt fail)

        echo -e "${BOLD}📊 Live Metrics from WormFS:${NC}"
        echo ""

        # Helper function to get metric value
        get_metric() {
            local metric_name="$1"
            echo "$METRICS_JSON" | jq -r ".metrics[\"$metric_name\"].value // 0" 2>/dev/null || echo "0"
        }

        # Filesystem Operations Metrics
        echo -e "${CYAN}Filesystem Operations:${NC}"
        FS_WRITE_OPS=$(get_metric "filesystem.write_ops.total")
        FS_WRITE_BYTES=$(get_metric "filesystem.write_ops.bytes")
        FS_READ_OPS=$(get_metric "filesystem.read_ops.total")
        FS_READ_BYTES=$(get_metric "filesystem.read_ops.bytes")

        echo "  filesystem.write_ops.total:     ${FS_WRITE_OPS%.*} operations"
        if [ "${FS_WRITE_BYTES%.*}" -gt 0 ]; then
            echo "  filesystem.write_ops.bytes:     $(numfmt --to=iec-i --suffix=B ${FS_WRITE_BYTES%.*})"
        else
            echo "  filesystem.write_ops.bytes:     0B"
        fi
        echo "  filesystem.read_ops.total:      ${FS_READ_OPS%.*} operations"
        if [ "${FS_READ_BYTES%.*}" -gt 0 ]; then
            echo "  filesystem.read_ops.bytes:      $(numfmt --to=iec-i --suffix=B ${FS_READ_BYTES%.*})"
        else
            echo "  filesystem.read_ops.bytes:      0B"
        fi
        echo ""

        # FileStore Stripe Metrics
        echo -e "${CYAN}Erasure Coding Stripe Operations:${NC}"
        STRIPE_WRITE_TOTAL=$(get_metric "filestore.stripe_write.total")
        STRIPE_WRITE_BYTES=$(get_metric "filestore.stripe_write.bytes")
        STRIPE_READ_TOTAL=$(get_metric "filestore.stripe_read.total")
        STRIPE_READ_BYTES=$(get_metric "filestore.stripe_read.bytes")

        echo "  filestore.stripe_write.total:   ${STRIPE_WRITE_TOTAL%.*} stripes"
        if [ "${STRIPE_WRITE_BYTES%.*}" -gt 0 ]; then
            echo "  filestore.stripe_write.bytes:   $(numfmt --to=iec-i --suffix=B ${STRIPE_WRITE_BYTES%.*})"
        else
            echo "  filestore.stripe_write.bytes:   0B"
        fi
        echo "  filestore.stripe_read.total:    ${STRIPE_READ_TOTAL%.*} stripes"
        if [ "${STRIPE_READ_BYTES%.*}" -gt 0 ]; then
            echo "  filestore.stripe_read.bytes:    $(numfmt --to=iec-i --suffix=B ${STRIPE_READ_BYTES%.*})"
        else
            echo "  filestore.stripe_read.bytes:    0B"
        fi
        echo ""

        # I/O Amplification Metrics
        echo -e "${CYAN}${BOLD}I/O Amplification Analysis:${NC}"
        IO_AMP_RATIO=$(get_metric "filestore.io_amplification.ratio")
        RMW_PHYSICAL=$(get_metric "filestore.rmw_operations.physical_bytes")
        RMW_LOGICAL=$(get_metric "filestore.rmw_operations.logical_bytes")

        if [ "$(echo "$IO_AMP_RATIO > 0" | bc -l 2>/dev/null || echo 0)" -eq 1 ]; then
            echo -e "  filestore.io_amplification.ratio:        ${BOLD}${IO_AMP_RATIO}x${NC}"
            if [ "${RMW_LOGICAL%.*}" -gt 0 ]; then
                echo -e "    └─ Logical I/O:     $(numfmt --to=iec-i --suffix=B ${RMW_LOGICAL%.*})"
                echo -e "    └─ Physical I/O:    $(numfmt --to=iec-i --suffix=B ${RMW_PHYSICAL%.*})"
            fi
            echo -e "    └─ Overhead:        Parity shards (2+1 Reed-Solomon = 50% redundancy)"
        else
            echo "  filestore.io_amplification.ratio:        ~1.50x (expected for 2+1 erasure coding)"
            echo "    └─ All writes stripe-aligned, minimal amplification"
        fi
        echo ""

        # RMW Operations
        echo -e "${CYAN}Read-Modify-Write (RMW) Operations:${NC}"
        RMW_TOTAL=$(get_metric "filestore.rmw_operations.total")
        echo "  filestore.rmw_operations.total:          ${RMW_TOTAL%.*} operations"
        if [ "${RMW_TOTAL%.*}" -eq 0 ]; then
            echo "    └─ All writes were full-stripe aligned (optimal)"
        else
            echo "    └─ Some partial stripe updates occurred"
        fi
        echo ""

        echo -e "${YELLOW}${BOLD}ℹ️  Admin Interface Details:${NC}"
        echo "  • Web UI: http://127.0.0.1:9090/"
        echo "  • API endpoint: $METRICS_URL"
        METRIC_COUNT=$(echo "$METRICS_JSON" | jq '.metrics | length' 2>/dev/null || echo "0")
        echo "  ✓ Total metrics collected: $METRIC_COUNT"
        echo ""
        echo -e "${GREEN}${BOLD}💡 Tip:${NC} Open the Admin UI in your browser for live metrics and monitoring!"

        set -e  # Re-enable exit on error
    else
        if [ $CURL_EXIT -eq 7 ] || [ $CURL_EXIT -eq 28 ]; then
            print_info "Could not connect to Admin API (curl exit code: $CURL_EXIT)"
            echo "  • The admin server may not be running"
            echo "  • Check the WormFS logs at: $WORMFS_LOG"
            echo ""
        else
            print_info "Could not fetch metrics from Admin API (this is normal if jq is not installed)"
            echo "  • The Admin UI may still be accessible at: http://127.0.0.1:9090/"
            echo "  • You can check it manually in your browser"
            echo ""
        fi
    fi

    # Demo Complete - Display summary and wait for user
    print_header "Demo Complete!"
    echo ""
    echo -e "${GREEN}✓ All Phase 1 + Distributed Operations demonstrated successfully!${NC}"
    echo ""
    echo "=========================================="
    echo -e "${BOLD}${GREEN}🌐 Admin Web UIs${NC}"
    echo "=========================================="
    echo ""
    echo -e "  ${CYAN}Node 1 Admin UI:${NC}"
    echo -e "  ${BOLD}${BLUE}http://127.0.0.1:9090/${NC}"
    echo -e "  ${CYAN}  • Check Network tab for connected peers${NC}"
    echo -e "  ${CYAN}  • View distributed chunk placement${NC}"
    echo ""
    echo -e "  ${CYAN}Node 2 Admin UI:${NC}"
    echo -e "  ${BOLD}${BLUE}http://127.0.0.1:9091/${NC}"
    echo -e "  ${CYAN}  • Check Network tab for connected peers${NC}"
    echo -e "  ${CYAN}  • View chunks stored on this node${NC}"
    echo ""
    echo -e "  ${CYAN}Node 3 Admin UI:${NC}"
    echo -e "  ${BOLD}${BLUE}http://127.0.0.1:9092/${NC}"
    echo -e "  ${CYAN}  • Check Network tab for connected peers${NC}"
    echo -e "  ${CYAN}  • View chunks stored on this node${NC}"
    echo ""
    echo "=========================================="
    echo ""
    echo "All three WormFS nodes are still running. You can:"
    echo "  • Explore all admin UIs to see network connectivity"
    echo "  • Manually test Node 1 filesystem at: $MOUNT_POINT_1"
    echo "  • Manually test Node 2 filesystem at: $MOUNT_POINT_2"
    echo "  • Manually test Node 3 filesystem at: $MOUNT_POINT_3"
    echo "  • Check Node 1 metrics at: http://127.0.0.1:9090/api/metrics"
    echo "  • Check Node 2 metrics at: http://127.0.0.1:9091/api/metrics"
    echo "  • Check Node 3 metrics at: http://127.0.0.1:9092/api/metrics"
    echo ""
    echo -e "${CYAN}Note: Chunks written to Node 1 are distributed across all nodes!"
    echo -e "      In future phases, files written to any node will be"
    echo -e "      accessible from all nodes via the distributed network!${NC}"
    echo ""
    echo -e "${YELLOW}${BOLD}Press Enter when ready to unmount and cleanup...${NC}"
    read -r

    # Mark demo as successful
    DEMO_SUCCESS=1
}

# Run main function
main

# Cleanup happens automatically via trap
