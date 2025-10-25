#!/bin/bash

# WormFS Phase 2 StorageNetwork Demo Script
# Demonstrates multi-node networking with heartbeat exchange

# Error handling
set -E
set +e

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
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STORAGE_NODE_BINARY="${PROJECT_ROOT}/target/release/storage_node"
NODE1_DATA_DIR=""
NODE2_DATA_DIR=""
NODE1_PID=""
NODE2_PID=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -h|--help)
            echo "WormFS Phase 2 StorageNetwork Demo Script"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Enable verbose output"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "This script demonstrates WormFS Phase 2 networking capabilities:"
            echo "  • Multi-node StorageNetwork setup"
            echo "  • Peer-to-peer connection establishment"
            echo "  • Heartbeat message exchange"
            echo "  • Network status monitoring"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
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
    echo -e "${YELLOW}Press Enter to cleanup and exit...${NC}"
    read -r
    cleanup
    exit $exit_code
}

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

# Trap errors
trap 'error_handler' ERR

cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"

    # Stop storage nodes
    if [ -n "$NODE1_PID" ]; then
        echo -e "${CYAN}Stopping Node 1 (PID: $NODE1_PID)${NC}"
        kill "$NODE1_PID" 2>/dev/null || true
        wait "$NODE1_PID" 2>/dev/null || true
    fi

    if [ -n "$NODE2_PID" ]; then
        echo -e "${CYAN}Stopping Node 2 (PID: $NODE2_PID)${NC}"
        kill "$NODE2_PID" 2>/dev/null || true
        wait "$NODE2_PID" 2>/dev/null || true
    fi

    # Clean up data directories
    if [ -n "$NODE1_DATA_DIR" ] && [ -d "$NODE1_DATA_DIR" ]; then
        echo -e "${CYAN}Removing Node 1 data directory${NC}"
        rm -rf "$NODE1_DATA_DIR"
    fi

    if [ -n "$NODE2_DATA_DIR" ] && [ -d "$NODE2_DATA_DIR" ]; then
        echo -e "${CYAN}Removing Node 2 data directory${NC}"
        rm -rf "$NODE2_DATA_DIR"
    fi

    echo -e "${GREEN}Cleanup complete${NC}"
}

# Trap Ctrl+C for graceful shutdown
trap '{ echo ""; echo -e "${YELLOW}Caught interrupt signal${NC}"; cleanup; exit 0; }' SIGINT SIGTERM

# Print banner
echo -e "${BOLD}${BLUE}"
echo "================================================================"
echo "  WormFS Phase 2: StorageNetwork Multi-Node Demo"
echo "================================================================"
echo -e "${NC}"

# Step 1: Build the storage_node binary
echo -e "${BOLD}Step 1: Building storage_node binary${NC}"
echo -e "${CYAN}Building in release mode for optimal performance...${NC}"
cd "$PROJECT_ROOT"
cargo build --release --bin storage_node --features libp2p
if [ $? -ne 0 ]; then
    fail "Failed to build storage_node binary"
fi
echo -e "${GREEN}✓ Build successful${NC}"
echo ""

# Step 2: Create temporary data directories
echo -e "${BOLD}Step 2: Creating temporary data directories${NC}"
NODE1_DATA_DIR=$(mktemp -d -t wormfs-node1-XXXXXX)
NODE2_DATA_DIR=$(mktemp -d -t wormfs-node2-XXXXXX)
echo -e "${CYAN}Node 1 data directory: ${NODE1_DATA_DIR}${NC}"
echo -e "${CYAN}Node 2 data directory: ${NODE2_DATA_DIR}${NC}"
echo ""

# Step 3: Create configuration files
echo -e "${BOLD}Step 3: Creating configuration files${NC}"

# Node 1 configuration
cat > "$NODE1_DATA_DIR/config.toml" <<EOF
node_id = "wormfs-node-001"
data_dir = "${NODE1_DATA_DIR}"
listen_address = "127.0.0.1:7001"
libp2p_listen_port = 7101

[[peer_addresses]]
address = "127.0.0.1:7102"
EOF

# Node 2 configuration
cat > "$NODE2_DATA_DIR/config.toml" <<EOF
node_id = "wormfs-node-002"
data_dir = "${NODE2_DATA_DIR}"
listen_address = "127.0.0.1:7002"
libp2p_listen_port = 7102

[[peer_addresses]]
address = "127.0.0.1:7101"
EOF

echo -e "${GREEN}✓ Configuration files created${NC}"
echo ""

# Step 4: Start storage nodes
echo -e "${BOLD}Step 4: Starting storage nodes${NC}"

echo -e "${CYAN}Starting Node 1...${NC}"
RUST_LOG=info "$STORAGE_NODE_BINARY" \
    --config "$NODE1_DATA_DIR/config.toml" \
    > "$NODE1_DATA_DIR/node.log" 2>&1 &
NODE1_PID=$!
echo -e "${GREEN}✓ Node 1 started (PID: $NODE1_PID)${NC}"

sleep 2

echo -e "${CYAN}Starting Node 2...${NC}"
RUST_LOG=info "$STORAGE_NODE_BINARY" \
    --config "$NODE2_DATA_DIR/config.toml" \
    > "$NODE2_DATA_DIR/node.log" 2>&1 &
NODE2_PID=$!
echo -e "${GREEN}✓ Node 2 started (PID: $NODE2_PID)${NC}"
echo ""

# Step 5: Wait for nodes to connect
echo -e "${BOLD}Step 5: Waiting for peer connection${NC}"
echo -e "${CYAN}Giving nodes 10 seconds to discover and connect...${NC}"
sleep 10
echo ""

# Step 6: Show heartbeat activity
echo -e "${BOLD}Step 6: Monitoring heartbeat activity${NC}"
echo -e "${CYAN}Checking logs for heartbeat messages...${NC}"
echo ""

echo -e "${MAGENTA}Node 1 heartbeat activity:${NC}"
grep -i "heartbeat" "$NODE1_DATA_DIR/node.log" | tail -5 || echo "No heartbeat messages yet"
echo ""

echo -e "${MAGENTA}Node 2 heartbeat activity:${NC}"
grep -i "heartbeat" "$NODE2_DATA_DIR/node.log" | tail -5 || echo "No heartbeat messages yet"
echo ""

# Step 7: Show connection status
echo -e "${BOLD}Step 7: Connection status${NC}"
echo -e "${CYAN}Checking for peer connections...${NC}"
echo ""

echo -e "${MAGENTA}Node 1 connections:${NC}"
grep -i "connection established" "$NODE1_DATA_DIR/node.log" | tail -3 || echo "No connections logged"
echo ""

echo -e "${MAGENTA}Node 2 connections:${NC}"
grep -i "connection established" "$NODE2_DATA_DIR/node.log" | tail -3 || echo "No connections logged"
echo ""

# Summary
echo -e "${BOLD}${GREEN}================================================================${NC}"
echo -e "${BOLD}${GREEN}  Demo Complete - Nodes Running${NC}"
echo -e "${BOLD}${GREEN}================================================================${NC}"
echo ""
echo -e "${CYAN}Node 1:${NC}"
echo -e "  - Node ID: wormfs-node-001"
echo -e "  - Address: 127.0.0.1:7001"
echo -e "  - libp2p: 127.0.0.1:7101"
echo -e "  - Log: $NODE1_DATA_DIR/node.log"
echo ""
echo -e "${CYAN}Node 2:${NC}"
echo -e "  - Node ID: wormfs-node-002"
echo -e "  - Address: 127.0.0.1:7002"
echo -e "  - libp2p: 127.0.0.1:7102"
echo -e "  - Log: $NODE2_DATA_DIR/node.log"
echo ""
echo -e "${YELLOW}Nodes are exchanging heartbeats every 5 seconds${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop and cleanup${NC}"
echo ""

# Keep script running and show live heartbeat count
echo -e "${BOLD}Live Heartbeat Monitor (updates every 5 seconds)${NC}"
echo -e "${CYAN}Press Ctrl+C to exit${NC}"
echo ""

while true; do
    sleep 5
    NODE1_HB=$(grep -c "Broadcasted heartbeat" "$NODE1_DATA_DIR/node.log" 2>/dev/null || echo 0)
    NODE2_HB=$(grep -c "Broadcasted heartbeat" "$NODE2_DATA_DIR/node.log" 2>/dev/null || echo 0)
    NODE1_RX=$(grep -c "Received heartbeat" "$NODE1_DATA_DIR/node.log" 2>/dev/null || echo 0)
    NODE2_RX=$(grep -c "Received heartbeat" "$NODE2_DATA_DIR/node.log" 2>/dev/null || echo 0)

    echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} Node1: ${GREEN}$NODE1_HB sent${NC}, ${BLUE}$NODE1_RX received${NC} | Node2: ${GREEN}$NODE2_HB sent${NC}, ${BLUE}$NODE2_RX received${NC}"
done
