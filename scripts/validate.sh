#!/bin/bash

# WormFS Validation Script
# Runs comprehensive quality checks: build, test, format, and clippy
# Fails if any command produces errors or warnings

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Track overall success
FAILED=0

echo "=========================================="
echo "WormFS Cargo Quality Validation"
echo "=========================================="
echo ""

# Function to run a command and check for failure
run_check() {
    local name="$1"
    local cmd="$2"
    
    echo -e "${BLUE}Running: $name${NC}"
    echo "Command: $cmd"
    echo "---"
    
    if eval "$cmd"; then
        echo -e "${GREEN}✓ $name passed${NC}"
        echo ""
        return 0
    else
        echo -e "${RED}✗ $name failed${NC}"
        echo ""
        FAILED=1
        return 1
    fi
}

# 1. Cargo Build - Check for errors and warnings
echo -e "${YELLOW}Step 1/4: Building project...${NC}"
run_check "Cargo Build" "cargo build 2>&1 | tee /tmp/wormfs_build.log && ! grep -i 'error' /tmp/wormfs_build.log && ! grep -i 'warning' /tmp/wormfs_build.log"

# 2. Cargo Test - Run all tests
echo -e "${YELLOW}Step 2/4: Running tests...${NC}"
run_check "Cargo Test" "cargo test"

# 3. Cargo Fmt Check - Verify code formatting
echo -e "${YELLOW}Step 3/4: Checking code format...${NC}"
run_check "Cargo Format Check" "cargo fmt --all -- --check"

# 4. Cargo Clippy - Lint with warnings as errors
echo -e "${YELLOW}Step 4/4: Running clippy linter...${NC}"
run_check "Cargo Clippy" "cargo clippy --all-targets --all-features -- -D warnings"

# Final Summary
echo "=========================================="
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All quality checks passed!${NC}"
    echo ""
    echo "Build:  ✓ No errors or warnings"
    echo "Tests:  ✓ All tests passing"
    echo "Format: ✓ Code properly formatted"
    echo "Clippy: ✓ No linter warnings"
    echo ""
    exit 0
else
    echo -e "${RED}✗ Some quality checks failed!${NC}"
    echo ""
    echo "Please review the output above and fix any issues."
    echo ""
    exit 1
fi
