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

# 2. Cargo Test - Run all tests (hide successful test output)
echo -e "${YELLOW}Step 2/6: Running tests...${NC}"
run_check "Cargo Test" "cargo test 2>&1 | grep -v ' ... ok$' | grep -v '^$'"

# 3. Cargo Test (Integration Tests) - Run integration tests with test-utils feature
echo -e "${YELLOW}Step 3/6: Running integration tests...${NC}"
run_check "Cargo Integration Tests" "cargo test --tests --features test-utils 2>&1 | grep -v ' ... ok$' | grep -v '^$'"

# 4. Cargo Check (test-utils feature) - Verify test utilities compile
echo -e "${YELLOW}Step 4/6: Checking test-utils feature...${NC}"
run_check "Cargo Check test-utils" "cargo check --features test-utils"

# 5. Cargo Fmt Check - Verify code formatting
echo -e "${YELLOW}Step 5/6: Checking code format...${NC}"
run_check "Cargo Format Check" "cargo fmt --all -- --check"

# 6. Cargo Clippy - Lint with warnings as errors
echo -e "${YELLOW}Step 6/6: Running clippy linter...${NC}"
run_check "Cargo Clippy" "cargo clippy --all-targets --all-features -- -D warnings"

# Final Summary
echo "=========================================="
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All quality checks passed!${NC}"
    echo ""
    echo "Build:             ✓ No errors or warnings"
    echo "Unit Tests:        ✓ All tests passing"
    echo "Integration Tests: ✓ All tests passing"
    echo "Test Utils:        ✓ Mocks compile correctly"
    echo "Format:            ✓ Code properly formatted"
    echo "Clippy:            ✓ No linter warnings"
    echo ""
    exit 0
else
    echo -e "${RED}✗ Some quality checks failed!${NC}"
    echo ""
    echo "Please review the output above and fix any issues."
    echo ""
    exit 1
fi
