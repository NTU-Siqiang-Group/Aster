#!/bin/bash

# Graph Test Build Script
# This script ensures stable compilation of graph test programs

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Graph Test Build Script ===${NC}"

# Step 1: Check if we're in the right directory
if [ ! -f "Makefile" ]; then
    echo -e "${RED}Error: Makefile not found. Please run this script from graph_test directory.${NC}"
    exit 1
fi

# Step 2: Build librocksdb.a with RTTI enabled
echo -e "${YELLOW}Step 1: Building librocksdb.a with RTTI enabled...${NC}"
cd ..
make clean > /dev/null 2>&1 || true
USE_RTTI=1 make static_lib EXTRA_CXXFLAGS="-Wno-error=shadow" -j$(nproc)

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to build librocksdb.a${NC}"
    exit 1
fi

echo -e "${GREEN}✓ librocksdb.a built successfully${NC}"

# Step 3: Build graph test programs
echo -e "${YELLOW}Step 2: Building graph test programs...${NC}"
cd graph_test
make clean

# Build both targets
make all

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to build graph test programs${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Graph test programs built successfully${NC}"

# Step 4: Verify executables
echo -e "${YELLOW}Step 3: Verifying executables...${NC}"

if [ -f "load_graph_test" ]; then
    echo -e "${GREEN}✓ load_graph_test: $(ls -lh load_graph_test | awk '{print $5}')${NC}"
else
    echo -e "${RED}✗ load_graph_test not found${NC}"
    exit 1
fi

if [ -f "graph_example" ]; then
    echo -e "${GREEN}✓ graph_example: $(ls -lh graph_example | awk '{print $5}')${NC}"
else
    echo -e "${RED}✗ graph_example not found${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}=== Build completed successfully! ===${NC}"
echo ""
echo "You can now run:"
echo "  ./load_graph_test"
echo "  ./graph_example"
