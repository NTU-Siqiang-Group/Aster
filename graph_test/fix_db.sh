#!/bin/bash

# RocksDB Corruption Fix Script
# This script helps fix corrupted RocksDB databases

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

DB_PATH="${1:-/tmp/demo}"

echo -e "${BLUE}=== RocksDB Corruption Fix Script ===${NC}"
echo -e "Database path: ${YELLOW}${DB_PATH}${NC}"
echo ""

# Check if database exists
if [ ! -d "$DB_PATH" ]; then
    echo -e "${RED}Error: Database directory does not exist: $DB_PATH${NC}"
    exit 1
fi

# Show current status
echo -e "${YELLOW}Current database status:${NC}"
ls -lh "$DB_PATH" | head -20

echo ""
echo -e "${YELLOW}Choose an option:${NC}"
echo "1) Clean and recreate database (removes all data)"
echo "2) Try to repair database (may recover some data)"
echo "3) Backup and clean database"
echo "4) Exit"
echo ""
read -p "Enter choice [1-4]: " choice

case $choice in
    1)
        echo -e "${YELLOW}Cleaning database...${NC}"
        rm -rf "$DB_PATH"
        mkdir -p "$DB_PATH"
        echo -e "${GREEN}✓ Database cleaned and recreated${NC}"
        echo -e "${GREEN}You can now run your program to create a fresh database${NC}"
        ;;
    2)
        echo -e "${YELLOW}Attempting to repair database...${NC}"

        # Check if ldb tool exists
        if [ -f "../ldb" ]; then
            echo -e "${BLUE}Using ldb repair...${NC}"
            ../ldb repair --db="$DB_PATH"

            if [ $? -eq 0 ]; then
                echo -e "${GREEN}✓ Database repair completed${NC}"
                echo -e "${YELLOW}Note: Some data may have been lost during repair${NC}"
            else
                echo -e "${RED}✗ Repair failed${NC}"
                echo -e "${YELLOW}Recommendation: Use option 1 to clean and recreate${NC}"
            fi
        else
            echo -e "${RED}ldb tool not found. Building it...${NC}"
            cd .. && make ldb
            if [ $? -eq 0 ]; then
                ./ldb repair --db="$DB_PATH"
                echo -e "${GREEN}✓ Database repair completed${NC}"
            else
                echo -e "${RED}Failed to build ldb tool${NC}"
            fi
        fi
        ;;
    3)
        BACKUP_PATH="${DB_PATH}.backup.$(date +%Y%m%d_%H%M%S)"
        echo -e "${YELLOW}Backing up to: ${BACKUP_PATH}${NC}"
        mv "$DB_PATH" "$BACKUP_PATH"
        mkdir -p "$DB_PATH"
        echo -e "${GREEN}✓ Database backed up and cleaned${NC}"
        echo -e "${BLUE}Backup location: ${BACKUP_PATH}${NC}"
        ;;
    4)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}=== Done ===${NC}"
