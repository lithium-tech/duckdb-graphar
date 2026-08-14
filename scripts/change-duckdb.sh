#!/bin/bash

# Script to update DuckDB version across configuration files and submodules
# Usage: ./scripts/change-duckdb.sh [--fast] <version>
# Example: ./scripts/change-duckdb.sh v1.5.0
#          ./scripts/change-duckdb.sh --fast v1.5.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Parse arguments
FAST_MODE=false
if [ "$1" == "--fast" ] || [ "$1" == "-f" ]; then
    FAST_MODE=true
    shift
fi

# Function to display current version
show_current_version() {
    echo "Current DuckDB version:"
    
    if [ -f "$PROJECT_ROOT/.github/duckdb_version" ]; then
        echo "  .github/duckdb_version: $(cat "$PROJECT_ROOT/.github/duckdb_version")"
    fi
    
    if [ -d "$PROJECT_ROOT/duckdb" ] && [ -e "$PROJECT_ROOT/duckdb/.git" ]; then
        cd "$PROJECT_ROOT/duckdb"
        duckdb_version=$(git describe --tags --always --match 'v*' 2>/dev/null || git rev-parse --short HEAD 2>/dev/null || echo "unknown")
        echo "  duckdb submodule: $duckdb_version"
        cd "$PROJECT_ROOT"
    else
        echo "  duckdb submodule: not initialized"
    fi
    
    if [ -d "$PROJECT_ROOT/extension-ci-tools" ] && [ -e "$PROJECT_ROOT/extension-ci-tools/.git" ]; then
        cd "$PROJECT_ROOT/extension-ci-tools"
        tools_version=$(git describe --tags --always --match 'v*' 2>/dev/null || git rev-parse --short HEAD 2>/dev/null || echo "unknown")
        echo "  extension-ci-tools submodule: $tools_version"
        cd "$PROJECT_ROOT"
    else
        echo "  extension-ci-tools submodule: not initialized"
    fi
    
    exit 0
}

# Check arguments
if [ "$1" == "--current" ] || [ "$1" == "-c" ]; then
    show_current_version
fi

if [ -z "$1" ]; then
    echo "Error: Version not specified"
    echo "Usage: $0 [--fast] <version>"
    echo "  --fast, -f    Skip git fetch, use only local tags"
    echo "Example: $0 v1.5.0"
    echo "         $0 --fast v1.5.0"
    echo "Use --current to show current version"
    exit 1
fi

NEW_VERSION="$1"

echo "=========================================="
echo "Updating DuckDB version to: $NEW_VERSION"
if [ "$FAST_MODE" = true ]; then
    echo "Mode: FAST (skipping git fetch)"
fi
echo "=========================================="
echo ""

# Update .github/duckdb_version file
echo "Step 1: Updating .github/duckdb_version file..."
if [ -f "$PROJECT_ROOT/.github/duckdb_version" ]; then
    echo "$NEW_VERSION" > "$PROJECT_ROOT/.github/duckdb_version"
    echo "  ✓ Updated"
else
    mkdir -p "$PROJECT_ROOT/.github"
    echo "$NEW_VERSION" > "$PROJECT_ROOT/.github/duckdb_version"
    echo "  ✓ Created"
fi
echo ""

# Update duckdb submodule
echo "Step 2: Updating duckdb submodule..."
if [ -d "$PROJECT_ROOT/duckdb" ] && [ -e "$PROJECT_ROOT/duckdb/.git" ]; then
    cd "$PROJECT_ROOT/duckdb"
    if [ "$FAST_MODE" = false ]; then
        echo "  Fetching tags from origin..."
        git fetch --tags origin
        echo ""
    else
        echo "  Skipping fetch (using local tags only)..."
    fi
    echo "  Checking out $NEW_VERSION..."
    if git rev-parse "$NEW_VERSION" >/dev/null 2>&1; then
        git checkout "$NEW_VERSION"
        echo "  ✓ Updated duckdb to $NEW_VERSION"
    else
        echo "  ✗ Version $NEW_VERSION not found"
        if [ "$FAST_MODE" = false ]; then
            echo "  Available tags (last 10):"
            git tag -l 'v*' | tail -10
        else
            echo "  Try running without --fast to fetch remote tags"
        fi
    fi
    cd "$PROJECT_ROOT"
else
    echo "  ✗ duckdb submodule not initialized"
    echo "  Run: git submodule update --init duckdb"
fi
echo ""

# Update extension-ci-tools submodule
echo "Step 3: Updating extension-ci-tools submodule..."
if [ -d "$PROJECT_ROOT/extension-ci-tools" ] && [ -e "$PROJECT_ROOT/extension-ci-tools/.git" ]; then
    cd "$PROJECT_ROOT/extension-ci-tools"
    if [ "$FAST_MODE" = false ]; then
        echo "  Fetching tags from origin..."
        git fetch --tags origin
        echo ""
    else
        echo "  Skipping fetch (using local tags only)..."
    fi
    echo "  Checking out $NEW_VERSION..."
    if git rev-parse "$NEW_VERSION" >/dev/null 2>&1; then
        git checkout "$NEW_VERSION"
        echo "  ✓ Updated extension-ci-tools to $NEW_VERSION"
    else
        echo "  ✗ Version $NEW_VERSION not found"
        if [ "$FAST_MODE" = false ]; then
            echo "  Available tags (last 10):"
            git tag -l 'v*' | tail -10
        else
            echo "  Try running without --fast to fetch remote tags"
        fi
    fi
    cd "$PROJECT_ROOT"
else
    echo "  ✗ extension-ci-tools submodule not initialized"
    echo "  Run: git submodule update --init extension-ci-tools"
fi
echo ""

echo "=========================================="
echo "Completed: DuckDB version set to $NEW_VERSION"
echo "=========================================="
echo ""
echo "Verify with: $0 --current"