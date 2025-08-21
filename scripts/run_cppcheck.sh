#!/bin/bash

set -e

CPPCHECK_FLAGS=(
    "--enable=style,performance,portability,missingInclude"
    "--std=c++20"
    "--platform=native"
    "--inline-suppr"
    "--suppress=missingIncludeSystem"
    "--suppress=unmatchedSuppression"
    "--suppress=normalCheckLevelMaxBranches"
    "--suppress=duplInheritedMember"
    "--suppress=constParameterCallback"
    "--suppress=constParameterReference"
    "--error-exitcode=1"
    "--quiet"
    "-Iinclude"
    "-Isrc"
    "-I."
    "--config-exclude=build"
)

echo "Running cppcheck..."
cppcheck "${CPPCHECK_FLAGS[@]}" src/ include/
echo "Cppcheck completed successfully!"
