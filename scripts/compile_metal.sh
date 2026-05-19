#!/bin/bash
# scripts/compile_metal.sh - AOT compile Metal shaders to .metallib

set -e

METAL_DIR="internal/gpu/metal"
SOURCE="${METAL_DIR}/kernels.metal"
OUTPUT="${METAL_DIR}/kernels.metallib"

if [ ! -f "$SOURCE" ]; then
    echo "Error: Metal source not found at $SOURCE"
    exit 1
fi

echo "Compiling Metal shaders for macOS ARM64..."

# 1. Compile to bitcode (.air)
xcrun -sdk macosx metal -c "$SOURCE" -o "${METAL_DIR}/kernels.air"

# 2. Link to library (.metallib)
xcrun -sdk macosx metallib "${METAL_DIR}/kernels.air" -o "$OUTPUT"

# 3. Clean up intermediate files
rm "${METAL_DIR}/kernels.air"

echo "Successfully generated $OUTPUT"
ls -lh "$OUTPUT"
