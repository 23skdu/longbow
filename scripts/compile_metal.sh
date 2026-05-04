#!/bin/bash
set -e

# Path to the kernels source and output library
SRC="internal/gpu/metal/kernels.metal"
LIB="internal/gpu/metal/kernels.metallib"
AIR="internal/gpu/metal/kernels.air"

echo "Compiling Metal kernels: $SRC -> $LIB"

# 1. Compile to AIR (Apple Intermediate Representation)
xcrun -sdk macosx metal -c "$SRC" -o "$AIR"

# 2. Compile to Metal Library
xcrun -sdk macosx metallib "$AIR" -o "$LIB"

# 3. Cleanup
rm "$AIR"

echo "Successfully compiled $LIB"
