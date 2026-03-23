#!/bin/bash
set -e

echo "=============================================="
echo "Longbow Mac Metal Performance Test"
echo "=============================================="

# Detect Metal GPU
echo ""
echo "Detecting Metal GPU..."
if [ -f "/System/Library/Frameworks/Metal.framework" ]; then
    echo "✓ Metal framework detected"
    uname -m
    if [ "$(uname -m)" = "arm64" ]; then
        echo "✓ Apple Silicon detected"
    fi
else
    echo "✗ Metal not available"
    exit 1
fi

# Build with Metal support
echo ""
echo "Building with Metal GPU support..."
make build-metal

# Clean up any existing data
echo ""
echo "Cleaning up old data..."
rm -rf data/test_metal_*
mkdir -p data/test_metal

# Start server with Metal
echo ""
echo "Starting Longbow with Metal GPU..."
./bin/longbow-metal server &
SERVER_PID=$!
sleep 5

# Check if server started
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "✗ Failed to start server"
    exit 1
fi
echo "✓ Server started (PID: $SERVER_PID)"

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    kill $SERVER_PID 2>/dev/null || true
    rm -rf data/test_metal_*
}
trap cleanup EXIT

# Run basic ingestion test
echo ""
echo "=============================================="
echo "Running Ingestion Test"
echo "=============================================="
python3 scripts/perf_test.py --rows 5000 --dim 128 --ingest-only --duration 10s

# Run search test
echo ""
echo "=============================================="
echo "Running Dense Search Test"
echo "=============================================="
python3 scripts/perf_test.py --rows 5000 --dim 128 --search --dense --duration 10s

# Run hybrid search test
echo ""
echo "=============================================="
echo "Running Hybrid Search Test"
echo "=============================================="
python3 scripts/perf_test.py --rows 5000 --dim 128 --search --hybrid --duration 10s

echo ""
echo "=============================================="
echo "Metal Performance Test Complete"
echo "=============================================="
