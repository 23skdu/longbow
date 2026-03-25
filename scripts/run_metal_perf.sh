#!/bin/bash
set -e

echo "=============================================="
echo "Longbow Mac Metal Performance Test"
echo "=============================================="

if [ -d "/System/Library/Frameworks/Metal.framework" ]; then
    echo "Metal framework detected"
    echo "Architecture: $(uname -m)"
    if [ "$(uname -m)" = "arm64" ]; then
        echo "Apple Silicon detected"
    fi
else
    echo "Metal not available"
    exit 1
fi

echo ""
echo "Building with Metal GPU support..."
make build-metal

echo ""
echo "Cleaning up old data..."
rm -rf data/snapshots data/wal data/wal.log data/test_metal_* data/bench* data/perf_logs
mkdir -p data/test_metal

echo ""
echo "Starting Longbow with Metal GPU (20GB memory)..."
LONGBOW_MAX_MEMORY=21474836480 ./bin/longbow-metal server &
SERVER_PID=$!
sleep 5

if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "Failed to start server"
    exit 1
fi
echo "Server started (PID: $SERVER_PID)"

cleanup() {
    echo ""
    echo "Cleaning up..."
    kill $SERVER_PID 2>/dev/null || true
    rm -rf data/test_metal_*
}
trap cleanup EXIT

echo ""
echo "=============================================="
echo "Running Ingestion Test"
echo "=============================================="
./bin/bench-tool \
    -peers 127.0.0.1:3000 \
    -mode ingest \
    -duration 10s \
    -concurrency 4 \
    -batch-size 5000 \
    -dim 128

echo ""
echo "=============================================="
echo "Running Search Test"
echo "=============================================="
./bin/bench-tool \
    -peers 127.0.0.1:3000 \
    -mode search \
    -duration 10s \
    -concurrency 4 \
    -dim 128

echo ""
echo "=============================================="
echo "Metal Performance Test Complete"
echo "=============================================="
