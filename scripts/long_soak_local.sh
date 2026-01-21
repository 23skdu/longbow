#!/bin/bash
set -e

# Long-running local soak test for memory profiling
# Duration: 20 minutes (1200 seconds)
# Captures heap profiles every 2 minutes

DURATION=1200
PROFILE_INTERVAL=120
DIM=384
CONCURRENT=4
DATASET="long_soak_$(date +%s)"

echo "=== Long Soak Test for Memory Profiling ==="
echo "Duration: ${DURATION}s (20 minutes)"
echo "Profile Interval: ${PROFILE_INTERVAL}s"
echo "Dimensions: ${DIM}"
echo "Concurrent Workers: ${CONCURRENT}"
echo "============================================"

# Cleanup function
function cleanup {
    echo "Cleaning up processes..."
    pkill -f 'bin/longbow' || true
    kill $PROFILER_PID 2>/dev/null || true
}
trap cleanup EXIT
trap cleanup SIGINT

# Ensure clean slate
echo "Killing any existing longbow processes..."
pkill -f 'bin/longbow' || true
sleep 3

# Build binary
echo "Building longbow binary..."
CGO_ENABLED=0 go build -tags=nogpu -o bin/longbow ./cmd/longbow

# Clean data directories
echo "Cleaning data directories..."
rm -rf data/
mkdir -p data/node1 profiles

# Start single node for profiling
echo "Starting node1..."
GOGC=75 \
LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
LONGBOW_NODE_ID=node1 \
LONGBOW_META_ADDR=0.0.0.0:3001 \
LONGBOW_METRICS_ADDR=0.0.0.0:9090 \
LONGBOW_DATA_PATH=data/node1 \
LONGBOW_MAX_MEMORY=5368709120 \
LONGBOW_USE_HNSW2=true \
./bin/longbow > data/node1/longbow.log 2>&1 &

echo "Waiting 10s for node to start..."
sleep 10

# Start profiler in background
echo "Starting heap profiler (captures every ${PROFILE_INTERVAL}s)..."
(
    elapsed=0
    capture_count=0
    while [ $elapsed -lt $DURATION ]; do
        TS=$(date +%s)
        MINUTES=$((elapsed / 60))
        echo "[${MINUTES}m] Capturing heap profile ${capture_count}..."
        curl -s -o profiles/heap_${capture_count}_${TS}.pprof http://localhost:9090/debug/pprof/heap
        curl -s -o profiles/allocs_${capture_count}_${TS}.pprof http://localhost:9090/debug/pprof/allocs
        
        capture_count=$((capture_count + 1))
        sleep $PROFILE_INTERVAL
        elapsed=$((elapsed + PROFILE_INTERVAL))
    done
    echo "Profiler completed ${capture_count} captures."
) &
PROFILER_PID=$!

# Run soak test
echo "Starting ${DURATION}s soak test..."
python3 scripts/perf_test.py \
    --dataset $DATASET \
    --rows 2000 \
    --dim $DIM \
    --search \
    --hybrid \
    --concurrent $CONCURRENT \
    --duration $DURATION \
    --data-uri grpc://localhost:3000 \
    --meta-uri grpc://localhost:3001

echo "Soak test completed. Waiting for final profile capture..."
wait $PROFILER_PID

echo ""
echo "=== Soak Test Complete ==="
echo "Profiles saved to ./profiles/"
echo "Analyze with: go tool pprof -http=:8080 profiles/heap_X_TIMESTAMP.pprof"
echo ""
echo "To compare memory growth:"
echo "  go tool pprof -base=profiles/heap_0_*.pprof profiles/heap_9_*.pprof"
