#!/bin/bash
set -e

# Configuration
SCALES=(5000 9000 15000 25000)
DIM=384
MEMORY_LIMIT=$((6 * 1024 * 1024 * 1024)) # 6GB
OUTPUT_DIR="reports"
mkdir -p $OUTPUT_DIR
mkdir -p data/node1 data/node2 data/node3

# Build
echo "Building Longbow..."
go build -o bin/longbow ./cmd/longbow

# Cleanup function
cleanup() {
    echo "Stopping all longbow instances..."
    pkill -f "./bin/longbow" || true
    sleep 2
}
trap cleanup EXIT

# Start Cluster
echo "Starting 3-node cluster..."

# Node 1 (Bootstrap)
LONGBOW_LISTEN_ADDR=127.0.0.1:3000 \
LONGBOW_META_ADDR=127.0.0.1:3001 \
LONGBOW_METRICS_ADDR=127.0.0.1:9090 \
LONGBOW_GOSSIP_PORT=7946 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_MAX_MEMORY=$MEMORY_LIMIT \
LONGBOW_DATA_PATH=data/node1 \
LONGBOW_NODE_ID=node1 \
LONGBOW_AUTO_SHARDING_ENABLED=false \
./bin/longbow > $OUTPUT_DIR/node1.log 2>&1 &
echo "Node 1 started (PID $!)"

sleep 2

# Node 2
LONGBOW_LISTEN_ADDR=127.0.0.1:3002 \
LONGBOW_META_ADDR=127.0.0.1:3003 \
LONGBOW_METRICS_ADDR=127.0.0.1:9091 \
LONGBOW_GOSSIP_PORT=7947 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946 \
LONGBOW_MAX_MEMORY=$MEMORY_LIMIT \
LONGBOW_DATA_PATH=data/node2 \
LONGBOW_NODE_ID=node2 \
LONGBOW_AUTO_SHARDING_ENABLED=false \
./bin/longbow > $OUTPUT_DIR/node2.log 2>&1 &
echo "Node 2 started (PID $!)"

# Node 3
LONGBOW_LISTEN_ADDR=127.0.0.1:3004 \
LONGBOW_META_ADDR=127.0.0.1:3005 \
LONGBOW_METRICS_ADDR=127.0.0.1:9092 \
LONGBOW_GOSSIP_PORT=7948 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946 \
LONGBOW_MAX_MEMORY=$MEMORY_LIMIT \
LONGBOW_DATA_PATH=data/node3 \
LONGBOW_NODE_ID=node3 \
LONGBOW_AUTO_SHARDING_ENABLED=false \
./bin/longbow > $OUTPUT_DIR/node3.log 2>&1 &
echo "Node 3 started (PID $!)"

echo "Waiting for cluster to stabilize (10s)..."
sleep 10

# Verify Cluster
echo "Verifying cluster health..."
python3 scripts/perf_test.py --check-cluster --meta-uri grpc://127.0.0.1:3001

# Run Benchmarks
for SCALE in "${SCALES[@]}"; do
    ECHO "=================================================="
    echo "Running Benchmark Scale: $SCALE vectors"
    ECHO "=================================================="
    
    REPORT_FILE="$OUTPUT_DIR/report_${SCALE}.json"
    PROFILE_FILE="$OUTPUT_DIR/profile_${SCALE}.pb.gz"

    # Start CPU Profile capture in background
    # Increased duration to 30s to match likely test duration
    (sleep 5 && curl -s -o "$PROFILE_FILE" "http://localhost:9090/debug/pprof/profile?seconds=30") &
    PPROF_PID=$!

    # Run Perf Test
    # Using Node 1 as entry point. Partitioning will handle distribution.
    # --all runs all tests (Put, Get, Search, Hybrid, etc.)
    python3 scripts/perf_test.py \
        --data-uri grpc://127.0.0.1:3000 \
        --meta-uri grpc://127.0.0.1:3001 \
        --rows $SCALE \
        --dim $DIM \
        --name "bench_${SCALE}" \
        --test-put \
        --test-get \
        --test-search \
        --test-hybrid \
        --test-graph \
        --test-delete \
        --skip-id-search \
        --json "$REPORT_FILE"

    wait $PPROF_PID
    echo "Saved pprof profile to $PROFILE_FILE"
    echo "Validating profile size..."
    ls -lh $PROFILE_FILE

    # Optional: Short cooldown between runs
    sleep 5
done

echo "Benchmarks Complete!"
