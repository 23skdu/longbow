#!/bin/bash
set -e

# Configuration
SCALES=(5000 9000 15000 25000)
# DIM set in loop below
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
# echo "Verifying cluster health..."
# python3 scripts/perf_test.py --check-cluster --meta-uri grpc://127.0.0.1:3001 --dataset health_check --skip-put --skip-search --skip-get

# Run Benchmarks
# Run Benchmarks
DIMS=(128 384)

for DIM in "${DIMS[@]}"; do
    echo "=================================================="
    echo "Running Benchmark Dimension: $DIM"
    echo "=================================================="

    for SCALE in "${SCALES[@]}"; do
        echo "--------------------------------------------------"
        echo "  Scale: $SCALE vectors"
        echo "--------------------------------------------------"
        
        REPORT_FILE="$OUTPUT_DIR/report_dim${DIM}_${SCALE}.json"
        
        # Run Perf Test
        # Removed invalid flags --test-put/get/search/hybrid/graph as they are default
        # Kept --test-delete
        python3 scripts/perf_test.py \
            --data-uri grpc://127.0.0.1:3000 \
            --meta-uri grpc://127.0.0.1:3001 \
            --rows $SCALE \
            --dim $DIM \
            --dataset "bench_dim${DIM}_${SCALE}" \
            --test-delete \
            --json "$REPORT_FILE" 

        # Optional: Short cooldown between runs
        sleep 5
    done
done

echo "Benchmarks Complete!"
