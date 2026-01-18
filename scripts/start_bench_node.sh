#!/bin/bash
set -e

# Build the binary
echo "Building longbow..."
CGO_ENABLED=0 go build -tags=nogpu -o bin/longbow ./cmd/longbow

# Create data directories
mkdir -p data/bench_node

# Cleanup function
cleanup() {
    echo "Stopping cluster..."
    kill $(jobs -p) 2>/dev/null || true
    echo "Cluster stopped."
}
trap cleanup EXIT

# Start the node
echo "Starting Longbow node..."
GOGC=75 \
LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
LONGBOW_NODE_ID=bench_node \
LONGBOW_META_ADDR=0.0.0.0:3001 \
LONGBOW_METRICS_ADDR=0.0.0.0:9090 \
LONGBOW_GOSSIP_PORT=7946 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_DISCOVERY_PROVIDER=static \
LONGBOW_GOSSIP_STATIC_PEERS="" \
LONGBOW_DATA_PATH=data/bench_node \
LONGBOW_MAX_MEMORY=8589934592 \
LONGBOW_HYBRID_SEARCH_ENABLED=true \
LONGBOW_HYBRID_TEXT_COLUMNS=text \
./bin/longbow > data/bench_node/longbow.log 2>&1 &

echo "Benchmark node started! Logs in ./data/bench_node/longbow.log"
echo "PID: $!"
echo "Waiting for healthy status..."

# Wait loop
for i in {1..30}; do
    if curl -s http://localhost:9090/metrics > /dev/null; then
        echo "Node is UP!"
        exit 0
    fi
    sleep 1
done

echo "Node failed to start."
exit 1
