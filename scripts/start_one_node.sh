#!/bin/bash
set -e

# Build the binary
echo "Building longbow..."
CGO_ENABLED=0 go build -tags=nogpu -o bin/longbow ./cmd/longbow

# Create data directories
mkdir -p data/node1

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
LONGBOW_NODE_ID=node1 \
LONGBOW_META_ADDR=0.0.0.0:3001 \
LONGBOW_METRICS_ADDR=0.0.0.0:9090 \
LONGBOW_GOSSIP_PORT=7946 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_DISCOVERY_PROVIDER=static \
LONGBOW_GOSSIP_STATIC_PEERS="" \
LONGBOW_DATA_PATH=data/node1 \
LONGBOW_MAX_MEMORY=25769803776 \
LONGBOW_HYBRID_SEARCH_ENABLED=true \
LONGBOW_HYBRID_TEXT_COLUMNS=text \
./bin/longbow > data/node1/longbow.log 2>&1 &

echo "One-node cluster started! Logs in ./data/node1/longbow.log"
echo "Press Ctrl+C to stop."

wait
