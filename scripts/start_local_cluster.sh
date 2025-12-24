#!/bin/bash
set -e

# Build the binary
echo "Building longbow..."
CGO_ENABLED=1 go build -tags=nogpu -o bin/longbow ./cmd/longbow

# Create data directories
mkdir -p data/node1 data/node2 data/node3

# Cleanup function
cleanup() {
    echo "Stopping cluster..."
    kill $(jobs -p) 2>/dev/null || true
    echo "Cluster stopped."
}
trap cleanup EXIT

# Start Node 1 (Seed)
echo "Starting Node 1..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
LONGBOW_NODE_ID=node1 \
LONGBOW_META_ADDR=0.0.0.0:3001 \
LONGBOW_METRICS_ADDR=0.0.0.0:9090 \
LONGBOW_GOSSIP_PORT=7946 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_DISCOVERY_PROVIDER=static \
LONGBOW_GOSSIP_STATIC_PEERS="" \
LONGBOW_DATA_PATH=data/node1 \
LONGBOW_MAX_MEMORY=3221225472 \
./bin/longbow > data/node1/longbow.log 2>&1 &

sleep 2 # Wait for seed to start

# Start Node 2
echo "Starting Node 2..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3010 \
LONGBOW_NODE_ID=node2 \
LONGBOW_META_ADDR=0.0.0.0:3011 \
LONGBOW_METRICS_ADDR=0.0.0.0:9091 \
LONGBOW_GOSSIP_PORT=7947 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_DISCOVERY_PROVIDER=static \
LONGBOW_GOSSIP_STATIC_PEERS="127.0.0.1:7946" \
LONGBOW_DATA_PATH=data/node2 \
LONGBOW_MAX_MEMORY=3221225472 \
./bin/longbow > data/node2/longbow.log 2>&1 &

# Start Node 3
echo "Starting Node 3..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3020 \
LONGBOW_NODE_ID=node3 \
LONGBOW_META_ADDR=0.0.0.0:3021 \
LONGBOW_METRICS_ADDR=0.0.0.0:9092 \
LONGBOW_GOSSIP_PORT=7948 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_DISCOVERY_PROVIDER=static \
LONGBOW_GOSSIP_STATIC_PEERS="127.0.0.1:7946" \
LONGBOW_DATA_PATH=data/node3 \
LONGBOW_MAX_MEMORY=3221225472 \
./bin/longbow > data/node3/longbow.log 2>&1 &

echo "Cluster started! Logs in ./data/nodeX/longbow.log"
echo "Press Ctrl+C to stop."

wait
