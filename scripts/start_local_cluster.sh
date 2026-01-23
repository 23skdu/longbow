#!/bin/bash
set -e

# Build the binary (Pure Go, No CGO)
echo "Building longbow binary..."
CGO_ENABLED=0 go build -tags=nogpu -o bin/longbow ./cmd/longbow

# Create data directories
mkdir -p data/node1 data/node2 data/node3
mkdir -p data/node1/snapshots_tmp data/node2/snapshots_tmp data/node3/snapshots_tmp

# Pre-flight cleanup for metrics ports
echo "Checking for stale metrics processes on 9090-9092..."
for port in 9090 9091 9092; do
    PID=$(lsof -ti tcp:$port || true)
    if [ -n "$PID" ]; then
        echo "Killing stale process $PID on port $port"
        kill -9 $PID || true
    fi
done

# Start Node 1 (Seed)
echo "Starting Node 1..."
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
LONGBOW_MAX_MEMORY=5368709120 \
LONGBOW_USE_HNSW2=true \
./bin/longbow > data/node1/longbow.log 2>&1 &

sleep 2 # Wait for seed to start

# Start Node 2
echo "Starting Node 2..."
GOGC=75 \
LONGBOW_LISTEN_ADDR=0.0.0.0:3010 \
LONGBOW_NODE_ID=node2 \
LONGBOW_META_ADDR=0.0.0.0:3011 \
LONGBOW_METRICS_ADDR=0.0.0.0:9091 \
LONGBOW_GOSSIP_PORT=7947 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_DISCOVERY_PROVIDER=static \
LONGBOW_GOSSIP_STATIC_PEERS="127.0.0.1:7946" \
LONGBOW_DATA_PATH=data/node2 \
LONGBOW_MAX_MEMORY=5368709120 \
LONGBOW_USE_HNSW2=true \
./bin/longbow > data/node2/longbow.log 2>&1 &

# Start Node 3
echo "Starting Node 3..."
GOGC=75 \
LONGBOW_LISTEN_ADDR=0.0.0.0:3020 \
LONGBOW_NODE_ID=node3 \
LONGBOW_META_ADDR=0.0.0.0:3021 \
LONGBOW_METRICS_ADDR=0.0.0.0:9092 \
LONGBOW_GOSSIP_PORT=7948 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_DISCOVERY_PROVIDER=static \
LONGBOW_GOSSIP_STATIC_PEERS="127.0.0.1:7946" \
LONGBOW_DATA_PATH=data/node3 \
LONGBOW_MAX_MEMORY=5368709120 \
LONGBOW_USE_HNSW2=true \
./bin/longbow > data/node3/longbow.log 2>&1 &

echo "Cluster started! Logs in ./data/nodeX/longbow.log"
