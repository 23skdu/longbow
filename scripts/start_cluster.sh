#!/bin/bash
# start_cluster.sh - Start a 3-node Longbow cluster locally

# Build binary
go build -o bin/longbow ./cmd/longbow

# Create data dirs
mkdir -p ./data/node1 ./data/node2 ./data/node3

# Ports
# Node 1: 3000 (Data), 3001 (Meta), 9090 (Metrics), 7946 (Gossip)
# Node 2: 3010 (Data), 3011 (Meta), 9091 (Metrics), 7947 (Gossip)
# Node 3: 3020 (Data), 3021 (Meta), 9092 (Metrics), 7948 (Gossip)

echo "Starting Node 1..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
LONGBOW_META_ADDR=0.0.0.0:3001 \
LONGBOW_METRICS_ADDR=0.0.0.0:9090 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_PORT=7946 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7947,127.0.0.1:7948 \
LONGBOW_DATA_PATH=./data/node1 \
LONGBOW_GPU_ENABLED=true \
./bin/longbow > node1.log 2>&1 &

sleep 2

echo "Starting Node 2..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3010 \
LONGBOW_META_ADDR=0.0.0.0:3011 \
LONGBOW_METRICS_ADDR=0.0.0.0:9091 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_PORT=7947 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946,127.0.0.1:7948 \
LONGBOW_DATA_PATH=./data/node2 \
LONGBOW_GPU_ENABLED=true \
./bin/longbow > node2.log 2>&1 &

sleep 2

echo "Starting Node 3..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3020 \
LONGBOW_META_ADDR=0.0.0.0:3021 \
LONGBOW_METRICS_ADDR=0.0.0.0:9092 \
LONGBOW_GOSSIP_ENABLED=true \
LONGBOW_GOSSIP_PORT=7948 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946,127.0.0.1:7947 \
LONGBOW_DATA_PATH=./data/node3 \
LONGBOW_GPU_ENABLED=true \
./bin/longbow > node3.log 2>&1 &

echo "Cluster started. Use 'pkill longbow' to stop."
echo "Endpoints:"
echo "  Node 1: localhost:3000 (Data), localhost:3001 (Meta)"
echo "  Node 2: localhost:3010 (Data), localhost:3011 (Meta)"
echo "  Node 3: localhost:3020 (Data), localhost:3021 (Meta)"
