#!/bin/bash
# start_cluster.sh - Start a 1-node or 3-node Longbow cluster locally

NODES=${1:-3}

if [ "$NODES" != "1" ] && [ "$NODES" != "3" ]; then
    echo "Usage: ./start_cluster.sh [1|3]"
    echo "Default is 3 nodes."
    exit 1
fi

echo "Building binary..."
go build -o bin/longbow ./cmd/longbow

# Create data dirs
mkdir -p ./data/node1
if [ "$NODES" == "3" ]; then
    mkdir -p ./data/node2 ./data/node3
fi

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

if [ "$NODES" == "3" ]; then
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
fi

echo "Cluster started with $NODES node(s). Use 'pkill longbow' to stop."
echo "Endpoints:"
echo "  Node 1: localhost:3000 (Data), localhost:3001 (Meta)"
if [ "$NODES" == "3" ]; then
    echo "  Node 2: localhost:3010 (Data), localhost:3011 (Meta)"
    echo "  Node 3: localhost:3020 (Data), localhost:3021 (Meta)"
fi
