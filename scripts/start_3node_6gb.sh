#!/bin/bash
# Start 3-node Longbow cluster with 6GB RAM per node

set -e

echo "Building Longbow..."
go build -o ./longbow cmd/longbow/main.go

echo "Cleaning up old data..."
rm -rf ./data1 ./data2 ./data3
rm -f node*.log node*.pid

export LONGBOW_GOSSIP_ENABLED=true
export LONGBOW_LOG_LEVEL=info
export GODEBUG=gctrace=1

# Node 1 (Leader)
echo "Starting Node 1..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
LONGBOW_META_ADDR=0.0.0.0:3001 \
LONGBOW_GOSSIP_PORT=7946 \
LONGBOW_DATA_PATH=./data1 \
LONGBOW_NODE_ID=node1 \
LONGBOW_HTTP_PORT=9090 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7947,127.0.0.1:7948 \
GOMEMLIMIT=6GiB \
./longbow > node1.log 2>&1 &
echo $! > node1.pid
echo "Node 1 PID: $(cat node1.pid)"

sleep 3

# Node 2
echo "Starting Node 2..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3010 \
LONGBOW_META_ADDR=0.0.0.0:3011 \
LONGBOW_GOSSIP_PORT=7947 \
LONGBOW_DATA_PATH=./data2 \
LONGBOW_NODE_ID=node2 \
LONGBOW_HTTP_PORT=9091 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946,127.0.0.1:7948 \
GOMEMLIMIT=6GiB \
./longbow > node2.log 2>&1 &
echo $! > node2.pid
echo "Node 2 PID: $(cat node2.pid)"

sleep 3

# Node 3
echo "Starting Node 3..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3020 \
LONGBOW_META_ADDR=0.0.0.0:3021 \
LONGBOW_GOSSIP_PORT=7948 \
LONGBOW_DATA_PATH=./data3 \
LONGBOW_NODE_ID=node3 \
LONGBOW_HTTP_PORT=9092 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946,127.0.0.1:7947 \
GOMEMLIMIT=6GiB \
./longbow > node3.log 2>&1 &
echo $! > node3.pid
echo "Node 3 PID: $(cat node3.pid)"

echo ""
echo "Waiting for cluster to stabilize..."
sleep 10

echo ""
echo "3-Node Cluster Started!"
echo "Node 1: grpc://localhost:3000 (HTTP: 9090)"
echo "Node 2: grpc://localhost:3010 (HTTP: 9091)"
echo "Node 3: grpc://localhost:3020 (HTTP: 9092)"
echo ""
echo "To stop: kill \$(cat node*.pid)"
echo "Logs: node1.log, node2.log, node3.log"
