#!/bin/bash
export LONGBOW_GOSSIP_ENABLED=true
export LONGBOW_LOG_LEVEL=debug

# Node 1
LONGBOW_LISTEN_ADDR=0.0.0.0:3000 \
LONGBOW_META_ADDR=0.0.0.0:3001 \
LONGBOW_GOSSIP_PORT=7946 \
LONGBOW_DATA_PATH=./data1 \
LONGBOW_NODE_ID=node1 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7947 \
go run cmd/longbow/main.go > node1.log 2>&1 &
echo $! > node1.pid

# Node 2
LONGBOW_LISTEN_ADDR=0.0.0.0:3002 \
LONGBOW_META_ADDR=0.0.0.0:3003 \
LONGBOW_GOSSIP_PORT=7947 \
LONGBOW_DATA_PATH=./data2 \
LONGBOW_NODE_ID=node2 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946 \
go run cmd/longbow/main.go > node2.log 2>&1 &
echo $! > node2.pid

echo "Waiting for nodes to start and discover each other..."
sleep 15
