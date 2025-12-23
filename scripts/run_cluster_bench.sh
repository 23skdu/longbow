#!/bin/bash
set -e

# Cleanup function
cleanup() {
    echo "Stopping Longbow nodes..."
    pkill longbow || true
    rm -rf ./data/node1 ./data/node2 ./data/node3
}

trap cleanup EXIT

echo "Starting node 1..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3000 LONGBOW_META_ADDR=0.0.0.0:3001 LONGBOW_METRICS_ADDR=0.0.0.0:9090 LONGBOW_DATA_PATH=./data/node1 LONGBOW_GOSSIP_ENABLED=true LONGBOW_GOSSIP_PORT=7946 LONGBOW_LOG_LEVEL=error ./longbow > node1.log 2>&1 &

echo "Starting node 2..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3010 LONGBOW_META_ADDR=0.0.0.0:3011 LONGBOW_METRICS_ADDR=0.0.0.0:9091 LONGBOW_DATA_PATH=./data/node2 LONGBOW_GOSSIP_ENABLED=true LONGBOW_GOSSIP_PORT=7956 LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946 LONGBOW_LOG_LEVEL=error ./longbow > node2.log 2>&1 &

echo "Starting node 3..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3020 LONGBOW_META_ADDR=0.0.0.0:3021 LONGBOW_METRICS_ADDR=0.0.0.0:9092 LONGBOW_DATA_PATH=./data/node3 LONGBOW_GOSSIP_ENABLED=true LONGBOW_GOSSIP_PORT=7966 LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946 LONGBOW_LOG_LEVEL=error ./longbow > node3.log 2>&1 &

sleep 5

echo "Cluster status check..."
python3 scripts/perf_test.py --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001 --check-cluster

echo "Running vector search benchmark..."
python3 scripts/perf_test.py --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001 --rows 1000 --search --search-k 10 --query-count 500

echo "Running hybrid search benchmark..."
python3 scripts/perf_test.py --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001 --rows 1000 --hybrid --search-k 10 --query-count 200

echo "Done."
