#!/bin/bash
# Start 3-node Longbow cluster with 6GB RAM per node

set -e

# Ports used by the cluster
# Node 1: 3000 (Data), 3001 (Meta), 9090 (Metrics), 7946 (Gossip)
# Node 2: 3010 (Data), 3011 (Meta), 9091 (Metrics), 7947 (Gossip)
# Node 3: 3020 (Data), 3021 (Meta), 9092 (Metrics), 7948 (Gossip)
PORTS="3000 3001 9090 7946 3010 3011 9091 7947 3020 3021 9092 7948"

cleanup_ports() {
    echo "Checking for lingering processes on ports..."
    for port in $PORTS; do
        # Use lsof to find PID on port (macOS/Linux compatible-ish)
        pid=$(lsof -ti :$port 2>/dev/null || true)
        if [ ! -z "$pid" ]; then
            echo "Killing process $pid on port $port"
            kill -9 $pid 2>/dev/null || true
        fi
    done
    echo "Ports cleaned."
}

wait_for_node() {
    local port=$1
    local name=$2
    local retries=30
    local wait_time=1

    echo -n "Waiting for $name (HTTP :$port)..."
    for i in $(seq 1 $retries); do
        if curl -s "http://localhost:$port/metrics" > /dev/null; then
            echo " UP!"
            return 0
        fi
        echo -n "."
        sleep $wait_time
    done
    echo " FAILED!"
    return 1
}

echo "Building Longbow..."
go build -o ./longbow cmd/longbow/main.go

echo "Cleaning up old data and processes..."
cleanup_ports
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
LONGBOW_METRICS_ADDR=0.0.0.0:9090 \
LONGBOW_MAX_MEMORY=6442450944 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7947,127.0.0.1:7948 \
GOMEMLIMIT=6GiB \
./longbow > node1.log 2>&1 &
echo $! > node1.pid
echo "Node 1 PID: $(cat node1.pid)"

# Node 2
echo "Starting Node 2..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3010 \
LONGBOW_META_ADDR=0.0.0.0:3011 \
LONGBOW_GOSSIP_PORT=7947 \
LONGBOW_DATA_PATH=./data2 \
LONGBOW_NODE_ID=node2 \
LONGBOW_METRICS_ADDR=0.0.0.0:9091 \
LONGBOW_MAX_MEMORY=6442450944 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946,127.0.0.1:7948 \
GOMEMLIMIT=6GiB \
./longbow > node2.log 2>&1 &
echo $! > node2.pid
echo "Node 2 PID: $(cat node2.pid)"

# Node 3
echo "Starting Node 3..."
LONGBOW_LISTEN_ADDR=0.0.0.0:3020 \
LONGBOW_META_ADDR=0.0.0.0:3021 \
LONGBOW_GOSSIP_PORT=7948 \
LONGBOW_DATA_PATH=./data3 \
LONGBOW_NODE_ID=node3 \
LONGBOW_METRICS_ADDR=0.0.0.0:9092 \
LONGBOW_MAX_MEMORY=6442450944 \
LONGBOW_GOSSIP_STATIC_PEERS=127.0.0.1:7946,127.0.0.1:7947 \
GOMEMLIMIT=6GiB \
./longbow > node3.log 2>&1 &
echo $! > node3.pid
echo "Node 3 PID: $(cat node3.pid)"

echo ""
echo "Verifying cluster health..."

wait_for_node 9090 "Node 1"
wait_for_node 9091 "Node 2"
wait_for_node 9092 "Node 3"

echo ""
echo "3-Node Cluster Started & Healthy!"
echo "Node 1: grpc://localhost:3000 (HTTP: 9090)"
echo "Node 2: grpc://localhost:3010 (HTTP: 9091)"
echo "Node 3: grpc://localhost:3020 (HTTP: 9092)"
echo ""
echo "To stop: kill \$(cat node*.pid)"
echo "Logs: node1.log, node2.log, node3.log"
