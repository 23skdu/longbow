#!/bin/bash
set -e

# Cleanup function
cleanup() {
    echo "Stopping nodes..."
    kill $(jobs -p) 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

# 1. Build
echo "Building binaries..."
go build -o bin/longbow ./cmd/longbow
go build -o bin/bench-tool ./cmd/bench-tool

# 2. Cleanup Data
rm -rf soak_data
mkdir -p soak_data/node1 soak_data/node2 soak_data/node3

# 3. Start Nodes
echo "Starting Node 1..."
LISTEN_ADDR=:3000 META_ADDR=:3001 METRICS_ADDR=127.0.0.1:9090 GOSSIP_PORT=7946 \
DATA_PATH=soak_data/node1 NODE_ID=node1 GOSSIP_ENABLED=true \
./bin/longbow > soak_data/node1.log 2>&1 &

echo "Starting Node 2..."
LISTEN_ADDR=:3002 META_ADDR=:3003 METRICS_ADDR=127.0.0.1:9091 GOSSIP_PORT=7947 \
DATA_PATH=soak_data/node2 NODE_ID=node2 GOSSIP_ENABLED=true \
GOSSIP_STATIC_PEERS=127.0.0.1:7946 \
./bin/longbow > soak_data/node2.log 2>&1 &

echo "Starting Node 3..."
LISTEN_ADDR=:3004 META_ADDR=:3005 METRICS_ADDR=127.0.0.1:9092 GOSSIP_PORT=7948 \
DATA_PATH=soak_data/node3 NODE_ID=node3 GOSSIP_ENABLED=true \
GOSSIP_STATIC_PEERS=127.0.0.1:7946 \
./bin/longbow > soak_data/node3.log 2>&1 &

# Wait for startup
echo "Waiting for cluster to converge..."
sleep 5

# 4. Start Load
echo "Starting Load (Ingest & Search)..."
./bin/bench-tool -peers 127.0.0.1:3000,127.0.0.1:3002,127.0.0.1:3004 \
    -duration 10m -mode ingest -concurrency 4 -batch-size 500 > soak_data/ingest.log 2>&1 &

./bin/bench-tool -peers 127.0.0.1:3000,127.0.0.1:3002,127.0.0.1:3004 \
    -duration 10m -mode search -concurrency 4 > soak_data/search.log 2>&1 &

# 5. Soak
echo "Soaking for 10 minutes..."
# We sleep 9.5m then capture profile
sleep 570 

# 6. Capture Profiles
echo "Capturing profiles..."
curl -s "http://127.0.0.1:9090/debug/pprof/profile?seconds=20" > soak_data/profile_node1.pb.gz &
curl -s "http://127.0.0.1:9091/debug/pprof/profile?seconds=20" > soak_data/profile_node2.pb.gz &
curl -s "http://127.0.0.1:9092/debug/pprof/profile?seconds=20" > soak_data/profile_node3.pb.gz &

wait

echo "Soak test complete. Artifacts in soak_data/"
