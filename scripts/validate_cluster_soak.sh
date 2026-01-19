#!/bin/bash
set -e

# Cleanup on exit
function cleanup {
    echo "Cleaning up..."
    pkill -f 'bin/longbow' || true
}
trap cleanup EXIT
trap cleanup SIGINT

# 0. Cleanup Start
echo "Ensuring clean slate..."
pkill -f 'bin/longbow' || true
sleep 3

# 1. Build
echo "Building binary..."
CGO_ENABLED=0 go build -tags=nogpu -o bin/longbow ./cmd/longbow

# 1. Start Cluster
echo "Cleaning up data directories..."
rm -rf data/
echo "Starting Cluster..."
# We use the existing start script but we need to ensure it doesn't block. 
# start_local_cluster.sh runs binaries in background.
./scripts/start_local_cluster.sh

echo "Waiting 15s for cluster to form and gossip to settle..."
sleep 15

# 2. Validation (Pre-Soak)
echo ">>> [1/5] Running Gossip & Global Search Validation (Pre-Soak)..."
python3 scripts/verify_global_search.py

# 3. Soak Test Node 1
echo ">>> [2/5] Soak Testing Node 1 (30s)..."
python3 scripts/perf_test.py --dataset soak_test --rows 2000 --dim 384 --search --hybrid --concurrent 4 --duration 30 --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001

# 4. Soak Test Node 2
echo ">>> [3/5] Soak Testing Node 2 (30s)..."
python3 scripts/perf_test.py --dataset soak_test --rows 2000 --dim 384 --search --hybrid --concurrent 4 --duration 30 --data-uri grpc://localhost:3010 --meta-uri grpc://localhost:3011

# 5. Soak Test Node 3
echo ">>> [4/5] Soak Testing Node 3 (30s)..."
python3 scripts/perf_test.py --dataset soak_test --rows 2000 --dim 384 --search --hybrid --concurrent 4 --duration 30 --data-uri grpc://localhost:3020 --meta-uri grpc://localhost:3021

# 6. Validation (Post-Soak)
echo ">>> [5/5] Running Gossip & Global Search Validation (Post-Soak)..."
python3 scripts/verify_global_search.py

echo ">>> SUCCESS: Cluster survived soak tests and remains consistent."
