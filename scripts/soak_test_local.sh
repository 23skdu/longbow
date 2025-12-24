#!/bin/bash
set -e

# Configuration
DIM=384
DURATION=300 # 5 minutes total soak
CONCURRENT_WORKERS=4
DATASET_NAME="soak_test_local_$(date +%s)"

echo "Starting Local Soak Test..."
echo " - Dimensions: $DIM"
echo " - Duration: $DURATION seconds"
echo " - Workers: $CONCURRENT_WORKERS"
echo "---------------------------------------------"

# 1. Build Binary
echo "Building longbow binary..."
go build -o bin/longbow ./cmd/longbow

# 2. Start Local Cluster in Background
echo "Starting local cluster..."
# We run start_local_cluster.sh in background, but we need to kill it later
./scripts/start_local_cluster.sh &
CLUSTER_PID=$!

# Give it time to start and form mesh
echo "Waiting 10s for cluster startup..."
sleep 10

# 3. Start Memory Monitor (RSS used by processes)
echo "Starting Memory Monitor..."
(
    end=$((SECONDS+DURATION+10))
    while [ $SECONDS -lt $end ]; do
        # Ps check for longbow processes
        echo "--- Memory Usage (KB) ---"
        ps -o rss,command | grep bin/longbow | grep -v grep | awk '{sum+=$1} END {print sum/1024 " MB (Total)"}'
        sleep 5
    done
) &
MONITOR_PID=$!

# 4. Run Load Test
echo "[perf_test] Starting concurrent mixed load against Node 1 (localhost:3000)..."
# We target Node 1. The client should balance or we test Node 1 handling it.
python3 scripts/perf_test.py \
    --concurrent $CONCURRENT_WORKERS \
    --duration $DURATION \
    --dim $DIM \
    --name $DATASET_NAME \
    --operation mixed \
    --data-uri grpc://localhost:3000 \
    --meta-uri grpc://localhost:3001

# 5. Cleanup
echo "Soak Test Complete. Cleaning up..."
kill $MONITOR_PID 2>/dev/null || true
# Kill the cluster script and its children
pkill -P $CLUSTER_PID || true
kill $CLUSTER_PID 2>/dev/null || true
# Ensure binaries are dead
pkill -f "bin/longbow" || true
echo "Done."
