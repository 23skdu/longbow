#!/bin/bash
set -e

# Configuration
DIM=384
DURATION=300 # 5 minutes total soak
CONCURRENT_WORKERS=4
DATASET_NAME="soak_test_$(date +%s)"

echo "Starting Soak Test..."
echo " - Dimensions: $DIM"
echo " - Duration: $DURATION seconds"
echo " - Workers: $CONCURRENT_WORKERS (Small batches)"
echo "---------------------------------------------"

# Ensure port forwarding is active
echo "Starting Port Forwarding to ns-a..."
kubectl port-forward svc/longbow-data 3000:3000 -n ns-a --context kind-longbow-multi > /dev/null 2>&1 &
PF_PID_DATA=$!
kubectl port-forward svc/longbow-meta 3001:3001 -n ns-a --context kind-longbow-multi > /dev/null 2>&1 &
PF_PID_META=$!
echo "Waiting for port forwarding..."
sleep 10

# Start Memory Monitor (Restart Count Check) in background
echo "Starting Restart Monitor..."
(
    end=$((SECONDS+DURATION+10))
    while [ $SECONDS -lt $end ]; do
        kubectl get pods -n ns-a --context kind-longbow-multi -l app.kubernetes.io/name=longbow --no-headers | awk '{print $1 " Restarts: " $4}'
        sleep 10
    done
) &
MONITOR_PID=$!

# Run Load Test (Concurrent Mixed Ops)
echo "[perf_test] Starting concurrent mixed load..."
python3 scripts/perf_test.py \
    --concurrent $CONCURRENT_WORKERS \
    --duration $DURATION \
    --dim $DIM \
    --name $DATASET_NAME \
    --operation mixed \
    --data-uri grpc://localhost:3000 \
    --meta-uri grpc://localhost:3001

# Run Ops Verification
echo "[ops_test] verifying dataset..."
python3 scripts/ops_test.py info --dataset "${DATASET_NAME}_worker_0" --meta-uri grpc://localhost:3001

echo "Soak Test Complete."
kill $MONITOR_PID 2>/dev/null || true
kill $PF_PID_DATA 2>/dev/null || true
kill $PF_PID_META 2>/dev/null || true
