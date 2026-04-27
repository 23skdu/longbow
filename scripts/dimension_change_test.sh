#!/bin/bash
# Dimension Change Stress Test
# Tests that the server handles dimension changes correctly

set -e
cd /Users/rsd/REPOS/longbow

DIMS="128 384 768 1536"
SCALE=1000
DATASET="dim_test"

echo "=== Dimension Change Stress Test ==="

cleanup() {
    echo "Cleaning up..."
    kill -9 $(lsof -t -i :3000) 2>/dev/null || true
    kill -9 $(lsof -t -i :3001) 2>/dev/null || true
    rm -rf ./data
}
trap cleanup EXIT

echo "Starting server..."
rm -rf ./data
nohup ./bin/longbow-server > /tmp/longbow.log 2>&1 &
SERVER_PID=$!

sleep 3
for i in {1..30}; do
    if lsof -i :3000 | grep -q LISTEN; then
        echo "Server ready"
        break
    fi
    sleep 1
done

echo ""
echo "Cycling through dimensions: $DIMS"
for dim in $DIMS; do
    echo ""
    echo "--- Testing dim=$dim ---"
    
    # Delete existing dataset
    curl -s -X DELETE "http://127.0.0.1:3000/v1/datasets/$DATASET" || true
    sleep 1
    
    # Create with new dimension
    curl -s -X POST "http://127.0.0.1:3000/v1/datasets" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"$DATASET\",\"dimension\":$dim,\"vector_type\":\"float32\"}" || true
    sleep 1
    
    # Ingest vectors
    python3 scripts/test_data_generator.py --dim $dim --count $SCALE --output /tmp/vectors_$dim.npz
    
    curl -s -X POST "http://127.0.0.1:3000/v1/datasets/$DATASET/insert" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @/tmp/vectors_$dim.npz || true
    
    sleep 2
    
    # Verify search works
    query=$(python3 -c "import numpy as np; v=np.random.randn($dim).astype(np.float32).tolist(); print(list(v))")
    
    result=$(curl -s -X POST "http://127.0.0.1:3000/v1/datasets/$DATASET/search" \
        -H "Content-Type: application/json" \
        -d "{\"vector\":$query,\"k\":10}")
    
    count=$(echo $result | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('results',[])))")
    
    if [ "$count" -gt 0 ]; then
        echo "  dim=$dim: OK ($count results)"
    else
        echo "  dim=$dim: FAILED (no results)"
        exit 1
    fi
done

echo ""
echo "=== Dimension change stress test PASSED ==="