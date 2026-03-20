#!/bin/bash
set -e

SCALES=(1000 3000 5000 9000 15000 20000 25000)
DIMS=(128 384)

mkdir -p reports
mkdir -p data

# Cleanup function for absolute isolation
cleanup() {
    echo "Stopping any hanging longbow instances..."
    pkill -9 -f "./bin/longbow" || true
    sleep 2
}
trap cleanup EXIT

for DIM in "${DIMS[@]}"; do
    for SCALE in "${SCALES[@]}"; do
        echo "=================================================="
        echo "Running Benchmark Dimension: $DIM Scale: $SCALE"
        echo "=================================================="
        
        # 1. Clear State
        rm -rf data/*
        
        # 2. Start Single Node
        LONGBOW_LISTEN_ADDR=127.0.0.1:3000 \
        LONGBOW_META_ADDR=127.0.0.1:3001 \
        LONGBOW_MAX_MEMORY=21474836480 \
        LONGBOW_DATA_PATH=data \
        ./bin/longbow > server_inc.log 2>&1 &
        SERVER_PID=$!
        
        echo "Started server with PID $SERVER_PID. Warming up 5s..."
        sleep 5
        
        # 3. Execution (With Text to cover full Dense/Sparse/Hybrid/Filtered)
        python3 scripts/perf_test.py \
            --data-uri grpc://127.0.0.1:3000 \
            --rows $SCALE \
            --dim $DIM \
            --with-text \
            --json reports/res_dim${DIM}_${SCALE}.json \
            --dataset incremental_perf
            
        # 4. Profile Capture (Peak/End State)
        echo "Capturing PProf..."
        curl -s http://localhost:9090/debug/pprof/heap > reports/heap_dim${DIM}_${SCALE}.pprof || echo "Failed to get heap"
        go tool pprof -text reports/heap_dim${DIM}_${SCALE}.pprof > reports/heap_analysis_dim${DIM}_${SCALE}.txt || echo "Failed to analyze heap"
        
        # 5. Stop Cleanly
        echo "Stopping server..."
        kill -9 $SERVER_PID || true
        pkill -9 -f "./bin/longbow" || true
        
        # 6. Cooling Period
        echo "Cooling 5s..."
        sleep 5
    done
done

echo "=== All incremental benchmarks and pprof analysis completed! ==="
