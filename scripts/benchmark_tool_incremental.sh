#!/bin/bash
set -e

# Support all types loop
TYPES=("float32" "complex64" "complex128" "int32" "int16" "int8")
SCALES=(1000 5000 15000 25000)
DIMS=(128 384 768)

mkdir -p reports_go
mkdir -p data

# Cleanup function 
cleanup() {
    echo "Stopping any hanging longbow instances..."
    pkill -9 -f "./bin/longbow" || true
    sleep 2
}
trap cleanup EXIT

# 1. Build tool
echo "Building benchmark_tool..."
go build -o bin/longbow_bench ./benchmark_tool/

for TYPE in "${TYPES[@]}"; do
    for SCALE in "${SCALES[@]}"; do
        for DIM in "${DIMS[@]}"; do
            echo "=================================================="
            echo "Go Bench: Type=$TYPE Dim=$DIM Scale=$SCALE"
            echo "=================================================="
            
            # Clear State
            rm -rf data/*
            
            # Start Node
            LONGBOW_LISTEN_ADDR=127.0.0.1:3000 \
            LONGBOW_META_ADDR=127.0.0.1:3001 \
            LONGBOW_MAX_MEMORY=21474836480 \
            LONGBOW_DATA_PATH=data \
            ./bin/longbow > server_inc_go.log 2>&1 &
            SERVER_PID=$!
            
            echo "Started server with PID $SERVER_PID. Warming up 5s..."
            sleep 5
            
            # Execution
            ./bin/longbow_bench \
                --uri 127.0.0.1:3000 \
                --scale $SCALE \
                --dim $DIM \
                --dtype $TYPE \
                --dataset bench_go_${TYPE}_dim${DIM} \
                --queries 100 \
                --json reports_go/res_${TYPE}_dim${DIM}_${SCALE}.json

            # Stop Cleanly
            echo "Stopping server..."
            kill -9 $SERVER_PID || true
            pkill -9 -f "./bin/longbow" || true
            
            # Cooling Period
            echo "Cooling 3s..."
            sleep 3
        done
    done
done

echo "=== Go benchmark tool incremental outputs finished! ==="
