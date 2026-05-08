#!/bin/bash
set -e

# Configuration
DTYPES=("float32" "float64" "float16" "int8" "int16" "int32" "int64" "uint8" "uint16" "uint32" "uint64" "complex64" "complex128" "turboquant2" "turboquant4" "turboquant8")
DIMS=(128 384 768 1024 3072)
COUNTS=(5000 10000 25000 100000 250000)

HOST=$(hostname)
MODE=$1 # "cpu" or "metal" or "cuda"
OUTPUT_DIR="bench_results/${HOST}_${MODE}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR/logs"
mkdir -p "$OUTPUT_DIR/profiles"

export LONGBOW_MAX_MEMORY=19327352832
export LONGBOW_LOG_LEVEL=info
export LONGBOW_TEMPORAL_ENABLED=true
export LONGBOW_HYBRID_SEARCH_ENABLED=true
export LONGBOW_LEARNED_INDEX_ENABLED=true
export LONGBOW_AUTOSCALE_ENABLED=false

BINARY="bin/longbow"
if [ "$MODE" == "metal" ]; then
    BINARY="bin/longbow-metal"
elif [ "$MODE" == "cuda" ]; then
    BINARY="bin/longbow-cuda"
elif [ "$MODE" == "avx2" ]; then
    BINARY="bin/longbow-avx2"
fi

echo "========================================="
echo "Full Matrix Benchmark: $HOST ($MODE)"
echo "Output: $OUTPUT_DIR"
echo "========================================="

# Helper to start server
start_server() {
    rm -rf data/*
    $BINARY > "$OUTPUT_DIR/logs/server.log" 2>&1 &
    SERVER_PID=$!
    
    # Wait for server to be ready
    for i in {1..30}; do
        if grep -q "Started ingestion workers" "$OUTPUT_DIR/logs/server.log"; then
            echo "Server is ready (PID: $SERVER_PID)"
            return 0
        fi
        sleep 1
    done
    echo "Server failed to start"
    tail -n 20 "$OUTPUT_DIR/logs/server.log"
    exit 1
}

# Helper to stop server
stop_server() {
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
    sleep 2
}

# Run the matrix
for count in "${COUNTS[@]}"; do
    for dim in "${DIMS[@]}"; do
        for dtype in "${DTYPES[@]}"; do
            # Handle turboquant naming
            TQ_BITS=4
            REAL_DTYPE=$dtype
            if [[ $dtype == turboquant* ]]; then
                TQ_BITS=${dtype#turboquant}
                REAL_DTYPE="turboquant"
            fi

            NAME="bench_${dtype}_${dim}_${count}"
            echo "[$(date +%H:%M:%S)] Running: $NAME"
            
            start_server
            
            # Start pprof collection
            curl -s "http://localhost:9090/debug/pprof/profile?seconds=10" > "$OUTPUT_DIR/profiles/${NAME}_cpu.pprof" &
            PPROF_PID=$!
            curl -s "http://localhost:9090/debug/pprof/heap" > "$OUTPUT_DIR/profiles/${NAME}_heap.pprof" &
            
            # Run bench-tool
            WORKERS=8
            if [[ "$HOST" == "ancalagon" ]]; then WORKERS=16; fi
            
            ./bin/bench-tool \
                -uri "127.0.0.1:3000" \
                -dataset "$NAME" \
                -dim "$dim" \
                -dtype "$REAL_DTYPE" \
                -tq-bits "$TQ_BITS" \
                -scale "$count" \
                -queries 1000 \
                -workers "$WORKERS" \
                -json "$OUTPUT_DIR/${NAME}.json" \
                -drop >> "$OUTPUT_DIR/logs/bench.log" 2>&1 || echo "[ERROR] $NAME failed"
            
            kill $PPROF_PID 2>/dev/null || true
            stop_server
            
            # Clean up potential hung processes
            pkill -9 -f longbow || true
        done
    done
done

echo "Benchmark suite completed."
