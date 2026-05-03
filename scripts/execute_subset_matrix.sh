#!/bin/bash
set -e

# Configuration (SUBSET for quick verification)
DTYPES=("float32" "int8" "turboquant8")
DIMS=(128 768)
COUNTS=(1000 10000 100000)

HOST=$(hostname)
MODE=$1 # "cpu" or "metal" or "cuda"
if [ -z "$MODE" ]; then MODE="cpu"; fi

OUTPUT_DIR="bench_results/${HOST}_${MODE}_subset_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR/logs"
mkdir -p "$OUTPUT_DIR/profiles"

export LONGBOW_MAX_MEMORY=19327352832
export LONGBOW_LOG_LEVEL=info

BINARY="bin/longbow"
if [ "$MODE" == "metal" ]; then
    BINARY="bin/longbow-metal"
elif [ "$MODE" == "cuda" ]; then
    BINARY="bin/longbow-cuda"
fi

echo "========================================="
echo "Subset Matrix Benchmark: $HOST ($MODE)"
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
            TQ_BITS=8
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
            
            # Run bench-tool
            WORKERS=8
            
            ./bin/bench-tool \
                -uri "127.0.0.1:3000" \
                -dataset "$NAME" \
                -dim "$dim" \
                -dtype "$REAL_DTYPE" \
                -tq-bits "$TQ_BITS" \
                -scale "$count" \
                -queries 100 \
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

echo "Benchmark subset completed."
