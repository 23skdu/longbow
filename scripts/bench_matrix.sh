#!/bin/bash

# Configuration
DTYPES=("float32" "float64" "float16" "int8" "int16" "int32" "int64" "uint8" "uint16" "uint32" "uint64" "complex64" "complex128" "turboquant2" "turboquant4" "turboquant8")
DIMS=(128 384 768 1024 3072)
COUNTS=(1000 5000 10000 50000 100000)
QUERIES=100
URI=${1:-"127.0.0.1:3000"}
METRICS_URI=${2:-"127.0.0.1:9090"}
MODE=${3:-"cpu"}
HOST=$(hostname)
OUTPUT_DIR="bench_results/${HOST}_${MODE}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR/profiles"

echo "Starting Performance Benchmark Matrix..."
echo "Host: $HOST, Mode: $MODE"
echo "Output Directory: $OUTPUT_DIR"
echo "Target URI: $URI, Metrics: $METRICS_URI"

# Pprof collection in background
(
    while true; do
        ts=$(date +%H%M%S)
        curl -s "http://$METRICS_URI/debug/pprof/profile?seconds=30" -o "$OUTPUT_DIR/profiles/cpu_${ts}.prof" > /dev/null 2>&1
        curl -s "http://$METRICS_URI/debug/pprof/heap" -o "$OUTPUT_DIR/profiles/heap_${ts}.prof" > /dev/null 2>&1
        sleep 60
    done
) &
PPROF_PID=$!

# Cleanup function to kill background pprof
cleanup() {
    echo "Cleaning up..."
    kill $PPROF_PID
    exit
}
trap cleanup SIGINT SIGTERM

for count in "${COUNTS[@]}"; do
    for dim in "${DIMS[@]}"; do
        for dtype_raw in "${DTYPES[@]}"; do
            dtype=$dtype_raw
            tq_bits=4
            
            # Handle turboquant special naming
            if [[ $dtype_raw == turboquant* ]]; then
                dtype="turboquant"
                tq_bits=${dtype_raw#turboquant}
            fi
            
            dataset="bench_${dtype_raw}_${dim}_${count}"
            json_out="$OUTPUT_DIR/${dataset}.json"
            
            echo "Running: $dataset (dim=$dim, dtype=$dtype, bits=$tq_bits, count=$count)"
            
            # Run benchmark tool
            # The tool runs all search modes (Dense, Hybrid, etc.) internally.
            ./bin/bench-tool \
                -uri "$URI" \
                -dataset "$dataset" \
                -dim "$dim" \
                -dtype "$dtype" \
                -tq-bits "$tq_bits" \
                -scale "$count" \
                -queries "$QUERIES" \
                -json "$json_out" \
                >> "$OUTPUT_DIR/bench.log" 2>&1
            
            if [ $? -ne 0 ]; then
                echo "  FAILED: check $OUTPUT_DIR/bench.log"
            else
                echo "  Completed."
            fi
            
            # Optional: Delete dataset after each run to save memory/disk if needed
            # For now we'll keep them since we have 18GB allocation.
        done
    done
done

kill $PPROF_PID
echo "Benchmark Matrix Finished."
