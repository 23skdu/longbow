#!/bin/bash

DEVICE=$1
HOST_LABEL=$2
PORT=${3:-3000}
METRICS_PORT=${4:-9090}

if [ -z "$DEVICE" ] || [ -z "$HOST_LABEL" ]; then
    echo "Usage: $0 <cpu|metal|cuda> <host_label> [port] [metrics_port]"
    exit 1
fi

export LONGBOW_MAX_MEMORY=19327352832 # 18 GB
export LONGBOW_LISTEN_ADDR="0.0.0.0:$PORT"
export LONGBOW_META_ADDR="0.0.0.0:$((PORT + 100))"
export LONGBOW_METRICS_ADDR="0.0.0.0:$METRICS_PORT"
export LONGBOW_GOSSIP_PORT="$((PORT + 4000))"
export LONGBOW_LOG_LEVEL=debug
export LONGBOW_LEARNED_INDEX_ENABLED=true
export LONGBOW_TEMPORAL_ENABLED=true
export LONGBOW_HYBRID_SEARCH_ENABLED=true

if [ "$DEVICE" == "metal" ] || [ "$DEVICE" == "cuda" ]; then
    export LONGBOW_GPU_ENABLED=true
else
    export LONGBOW_GPU_ENABLED=false
fi

mkdir -p profiles
mkdir -p data_${HOST_LABEL}_${DEVICE}
export LONGBOW_DATA_PATH="./data_${HOST_LABEL}_${DEVICE}"

TYPES=("float32" "float64" "float16" "int8" "int16" "int32" "int64" "uint8" "uint16" "uint32" "uint64" "complex64" "complex128" "turboquant2" "turboquant4" "turboquant8")
DIMS=(128 384 768 1024 3072)
COUNTS=("1k" "5k" "10k" "50k" "100k")

get_count() {
    case $1 in
        "1k") echo 1000 ;;
        "5k") echo 5000 ;;
        "10k") echo 10000 ;;
        "50k") echo 50000 ;;
        "100k") echo 100000 ;;
    esac
}

echo "Starting benchmark matrix for $HOST_LABEL $DEVICE on port $PORT"
echo "Results will be saved to results_${HOST_LABEL}_${DEVICE}_*.json"

for dim in "${DIMS[@]}"; do
    for count_str in "${COUNTS[@]}"; do
        count=$(get_count $count_str)
        for type in "${TYPES[@]}"; do
            dtype=$type
            bits=4
            if [[ $type == turboquant* ]]; then
                dtype="turboquant"
                bits=${type#turboquant}
            fi

            # Start Longbow server for this specific test
            ./bin/longbow > "server_${HOST_LABEL}_${DEVICE}_${type}_${dim}_${count_str}.log" 2>&1 &
            SERVER_PID=$!

            # Wait for server to be ready
            echo "Waiting for Longbow server ($SERVER_PID) for $type, dim=$dim, count=$count..."
            until curl -s "http://localhost:$METRICS_PORT/ready" > /dev/null; do
                sleep 1
                if ! kill -0 $SERVER_PID 2>/dev/null; then
                    echo "Server failed to start. See server log."
                    exit 1
                fi
            done
            
            echo "--- Benchmarking $HOST_LABEL $DEVICE: $type, dim=$dim, count=$count ---"
            
            # Start pprof collection in background
            curl -s "http://localhost:$METRICS_PORT/debug/pprof/profile?seconds=15" -o "profiles/${HOST_LABEL}_${DEVICE}_${type}_${dim}_${count_str}_cpu.prof" &
            PPROF_PID=$!
            
            # Run benchmark
            if ! ./bin/bench-tool \
                -uri "127.0.0.1:$PORT" \
                -dtype "$dtype" \
                -dim "$dim" \
                -scale "$count" \
                -tq-bits "$bits" \
                -dataset "bench_${type}_${dim}_${count_str}" \
                -json "results_${HOST_LABEL}_${DEVICE}_${type}_${dim}_${count_str}.json" >> "bench_${HOST_LABEL}_${DEVICE}.log" 2>&1; then
                echo "ERROR: Benchmark failed for $type, dim=$dim, count=$count"
            fi
            
            # Wait for pprof
            wait $PPROF_PID 2>/dev/null
            
            # Collect heap profile
            curl -s "http://localhost:$METRICS_PORT/debug/pprof/heap" -o "profiles/${HOST_LABEL}_${DEVICE}_${type}_${dim}_${count_str}_heap.prof"

            # Shutdown server
            kill $SERVER_PID
            wait $SERVER_PID 2>/dev/null
            
            # Clean up data directory to save space and ensure clean state
            rm -rf "${LONGBOW_DATA_PATH:?}"/*
        done
    done
done
echo "Benchmarks completed for $HOST_LABEL $DEVICE."
