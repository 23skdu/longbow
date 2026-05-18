#!/bin/bash
export LONGBOW_MAX_MEMORY=19327352832
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="5000,25000"

function run_with_timeout() {
    local duration=$1
    shift
    if command -v timeout &> /dev/null; then
        timeout "$duration" "$@"
    elif command -v gtimeout &> /dev/null; then
        gtimeout "$duration" "$@"
    else
        "$@" &
        local pid=$!
        (sleep "$duration"; kill -9 "$pid" 2>/dev/null) &
        local watcher=$!
        wait "$pid"
        kill -9 "$watcher" 2>/dev/null
    fi
}

STDBUF=""
if command -v stdbuf &> /dev/null; then
    STDBUF="stdbuf -oL"
elif command -v unbuffer &> /dev/null; then
    STDBUF="unbuffer"
fi

echo "Starting Remote CPU Benchmark..."
run_with_timeout 18000 $STDBUF python3 -u scripts/unified_benchmark.py --mode cpu --dtypes $DTYPES --dims $DIMS --counts $COUNTS --queries 100 --search-modes all --label remote_cpu --duration 3 --pprof --memory 19327352832 2>&1 | tee remote_cpu.log

echo "Starting Remote CUDA Benchmark..."
run_with_timeout 18000 $STDBUF python3 -u scripts/unified_benchmark.py --mode cuda --dtypes $DTYPES --dims $DIMS --counts $COUNTS --queries 100 --search-modes all --label remote_cuda --duration 3 --pprof --memory 19327352832 2>&1 | tee remote_cuda.log
