#!/bin/bash
export LONGBOW_MAX_MEMORY=19327352832
DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="5000,10000,25000,100000,250000"

# Timeout wrapper function prioritizing standard commands then fallback
function run_with_timeout() {
    local duration=$1
    shift
    if command -v timeout &> /dev/null; then
        timeout "$duration" "$@"
    elif command -v gtimeout &> /dev/null; then
        gtimeout "$duration" "$@"
    else
        # POSIX fallback
        "$@" &
        local pid=$!
        (sleep "$duration"; kill -9 "$pid" 2>/dev/null) &
        local watcher=$!
        wait "$pid"
        kill -9 "$watcher" 2>/dev/null
    fi
}

# Detect stdbuf or unbuffer for line buffering
STDBUF=""
if command -v stdbuf &> /dev/null; then
    STDBUF="stdbuf -oL"
elif command -v unbuffer &> /dev/null; then
    STDBUF="unbuffer"
fi

echo "Starting Local CPU Benchmark..."
run_with_timeout 18000 $STDBUF python3 -u scripts/unified_benchmark.py --mode cpu,temporal,geo,graphrag,learned_index --dtypes $DTYPES --dims $DIMS --counts $COUNTS --search-modes all --label local_cpu --duration 3 --pprof --memory 19327352832 2>&1 | tee local_cpu.log

echo "Starting Local Metal Benchmark..."
run_with_timeout 18000 $STDBUF python3 -u scripts/unified_benchmark.py --mode metal --dtypes $DTYPES --dims $DIMS --counts $COUNTS --search-modes all --label local_metal --duration 3 --pprof --memory 19327352832 2>&1 | tee local_metal.log


