#!/bin/bash
set -e

echo "Starting parallel benchmarks..."

# Isolate execution environments
rm -rf /tmp/longbow_bench_data /tmp/longbow_perf_logs /tmp/longbow_bin
mkdir -p /tmp/longbow_bench_data /tmp/longbow_perf_logs /tmp/longbow_bin
make build
make build-metal
cp -P bin/* /tmp/longbow_bin/

# Matrix definitions
DTYPES="float16,float32,float64,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
SEARCH_MODES="hybrid,dense,sparse,filtered,byid,learnedindex,geo,graphrag,temporal"

DIMS_1="128,384,768,1024,3072"
COUNTS_1="1000,5000,25000,100000,250000"

DIMS_2="128,384"
COUNTS_2="500000,750000,1000000"

# Localhost command wrapper
run_local() {
  echo "[LOCAL] Starting local benchmarks (Metal/CPU)..."
  
  export LONGBOW_MAX_MEMORY=19327352832
  export LONGBOW_DATA_PATH=/tmp/longbow_bench_data
  export LONGBOW_PERF_LOGS=/tmp/longbow_perf_logs
  export LONGBOW_BIN_PATH=/tmp/longbow_bin

  echo "[LOCAL] Running Matrix 1..."
  python3 scripts/unified_benchmark.py --mode metal --dtypes $DTYPES --dims $DIMS_1 --counts $COUNTS_1 --search-modes $SEARCH_MODES --pprof || true

  echo "[LOCAL] Running Matrix 2..."
  python3 scripts/unified_benchmark.py --mode metal --dtypes $DTYPES --dims $DIMS_2 --counts $COUNTS_2 --search-modes $SEARCH_MODES --pprof || true

  echo "[LOCAL] Local benchmarks completed."
}

# Ancalagon command wrapper
run_remote() {
  echo "[REMOTE] Starting remote benchmarks (CUDA/CPU)..."
  
  CMD="cd ~/longbow; "
  CMD+="git stash; git pull origin main; "
  CMD+="rm -rf /tmp/longbow_bench_data /tmp/longbow_perf_logs /tmp/longbow_bin; "
  CMD+="mkdir -p /tmp/longbow_bench_data /tmp/longbow_perf_logs /tmp/longbow_bin; "
  CMD+="make build; make build-cuda; "
  CMD+="cp -P bin/* /tmp/longbow_bin/; "
  CMD+="export LONGBOW_MAX_MEMORY=15032385536; "
  CMD+="export LONGBOW_DATA_PATH=/tmp/longbow_bench_data; "
  CMD+="export LONGBOW_PERF_LOGS=/tmp/longbow_perf_logs; "
  CMD+="export LONGBOW_BIN_PATH=/tmp/longbow_bin; "
  CMD+="python3 scripts/unified_benchmark.py --mode cuda --dtypes $DTYPES --dims $DIMS_1 --counts $COUNTS_1 --search-modes $SEARCH_MODES --pprof || true; "
  CMD+="python3 scripts/unified_benchmark.py --mode cuda --dtypes $DTYPES --dims $DIMS_2 --counts $COUNTS_2 --search-modes $SEARCH_MODES --pprof || true;"
  
  ssh ancalagon "$CMD"
  echo "[REMOTE] Remote benchmarks completed."
}

# Run in parallel
run_local > local_bench.log 2>&1 &
LOCAL_PID=$!

run_remote > remote_bench.log 2>&1 &
REMOTE_PID=$!

echo "Local process PID: $LOCAL_PID"
echo "Remote process PID: $REMOTE_PID"

echo "Waiting for both hosts to complete..."
wait $LOCAL_PID
wait $REMOTE_PID

echo "All benchmarks finished."
