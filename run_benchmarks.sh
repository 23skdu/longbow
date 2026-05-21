#!/bin/bash
set -e

echo "Starting parallel benchmarks..."

# Matrix definitions
DTYPES="float16,float32,float64,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
SEARCH_MODES="hybrid,dense,sparse,filtered,byid,learnedindex,geo,graphrag,temporal"

DIMS_1="128,384,768,1024,3072"
COUNTS_1="1000,5000,25000,100000,250000"

DIMS_2="128,384"
COUNTS_2="500000,750000,1000000"

# Localhost command wrapper
run_local() {
  echo "[LOCAL] Building fresh binaries locally..."
  go build -o build/longbow ./cmd/longbow
  go build -o build/bench-tool ./cmd/bench-tool
  echo "[LOCAL] Starting local benchmarks (Metal/CPU)..."
  
  echo "[LOCAL] Running Matrix 1..."
  export LONGBOW_MAX_MEMORY=19327352832
  python3 scripts/unified_benchmark.py --mode metal --dtypes $DTYPES --dims $DIMS_1 --counts $COUNTS_1 --search-modes $SEARCH_MODES --pprof || true

  echo "[LOCAL] Running Matrix 2..."
  export LONGBOW_MAX_MEMORY=19327352832
  python3 scripts/unified_benchmark.py --mode metal --dtypes $DTYPES --dims $DIMS_2 --counts $COUNTS_2 --search-modes $SEARCH_MODES --pprof || true

  echo "[LOCAL] Local benchmarks completed."
}

# Ancalagon command wrapper
run_remote() {
  echo "[REMOTE] Building fresh binaries and running benchmarks on ancalagon..."
  
  CMD="cd ~/longbow; "
  CMD+="git pull origin main; "
  CMD+="go build -o build/longbow ./cmd/longbow; "
  CMD+="go build -o build/bench-tool ./cmd/bench-tool; "
  CMD+="export LONGBOW_MAX_MEMORY=15032385536; "
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
