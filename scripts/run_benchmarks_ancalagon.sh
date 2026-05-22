#!/usr/bin/env bash
# Ancalagon remote benchmark runner (x86_64 Linux - CPU + CUDA)
# Memory cap: 14GB
set -euo pipefail

cd "$HOME/longbow"

MEMORY=$((14 * 1024 * 1024 * 1024))  # 14GB
DTYPES="float16,float32,float64,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
SEARCH_MODES="all"
QUERIES=1000
DURATION=15
WORKERS=6
PPROF_FLAG="--pprof"
BIN_DIR="$HOME/longbow/bin"

# Override paths for remote
export PATH="$BIN_DIR:$PATH"

# Phase 1: Small counts (1k, 5k) - all dims
echo "=== ANCALAGON PHASE 1: Small counts (1k, 5k) - all dims ==="
for COUNT in 1000 5000; do
  DIMS="128,384,768,1024,3072"
  for MODE in cpu cuda; do
    echo "[$(date)] Running $MODE mode, count=$COUNT, dims=$DIMS"
    python3 scripts/unified_benchmark.py \
      --mode "$MODE" \
      --dims "$DIMS" \
      --counts "$COUNT" \
      --dtypes "$DTYPES" \
      --memory "$MEMORY" \
      --queries "$QUERIES" \
      --duration "$DURATION" \
      --workers "$WORKERS" \
      --search-modes "$SEARCH_MODES" \
      $PPROF_FLAG \
      --label "ancalagon_${MODE}_${COUNT}" \
      2>&1 | tee "logs/bench_ancalagon_${MODE}_${COUNT}.log"
  done
done

# Phase 2: Medium counts (25k, 100k, 250k) - all dims
echo "=== ANCALAGON PHASE 2: Medium counts (25k, 100k, 250k) - all dims ==="
for COUNT in 25000 100000 250000; do
  DIMS="128,384,768,1024,3072"
  for MODE in cpu cuda; do
    echo "[$(date)] Running $MODE mode, count=$COUNT, dims=$DIMS"
    python3 scripts/unified_benchmark.py \
      --mode "$MODE" \
      --dims "$DIMS" \
      --counts "$COUNT" \
      --dtypes "$DTYPES" \
      --memory "$MEMORY" \
      --queries "$QUERIES" \
      --duration "$DURATION" \
      --workers "$WORKERS" \
      --search-modes "$SEARCH_MODES" \
      $PPROF_FLAG \
      --label "ancalagon_${MODE}_${COUNT}" \
      2>&1 | tee "logs/bench_ancalagon_${MODE}_${COUNT}.log"
  done
done

# Phase 3: Large counts (500k, 750k, 1M) - dims 128, 384 only
echo "=== ANCALAGON PHASE 3: Large counts (500k, 750k, 1M) - dims 128,384 ==="
for COUNT in 500000 750000 1000000; do
  DIMS="128,384"
  for MODE in cpu cuda; do
    echo "[$(date)] Running $MODE mode, count=$COUNT, dims=$DIMS"
    python3 scripts/unified_benchmark.py \
      --mode "$MODE" \
      --dims "$DIMS" \
      --counts "$COUNT" \
      --dtypes "$DTYPES" \
      --memory "$MEMORY" \
      --queries "$QUERIES" \
      --duration "$DURATION" \
      --workers "$WORKERS" \
      --search-modes "$SEARCH_MODES" \
      $PPROF_FLAG \
      --label "ancalagon_${MODE}_${COUNT}" \
      2>&1 | tee "logs/bench_ancalagon_${MODE}_${COUNT}.log"
  done
done

echo "=== ANCALAGON BENCHMARKS COMPLETE ==="
