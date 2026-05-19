#!/bin/bash
# scripts/run_benchmark_now.sh
# Launches the full benchmark sweep without rebuilding binaries.
# Local (CPU + Metal) and Remote (CPU + CUDA) run in parallel;
# each host runs its two modes sequentially.

set -euo pipefail

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128,384,768,1024,3072"
COUNTS="100000,250000,500000,750000"
MAX_MEM=19327352832  # 18 GB

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(dirname "$SCRIPT_DIR")"

mkdir -p "$REPO/logs" "$REPO/data/perf_logs"

# ── Local: CPU then Metal ────────────────────────────────────────────────────
run_local() {
    echo "[LOCAL] Starting CPU benchmarks..."
    python3 "$REPO/scripts/unified_benchmark.py" \
        --mode cpu \
        --dtypes "$DTYPES" \
        --dims "$DIMS" \
        --counts "$COUNTS" \
        --search-modes all \
        --label local_cpu \
        --duration 2 \
        --queries 100 \
        --workers 8 \
        --memory $MAX_MEM \
        --pprof > "$REPO/logs/local_cpu_audit.log" 2>&1

    mkdir -p "$REPO/data/perf_logs/local_cpu"
    mv "$REPO/data/perf_logs/result_"*.json  "$REPO/data/perf_logs/local_cpu/" 2>/dev/null || true
    mv "$REPO/data/perf_logs/profile_"*.pprof "$REPO/data/perf_logs/local_cpu/" 2>/dev/null || true
    mv "$REPO/data/perf_logs/bench_"*.log     "$REPO/data/perf_logs/local_cpu/" 2>/dev/null || true
    mv "$REPO/data/perf_logs/longbow_"*.log   "$REPO/data/perf_logs/local_cpu/" 2>/dev/null || true
    echo "[LOCAL] CPU benchmarks done."

    echo "[LOCAL] Starting Metal benchmarks..."
    python3 "$REPO/scripts/unified_benchmark.py" \
        --mode metal \
        --dtypes "$DTYPES" \
        --dims "$DIMS" \
        --counts "$COUNTS" \
        --search-modes all \
        --label local_metal \
        --duration 2 \
        --queries 100 \
        --workers 8 \
        --memory $MAX_MEM \
        --pprof > "$REPO/logs/local_metal_audit.log" 2>&1

    mkdir -p "$REPO/data/perf_logs/local_metal"
    mv "$REPO/data/perf_logs/result_"*.json  "$REPO/data/perf_logs/local_metal/" 2>/dev/null || true
    mv "$REPO/data/perf_logs/profile_"*.pprof "$REPO/data/perf_logs/local_metal/" 2>/dev/null || true
    mv "$REPO/data/perf_logs/bench_"*.log     "$REPO/data/perf_logs/local_metal/" 2>/dev/null || true
    mv "$REPO/data/perf_logs/longbow_"*.log   "$REPO/data/perf_logs/local_metal/" 2>/dev/null || true
    echo "[LOCAL] Metal benchmarks done."
}

# ── Remote: CPU then CUDA on ancalagon ──────────────────────────────────────
run_remote() {
    echo "[REMOTE] Starting CPU + CUDA benchmarks on ancalagon..."
    ssh ancalagon "cd REPOS/longbow && \
        python3 scripts/unified_benchmark.py \
            --mode cpu \
            --dtypes \"$DTYPES\" \
            --dims \"$DIMS\" \
            --counts \"$COUNTS\" \
            --search-modes all \
            --label remote_cpu \
            --duration 2 \
            --queries 100 \
            --workers 8 \
            --memory $MAX_MEM \
            --pprof > logs/remote_cpu_audit.log 2>&1 && \
        mkdir -p data/perf_logs/remote_cpu && \
        mv data/perf_logs/result_*.json  data/perf_logs/remote_cpu/ 2>/dev/null || true && \
        mv data/perf_logs/profile_*.pprof data/perf_logs/remote_cpu/ 2>/dev/null || true && \
        mv data/perf_logs/bench_*.log     data/perf_logs/remote_cpu/ 2>/dev/null || true && \
        mv data/perf_logs/longbow_*.log   data/perf_logs/remote_cpu/ 2>/dev/null || true && \
        echo '[REMOTE] CPU done.' && \
        python3 scripts/unified_benchmark.py \
            --mode cuda \
            --dtypes \"$DTYPES\" \
            --dims \"$DIMS\" \
            --counts \"$COUNTS\" \
            --search-modes all \
            --label remote_cuda \
            --duration 2 \
            --queries 100 \
            --workers 8 \
            --memory $MAX_MEM \
            --pprof > logs/remote_cuda_audit.log 2>&1 && \
        mkdir -p data/perf_logs/remote_cuda && \
        mv data/perf_logs/result_*.json  data/perf_logs/remote_cuda/ 2>/dev/null || true && \
        mv data/perf_logs/profile_*.pprof data/perf_logs/remote_cuda/ 2>/dev/null || true && \
        mv data/perf_logs/bench_*.log     data/perf_logs/remote_cuda/ 2>/dev/null || true && \
        mv data/perf_logs/longbow_*.log   data/perf_logs/remote_cuda/ 2>/dev/null || true && \
        echo '[REMOTE] CUDA done.'"
}

echo "Launching parallel benchmarks (local + remote)..."
run_local &
LOCAL_PID=$!

run_remote &
REMOTE_PID=$!

wait $LOCAL_PID
echo "[LOCAL] All local benchmarks complete."

wait $REMOTE_PID
echo "[REMOTE] All remote benchmarks complete."

echo "Syncing remote results..."
rsync -avz ancalagon:REPOS/longbow/data/perf_logs/ "$REPO/data/perf_logs/" || true
rsync -avz ancalagon:REPOS/longbow/logs/ "$REPO/logs/" || true

echo "Aggregating results..."
if [ -f "$REPO/scripts/aggregate_results.py" ]; then
    python3 "$REPO/scripts/aggregate_results.py" \
        --dir "$REPO/data/perf_logs" \
        --out "$REPO/docs/performance_matrix_v023rc1.md" || true
fi

echo "Done! Check logs/ for output and data/perf_logs/ for results."
