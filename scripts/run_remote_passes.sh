#!/bin/bash
# scripts/run_remote_passes.sh
set -euo pipefail

DTYPES="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
MAX_MEM=15032385536  # 14 GB

echo "[REMOTE] Starting Sequenced Pass 1 (Wide/Shallow) on ancalagon..."
python3 scripts/unified_benchmark.py \
    --mode cpu,cuda,temporal,geo,graphrag,learned_index \
    --dims 128,384,768,1024,3072 \
    --counts 1000,5000,25000,100000,250000 \
    --dtypes "$DTYPES" \
    --memory $MAX_MEM \
    --duration 2 \
    --queries 100 \
    --pprof \
    --label remote_pass1 > logs/remote_pass1.log 2>&1

echo "[REMOTE] Pass 1 complete. Organizing results..."
for mode in cpu cuda temporal geo graphrag learned_index; do
    mkdir -p data/perf_logs/remote_${mode}
    mv data/perf_logs/result_${mode}_*.json data/perf_logs/remote_${mode}/ 2>/dev/null || true
    mv data/perf_logs/profile_${mode}_*.pprof data/perf_logs/remote_${mode}/ 2>/dev/null || true
    mv data/perf_logs/bench_${mode}_*.log data/perf_logs/remote_${mode}/ 2>/dev/null || true
    mv data/perf_logs/longbow_${mode}_*.log data/perf_logs/remote_${mode}/ 2>/dev/null || true
done

echo "[REMOTE] Starting Sequenced Pass 2 (Narrow/Deep) on ancalagon..."
python3 scripts/unified_benchmark.py \
    --mode cpu,cuda,temporal,geo,graphrag,learned_index \
    --dims 128,384 \
    --counts 500000,750000,1000000 \
    --dtypes "$DTYPES" \
    --memory $MAX_MEM \
    --duration 2 \
    --queries 100 \
    --pprof \
    --label remote_pass2 > logs/remote_pass2.log 2>&1

echo "[REMOTE] Pass 2 complete. Organizing results..."
for mode in cpu cuda temporal geo graphrag learned_index; do
    mkdir -p data/perf_logs/remote_${mode}
    mv data/perf_logs/result_${mode}_*.json data/perf_logs/remote_${mode}/ 2>/dev/null || true
    mv data/perf_logs/profile_${mode}_*.pprof data/perf_logs/remote_${mode}/ 2>/dev/null || true
    mv data/perf_logs/bench_${mode}_*.log data/perf_logs/remote_${mode}/ 2>/dev/null || true
    mv data/perf_logs/longbow_${mode}_*.log data/perf_logs/remote_${mode}/ 2>/dev/null || true
done

echo "[REMOTE] All remote passes successfully completed!"
