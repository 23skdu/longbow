#!/bin/bash
# Full baseline matrix: 4 build variants × 2 disk modes
# 100k / 250k vectors × 16 dtypes × all 13 search modes
#
# Resumable: a config whose perf_matrix_*_<label>_*.json already exists is skipped.
set -uo pipefail

cd /home/rsd/REPOS/longbow

DTYPES="int8,uint8,int16,uint16,int32,uint32,int64,uint64,float16,float32,float64,complex64,complex128,turboquant2,turboquant4,turboquant8"
DIMS="128"
COUNTS="100000,250000"
SEARCH="dense,hybrid,sparse,filtered,byid,graphrag,geo,temporal,learned_index"
QUERIES=500
WORKERS=8
MEMORY=17179869184
TIMEOUT=3600

cleanup() {
    pkill -9 -x longbow 2>/dev/null || true
    pkill -9 -x longbow-cuda 2>/dev/null || true
    pkill -9 -x bench-tool 2>/dev/null || true
    sleep 2
}

run_config() {
    local mode=$1
    local label=$2
    local server_binary=$3
    local cuda_binary=$4
    local use_disk=$5

    local disk_suffix="nodisk"
    if [ "$use_disk" = "yes" ]; then
        disk_suffix="disk"
    fi
    local full_label="${label}_${disk_suffix}"

    # Resume: skip configs that already produced a result file
    if ls data/perf_logs/perf_matrix_"${mode}"_"${full_label}"_*.json >/dev/null 2>&1; then
        echo "  [SKIP] ${full_label} already has results — skipping (resume)"
        return 0
    fi

    echo ""
    echo "================================================================"
    echo "  RUNNING: $full_label ($mode mode, disk=$use_disk)"
    echo "  Binary: $server_binary"
    echo "  Started: $(date)"
    echo "================================================================"
    echo ""

    cleanup

    cp "$server_binary" bin/longbow
    chmod +x bin/longbow

    if [ "$mode" = "cuda" ]; then
        cp "$cuda_binary" bin/longbow-cuda
        chmod +x bin/longbow-cuda
    fi

    local disk_flag=""
    if [ "$use_disk" = "yes" ]; then
        disk_flag="--use-disk"
    fi

    python3 scripts/unified_benchmark.py \
        --mode "$mode" \
        --dims "$DIMS" \
        --counts "$COUNTS" \
        --dtypes "$DTYPES" \
        --search-modes "$SEARCH" \
        --queries "$QUERIES" \
        --workers "$WORKERS" \
        --memory "$MEMORY" \
        --timeout "$TIMEOUT" \
        $disk_flag \
        --random-port-fallback \
        --label "$full_label" \
        --report-md "data/perf_logs/perf_matrix_${mode}_${full_label}.md" \
        2>&1 | tee "data/perf_logs/run_${full_label}.log"

    echo ""
    echo "  COMPLETED: $full_label at $(date)"
    echo ""

    cleanup
}

echo "================================================================"
echo "  LONGBOW BASELINE BENCHMARK SUITE (100k/250k, all dtypes, all modes)"
echo "  Started: $(date)"
echo "  Configs: CPU std/emlgo, GPU std/emlgo × disk/nodisk"
echo "================================================================"

mkdir -p data/perf_logs data/bench

# CPU standard, no disk
run_config "cpu" "cpu_std" "bin/longbow_main" "" "no"

# CPU standard, disk
run_config "cpu" "cpu_std" "bin/longbow_main" "" "yes"

# CPU emlgo, no disk
run_config "cpu" "cpu_emlgo" "bin/longbow_emlgo" "" "no"

# CPU emlgo, disk
run_config "cpu" "cpu_emlgo" "bin/longbow_emlgo" "" "yes"

# GPU standard, no disk
run_config "cuda" "gpu_std" "bin/longbow-cuda_main" "bin/longbow-cuda_main" "no"

# GPU standard, disk
run_config "cuda" "gpu_std" "bin/longbow-cuda_main" "bin/longbow-cuda_main" "yes"

# GPU emlgo, no disk
run_config "cuda" "gpu_emlgo" "bin/longbow-cuda_emlgo" "bin/longbow-cuda_emlgo" "no"

# GPU emlgo, disk
run_config "cuda" "gpu_emlgo" "bin/longbow-cuda_emlgo" "bin/longbow-cuda_emlgo" "yes"

echo ""
echo "================================================================"
echo "  ALL BENCHMARKS COMPLETED"
echo "  Finished: $(date)"
echo "================================================================"
echo ""

echo "Result files:"
ls -la data/perf_logs/perf_matrix_*.json 2>/dev/null
