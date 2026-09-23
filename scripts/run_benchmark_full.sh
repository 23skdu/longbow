#!/bin/bash
# Full benchmark: 4 configs × 2 disk modes
# CPU std, CPU emlgo, GPU std, GPU emlgo × use_disk=yes, use_disk=no
set -e

cd /home/rsd/REPOS/longbow
DTYPES="int8,uint8,float16,float32,float64,complex64,complex128,turboquant4"
DIMS="128"
COUNTS="100000,250000"
SEARCH="dense,sparse,hybrid,graphrag,temporal"
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
        2>&1 | tee "data/perf_logs/run_${full_label}.log"

    echo ""
    echo "  COMPLETED: $full_label at $(date)"
    echo ""

    cleanup
}

echo "================================================================"
echo "  LONGBOW BENCHMARK SUITE (100k/250k, disk/nodisk)"
echo "  Started: $(date)"
echo "  Configs: CPU std/emlgo, GPU std/emlgo"
echo "  Disk modes: yes, no"
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
