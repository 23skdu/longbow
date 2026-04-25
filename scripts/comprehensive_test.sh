#!/bin/bash
# scripts/comprehensive_test.sh

set -e

# Configuration
DTYPE_LIST="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"
DIMS_LOW="128,384"
COUNTS_LOW="500,1000,5000,15000,50000,100000"
DIMS_HIGH="768,1024,3072"
COUNTS_HIGH="500,1000,5000,10000,20000"
MEMORY=19327352832 # 18 GB

# Modes
BASIC_MODES="cpu metal cuda" # metal/cuda depends on host
SPECIAL_MODES="hybrid dense sparse filtered byid learned_index geo graphrag temporal"

run_host() {
    local host_label=$1
    local gpu_mode=$2 # metal or cuda
    
    echo "[$host_label] Starting comprehensive tests..."
    
    # 1. Low Dims
    for dim in 128 384; do
        for count in 500 1000 5000 15000 50000 100000; do
            duration=30
            [ $count -gt 5000 ] && duration=60
            
            # Standard Search (CPU)
            python3 scripts/unified_benchmark.py --mode cpu --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_cpu_${dim}_${count}"
            
            # Standard Search (GPU)
            python3 scripts/unified_benchmark.py --mode $gpu_mode --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_${gpu_mode}_${dim}_${count}"
            
            # Special Modes
            for smode in learned_index geo graphrag temporal recommend; do
                python3 scripts/unified_benchmark.py --mode $smode --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_${smode}_${dim}_${count}"
            done
        done
    done

    # 2. High Dims
    for dim in 768 1024 3072; do
        for count in 500 1000 5000 10000 20000; do
            duration=30
            [ $count -gt 5000 ] && duration=60
            
            # Standard Search (CPU)
            python3 scripts/unified_benchmark.py --mode cpu --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_cpu_${dim}_${count}"
            
            # Standard Search (GPU)
            python3 scripts/unified_benchmark.py --mode $gpu_mode --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_${gpu_mode}_${dim}_${count}"
            
            # Special Modes
            for smode in learned_index geo graphrag temporal recommend; do
                python3 scripts/unified_benchmark.py --mode $smode --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries $1000 --label "${host_label}_${smode}_${dim}_${count}"
            done
        done
    done
}

# Local run
source venv/bin/activate
run_host "local" "metal" &
LOCAL_PID=$!

# Remote run
ssh ancalagon "cd REPOS/longbow && source venv/bin/activate && ./scripts/comprehensive_test_remote.sh" &
REMOTE_PID=$!

wait $LOCAL_PID
wait $REMOTE_PID

echo "All tests complete."
