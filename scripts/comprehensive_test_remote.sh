#!/bin/bash
# scripts/comprehensive_test_remote.sh
# Identical to local one but uses cuda instead of metal

set -e

DTYPE_LIST="float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"
DIMS_LOW="128,384"
COUNTS_LOW="500,1000,5000,15000,50000,100000"
DIMS_HIGH="768,1024,3072"
COUNTS_HIGH="500,1000,5000,10000,20000"
MEMORY=19327352832 # 18 GB

host_label="ancalagon"
gpu_mode="cuda"

echo "[$host_label] Starting comprehensive tests..."

# 1. Low Dims
for dim in 128 384; do
    for count in 500 1000 5000 15000 50000 100000; do
        duration=30
        [ $count -gt 5000 ] && duration=60
        
        # CPU
        python3 scripts/unified_benchmark.py --mode cpu --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_cpu_${dim}_${count}"
        
        # GPU
        python3 scripts/unified_benchmark.py --mode $gpu_mode --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_${gpu_mode}_${dim}_${count}"
        
        # Special
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
        
        # CPU
        python3 scripts/unified_benchmark.py --mode cpu --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_cpu_${dim}_${count}"
        
        # GPU
        python3 scripts/unified_benchmark.py --mode $gpu_mode --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_${gpu_mode}_${dim}_${count}"
        
        # Special
        for smode in learned_index geo graphrag temporal recommend; do
            python3 scripts/unified_benchmark.py --mode $smode --dims $dim --counts $count --dtypes $DTYPE_LIST --memory $MEMORY --duration $duration --queries 1000 --label "${host_label}_${smode}_${dim}_${count}"
        done
    done
done

echo "[$host_label] All tests complete."
