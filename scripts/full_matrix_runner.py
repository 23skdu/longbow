#!/usr/bin/env python3
import subprocess
import os
import sys
import time

# Matrix 1: Low Dims
DIMS_LOW = [128, 384]
COUNTS_LOW = [500, 1000, 5000, 15000, 50000, 100000]

# Matrix 2: High Dims
DIMS_HIGH = [768, 1024, 3072]
COUNTS_HIGH = [500, 1000, 5000, 10000, 20000]

DTYPES = "float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant"
MEMORY = 19327352832  # 18 GB

def run_bench(mode, dims, counts, label):
    cmd = [
        "venv/bin/python3", "scripts/unified_benchmark.py",
        "--mode", mode,
        "--dims", str(dims),
        "--counts", str(counts),
        "--dtypes", DTYPES,
        "--memory", str(MEMORY),
        "--queries", "1000",
        "--label", label
    ]
    
    # Set timeout based on count (30s for small, 60s for large as per user request)
    if int(counts) >= 5000:
        cmd += ["--duration", "60"]
    else:
        cmd += ["--duration", "30"]
        
    print(f"[{label}] Running {mode} dims={dims} counts={counts}...")
    subprocess.run(" ".join(cmd), shell=True)

def main():
    if len(sys.argv) < 3:
        print("Usage: full_matrix_runner.py <hw_mode> <host_label>")
        sys.exit(1)
        
    hw_mode = sys.argv[1] # "metal" or "cuda"
    host_label = sys.argv[2] # "local" or "ancalagon"

    # Modes to iterate:
    # 1. Hardware mode (Metal/CUDA) - this runs Dense, Hybrid, Sparse, Filtered, ByID, Geo, GraphRAG, Temporal
    # 2. CPU mode - same as above but forced to CPU
    # 3. Learned Index mode
    modes = [hw_mode, "cpu", "learned_index"]

    # Matrix 1: Low Dims
    for dim in DIMS_LOW:
        for count in COUNTS_LOW:
            for mode in modes:
                run_bench(mode, dim, count, f"{host_label}_{mode}_{dim}_{count}")

    # Matrix 2: High Dims
    for dim in DIMS_HIGH:
        for count in COUNTS_HIGH:
            for mode in modes:
                run_bench(mode, dim, count, f"{host_label}_{mode}_{dim}_{count}")

if __name__ == "__main__":
    main()
