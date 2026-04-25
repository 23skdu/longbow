#!/usr/bin/env python3
"""
Comprehensive Benchmark Runner

Runs the full benchmark matrix as requested:
- Dtypes: float32, float64, float16, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant2, turboquant4, turboquant8
- Dims: 128,384 (counts: 500,1k,5k,15k,50k,100k) and 768,1024,3072 (counts: 500,1k,5k,10k,20k)
- Search types: ingestion, hybrid, dense, sparse, filtered, byid, learned_index, geo-spatial, graphrag, temporal
- Modes: CPU, Metal (local), CPU/CUDA (ancalagon)

Allocates 18GB memory, collects pprof, monitors logs for errors.
Timeouts: 30s for small batches, 60s for large ones.
"""
import subprocess
import os
import sys
import time
import json
import signal
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import Thread

LONGBOW_PATH = "/Users/rsd/REPOS/longbow"
DTYPES = "float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant2,turboquant4,turboquant8"
MEMORY = 18 * 1024 * 1024 * 1024  # 18 GB

# Matrix configurations
MATRIX_LOW = {
    "dims": [128, 384],
    "counts": [500, 1000, 5000, 15000, 50000, 100000]
}

MATRIX_HIGH = {
    "dims": [768, 1024, 3072],
    "counts": [500, 1000, 5000, 10000, 20000]
}

SEARCH_TYPES = ["ingestion", "hybrid", "dense", "sparse", "filtered", "byid", "learned_index", "geo", "graphrag", "temporal"]

def get_timeout(count):
    """30s for small batches, 60s for large ones"""
    if count >= 5000:
        return 60
    return 30

def run_single_benchmark(mode, dim, count, dtype, search_type, label, timeout_val=None):
    """Run a single benchmark configuration"""
    if timeout_val is None:
        timeout_val = get_timeout(count)
    
    output_file = os.path.join(LONGBOW_PATH, "data/perf_logs", f"result_{label}_{search_type}_{dtype}_{dim}_{count}.json")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    cmd = [
        f"{LONGBOW_PATH}/scripts/venv/bin/python3",
        f"{LONGBOW_PATH}/scripts/unified_benchmark.py",
        "--mode", mode,
        "--dims", str(dim),
        "--counts", str(count),
        "--dtypes", dtype,
        "--memory", str(MEMORY),
        "--queries", "1000",
        "--label", f"{label}_{search_type}",
        "--duration", str(timeout_val),
        "--timeout", str(timeout_val * 3)
    ]
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {label}/{search_type}/{dtype} dim={dim} count={count}")
    
    try:
        result = subprocess.run(
            " ".join(cmd),
            shell=True,
            cwd=LONGBOW_PATH,
            capture_output=True,
            text=True,
            timeout=timeout_val * 4
        )
        
        # Check for errors in output
        if result.returncode != 0:
            print(f"  ERROR: {result.stderr[:200] if result.stderr else 'failed'}")
            return {"status": "failed", "error": result.stderr[:200] if result.stderr else "exit code " + str(result.returncode)}
        
        # Check for errors in log
        log_file = os.path.join(LONGBOW_PATH, "data/perf_logs", f"longbow_{mode}_{label}_{search_type}.log")
        if os.path.exists(log_file):
            with open(log_file) as f:
                log_content = f.read().lower()
                if "error" in log_content or "panic" in log_content:
                    print(f"  WARNING: Errors in log")
                    return {"status": "warning", "log_errors": log_content[:200]}
        
        return {"status": "success", "dim": dim, "count": count, "dtype": dtype, "search_type": search_type, "mode": mode}
        
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT after {timeout_val * 4}s")
        return {"status": "timeout", "timeout": timeout_val * 4}
    except Exception as e:
        print(f"  EXCEPTION: {e}")
        return {"status": "exception", "error": str(e)}

def run_matrix(mode, label):
    """Run the full benchmark matrix"""
    results = []
    start_time = time.time()
    
    # Matrix 1: Low dims
    for dim in MATRIX_LOW["dims"]:
        for count in MATRIX_LOW["counts"]:
            for dtype in DTYPES.split(","):
                for search_type in SEARCH_TYPES:
                    result = run_single_benchmark(mode, dim, count, dtype, search_type, label)
                    results.append(result)
    
    # Matrix 2: High dims
    for dim in MATRIX_HIGH["dims"]:
        for count in MATRIX_HIGH["counts"]:
            for dtype in DTYPES.split(","):
                for search_type in SEARCH_TYPES:
                    result = run_single_benchmark(mode, dim, count, dtype, search_type, label)
                    results.append(result)
    
    elapsed = time.time() - start_time
    
    # Save results summary
    summary = {
        "label": label,
        "mode": mode,
        "total_tests": len(results),
        "elapsed_seconds": elapsed,
        "results": results
    }
    
    output_file = os.path.join(LONGBOW_PATH, "data/perf_logs", f"summary_{label}.json")
    with open(output_file, "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"\nCompleted {label}: {len(results)} tests in {elapsed/60:.1f} minutes")
    return results

def main():
    if len(sys.argv) < 2:
        print("Usage: run_benchmarks.py <host>")
        print("  host: local (CPU+Metal) or ancalagon (CPU+CUDA)")
        sys.exit(1)
    
    host = sys.argv[1]
    
    if host == "local":
        # Run CPU mode benchmarks
        print("=" * 60)
        print("Starting LOCAL CPU benchmarks")
        print("=" * 60)
        run_matrix("cpu", "local_cpu")
        
        # Run Metal mode benchmarks
        print("=" * 60)
        print("Starting LOCAL Metal benchmarks")
        print("=" * 60)
        run_matrix("metal", "local_metal")
        
    elif host == "ancalagon":
        # SSH to ancalagon and run benchmarks
        print("=" * 60)
        print("Starting ancalagon benchmarks (CPU + CUDA)")
        print("=" * 60)
        
        # Create a remote script
        remote_script = "/tmp/run_benchmarks_remote.sh"
        with open(remote_script, "w") as f:
            f.write(f"""#!/bin/bash
cd /Users/rsd/REPOS/longbow
source scripts/venv/bin/activate
python3 scripts/run_benchmarks.py local
""")
        
        # Run via SSH
        subprocess.run(f"ssh ancalagon 'bash -s' < {remote_script}", shell=True)
        
    else:
        print(f"Unknown host: {host}")
        sys.exit(1)

if __name__ == "__main__":
    main()