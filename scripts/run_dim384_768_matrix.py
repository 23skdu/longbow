#!/usr/bin/env python3
"""
High-Dimensional Benchmark Matrix Runner (dim=384 and dim=768)
Runs fresh tests across 100k, 500k, and 1M vector counts for
Standard and EMLGo versions across all 13 search modes with pprof.
"""

import os
import sys
import subprocess
from datetime import datetime

DIMS = [384, 768]
DTYPES_100K = [
    "float32", "float64", "float16",
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "complex64", "complex128",
    "turboquant"
]
DTYPES_SCALE = ["float32", "int8", "complex128", "turboquant"]

def run_cmd(cmd):
    print(f"\n[RUNNING] {cmd}")
    sys.stdout.flush()
    res = subprocess.run(cmd, shell=True)
    if res.returncode != 0:
        print(f"[WARNING] Command returned {res.returncode}")
    sys.stdout.flush()
    return res.returncode

def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(repo_root)

    print("==================================================================")
    print("STARTING FRESH HIGH-DIMENSIONAL BENCHMARK MATRIX (DIM 384 & 768)")
    print(f"Time: {datetime.now()}")
    print("==================================================================")

    for dim in DIMS:
        print(f"\n##################################################################")
        print(f"### EXECUTING FOR DIMENSION {dim}")
        print(f"##################################################################")

        # 1. 100,000 vectors for dim
        dtypes_100k_str = ",".join(DTYPES_100K)
        for variant, binary, extra_flags in [
            ("standard", "bin/longbow_main", ""),
            ("emlgo", "bin/longbow_emlgo", "--emlgo")
        ]:
            cmd = (
                f"python3 scripts/unified_benchmark.py --mode cpu "
                f"--server-binary {binary} {extra_flags} "
                f"--counts 100000 --dims {dim} --dtypes {dtypes_100k_str} "
                f"--queries 500 --workers 4 --cpu-affinity 12-15 "
                f"--search-modes all --pprof --label {variant}_100k_dim{dim}"
            )
            run_cmd(cmd)

        # 2. 500,000 vectors for dim
        dtypes_scale_str = ",".join(DTYPES_SCALE)
        for variant, binary, extra_flags in [
            ("standard", "bin/longbow_main", ""),
            ("emlgo", "bin/longbow_emlgo", "--emlgo")
        ]:
            cmd = (
                f"python3 scripts/unified_benchmark.py --mode cpu "
                f"--server-binary {binary} {extra_flags} "
                f"--counts 500000 --dims {dim} --dtypes {dtypes_scale_str} "
                f"--queries 500 --workers 4 --cpu-affinity 12-15 "
                f"--search-modes all --pprof --label {variant}_500k_dim{dim}"
            )
            run_cmd(cmd)

        # 3. 1,000,000 vectors for dim
        for variant, binary, extra_flags in [
            ("standard", "bin/longbow_main", ""),
            ("emlgo", "bin/longbow_emlgo", "--emlgo")
        ]:
            cmd = (
                f"python3 scripts/unified_benchmark.py --mode cpu "
                f"--server-binary {binary} {extra_flags} "
                f"--counts 1000000 --dims {dim} --dtypes {dtypes_scale_str} "
                f"--queries 500 --workers 4 --cpu-affinity 12-15 "
                f"--search-modes all --pprof --label {variant}_1m_dim{dim}"
            )
            run_cmd(cmd)

    print("\n==================================================================")
    print("ALL HIGH-DIMENSIONAL BENCHMARK RUNS COMPLETED!")
    print("==================================================================")

if __name__ == "__main__":
    main()
