#!/usr/bin/env python3
"""
Full Benchmark Suite Runner for Remote Testbed (8 Workers, No CPU Pinning)
Executes all benchmarks across dimensions (128, 384, 768), scales (100k, 500k, 1M),
Standard vs EMLGo builds, with 8 workers, full pprof profiling, across all 13 search modes.
"""

import os
import sys
import subprocess
import time
from datetime import datetime

ALL_DTYPES = [
    "float32", "float64", "float16",
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "complex64", "complex128",
    "turboquant"
]

SCALE_500K_DTYPES = [
    "float32", "float64", "float16",
    "int8", "int16",
    "complex64", "complex128",
    "turboquant"
]

SCALE_1M_DTYPES = [
    "float32", "int8", "complex128", "turboquant"
]

DIMENSIONS = [128, 384, 768]

def run_cmd(cmd):
    print(f"\n[RUNNING] {cmd}")
    sys.stdout.flush()
    res = subprocess.run(cmd, shell=True)
    if res.returncode != 0:
        print(f"[WARNING] Command returned {res.returncode}")
    sys.stdout.flush()
    return res.returncode

def run_dimension_suite(dim):
    print(f"\n==================================================================")
    print(f"=== STARTING SUITE FOR DIMENSION {dim} (8 WORKERS, UNPINNED) ===")
    print(f"==================================================================")

    # 1. 100k Scale - All 14 dtypes for Standard and EMLGo
    print(f"\n>>> Dim {dim} | 100,000 Vectors (Standard vs EMLGo, All 14 Dtypes) <<<")
    dtypes_100k_str = ",".join(ALL_DTYPES)
    for variant, binary, extra_flags in [
        ("standard", "bin/longbow_main", ""),
        ("emlgo", "bin/longbow_emlgo", "--emlgo")
    ]:
        cmd = (
            f"python3 scripts/unified_benchmark.py --mode cpu "
            f"--server-binary {binary} {extra_flags} "
            f"--counts 100000 --dims {dim} --dtypes {dtypes_100k_str} "
            f"--queries 500 --workers 8 "
            f"--search-modes all --pprof --label {variant}_100k_dim{dim}_w8"
        )
        run_cmd(cmd)

    # 2. 500k Scale - Core dtypes for Standard and EMLGo
    print(f"\n>>> Dim {dim} | 500,000 Vectors (Standard vs EMLGo) <<<")
    dtypes_500k_str = ",".join(SCALE_500K_DTYPES if dim == 128 else SCALE_1M_DTYPES)
    for variant, binary, extra_flags in [
        ("standard", "bin/longbow_main", ""),
        ("emlgo", "bin/longbow_emlgo", "--emlgo")
    ]:
        cmd = (
            f"python3 scripts/unified_benchmark.py --mode cpu "
            f"--server-binary {binary} {extra_flags} "
            f"--counts 500000 --dims {dim} --dtypes {dtypes_500k_str} "
            f"--queries 500 --workers 8 "
            f"--search-modes all --pprof --label {variant}_500k_dim{dim}_w8"
        )
        run_cmd(cmd)

    # 3. 1,000,000 Scale - Representative dtypes for Standard and EMLGo
    print(f"\n>>> Dim {dim} | 1,000,000 Vectors (Standard vs EMLGo) <<<")
    dtypes_1m_str = ",".join(SCALE_1M_DTYPES)
    for variant, binary, extra_flags in [
        ("standard", "bin/longbow_main", ""),
        ("emlgo", "bin/longbow_emlgo", "--emlgo")
    ]:
        cmd = (
            f"python3 scripts/unified_benchmark.py --mode cpu "
            f"--server-binary {binary} {extra_flags} "
            f"--counts 1000000 --dims {dim} --dtypes {dtypes_1m_str} "
            f"--queries 500 --workers 8 "
            f"--search-modes all --pprof --label {variant}_1m_dim{dim}_w8"
        )
        run_cmd(cmd)

def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(repo_root)

    print("==================================================================")
    print("STARTING LONGBOW FULL VALIDATION BENCHMARK SUITE")
    print("CONFIGURATION: 8 WORKERS, UNPINNED CPU AFFINITY, AVX512+FMA XEON")
    print(f"Time: {datetime.now()}")
    print("==================================================================")

    for dim in DIMENSIONS:
        run_dimension_suite(dim)

    print("\n==================================================================")
    print(f"ALL DIMENSIONS (128, 384, 768) AND SCALES COMPLETED AT {datetime.now()}!")
    print("==================================================================")

if __name__ == "__main__":
    main()
