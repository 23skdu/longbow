#!/usr/bin/env python3
"""
Validation Matrix Runner for Remote Testbed
Executes CPU benchmarks for non-emlgo and emlgo binaries across
100k, 500k, and 1M vector counts with --pprof enabled.
"""

import os
import sys
import subprocess
import json
import glob
import time
from datetime import datetime

ALL_DTYPES = [
    "float32", "float64", "float16",
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "complex64", "complex128",
    "turboquant"
]

SCALES = [100000, 500000, 1000000]

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
    print("STARTING LONGBOW BENCHMARK VALIDATION MATRIX ON REMOTE HOST")
    print(f"Time: {datetime.now()}")
    print("==================================================================")

    # 1. 100k Run - All dtypes for standard and emlgo
    print("\n>>> PHASE 1: 100,000 Vectors (Standard vs EMLGo, All Dtypes) <<<")
    for variant, binary, extra_flags in [
        ("standard", "bin/longbow_main", ""),
        ("emlgo", "bin/longbow_emlgo", "--emlgo")
    ]:
        # For 100k, run all dtypes
        dtypes_str = ",".join(ALL_DTYPES)
        cmd = (
            f"python3 scripts/unified_benchmark.py --mode cpu "
            f"--server-binary {binary} {extra_flags} "
            f"--counts 100000 --dims 128 --dtypes {dtypes_str} "
            f"--queries 500 --workers 4 --cpu-affinity 12-15 "
            f"--search-modes all --pprof --label {variant}_100k"
        )
        run_cmd(cmd)

    # 2. 500k Run - Standard and EMLGo
    print("\n>>> PHASE 2: 500,000 Vectors (Standard vs EMLGo) <<<")
    # At 500k, run the core representative dtypes across float, int, complex, TQ
    scale_500k_dtypes = ["float32", "float64", "float16", "int8", "int16", "complex64", "complex128", "turboquant"]
    dtypes_500k_str = ",".join(scale_500k_dtypes)
    for variant, binary, extra_flags in [
        ("standard", "bin/longbow_main", ""),
        ("emlgo", "bin/longbow_emlgo", "--emlgo")
    ]:
        cmd = (
            f"python3 scripts/unified_benchmark.py --mode cpu "
            f"--server-binary {binary} {extra_flags} "
            f"--counts 500000 --dims 128 --dtypes {dtypes_500k_str} "
            f"--queries 500 --workers 4 --cpu-affinity 12-15 "
            f"--search-modes all --pprof --label {variant}_500k"
        )
        run_cmd(cmd)

    # 3. 1,000,000 Run - Standard and EMLGo
    print("\n>>> PHASE 3: 1,000,000 Vectors (Standard vs EMLGo) <<<")
    scale_1m_dtypes = ["float32", "int8", "complex128", "turboquant"]
    dtypes_1m_str = ",".join(scale_1m_dtypes)
    for variant, binary, extra_flags in [
        ("standard", "bin/longbow_main", ""),
        ("emlgo", "bin/longbow_emlgo", "--emlgo")
    ]:
        cmd = (
            f"python3 scripts/unified_benchmark.py --mode cpu "
            f"--server-binary {binary} {extra_flags} "
            f"--counts 1000000 --dims 128 --dtypes {dtypes_1m_str} "
            f"--queries 500 --workers 4 --cpu-affinity 12-15 "
            f"--search-modes all --pprof --label {variant}_1m"
        )
        run_cmd(cmd)

    print("\n==================================================================")
    print("ALL BENCHMARK PHASES COMPLETED!")
    print("==================================================================")

if __name__ == "__main__":
    main()
