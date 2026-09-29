#!/usr/bin/env python3
"""
Orchestrates benchmark runs across:
- Scales: 10k, 50k, 250k, 1M
- Dims: 128, 384
- Data Types: int8, float32, complex128, turboquant4
- Search Types: all 9 vec modes (dense, hybrid, sparse, filtered, byid, graphrag, geo, temporal, learned_index)
- 4 Engines:
  1. CPU standard (bin/longbow_main)
  2. GPU standard (bin/longbow-cuda_main)
  3. CPU EMLGo (bin/longbow_emlgo)
  4. GPU EMLGo (bin/longbow-cuda_emlgo)
- Workers: 4 (pinned to unthrottled cores 12-15)
- Saves baseline JSONs to benchmarks/
- Overwrites docs/performance.md
- Updates docs/roadmap.md with 10 performance steps based on empirical pprof profiles.
"""

import os
import sys
import json
import time
import subprocess
import glob

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCHMARKS_DIR = os.path.join(REPO_ROOT, "benchmarks")
PROFILES_DIR = os.path.join(REPO_ROOT, "profiles")
DOCS_DIR = os.path.join(REPO_ROOT, "docs")

VARIANTS = [
    {
        "name": "cpu_main",
        "label": "CPU Standard",
        "mode": "cpu",
        "binary": "bin/longbow_main",
        "emlgo": False,
        "output_json": os.path.join(BENCHMARKS_DIR, "run_cpu_main.json"),
    },
    {
        "name": "cuda_main",
        "label": "GPU Standard",
        "mode": "cuda",
        "binary": "bin/longbow-cuda_main",
        "emlgo": False,
        "output_json": os.path.join(BENCHMARKS_DIR, "run_cuda_main.json"),
    },
    {
        "name": "cpu_emlgo",
        "label": "CPU EMLGo",
        "mode": "cpu",
        "binary": "bin/longbow_emlgo",
        "emlgo": True,
        "output_json": os.path.join(BENCHMARKS_DIR, "run_cpu_emlgo.json"),
    },
    {
        "name": "cuda_emlgo",
        "label": "GPU EMLGo",
        "mode": "cuda",
        "binary": "bin/longbow-cuda_emlgo",
        "emlgo": True,
        "output_json": os.path.join(BENCHMARKS_DIR, "run_cuda_emlgo.json"),
    },
]

def run_variant(v, counts, dims, dtypes, queries=25):
    print(f"\n=======================================================")
    print(f" Starting Variant: {v['label']} ({v['name']})")
    print(f"=======================================================")
    cmd = [
        sys.executable,
        os.path.join(REPO_ROOT, "scripts", "unified_benchmark.py"),
        "--mode", v["mode"],
        "--server-binary", v["binary"],
        "--dims", ",".join(str(d) for d in dims),
        "--counts", ",".join(str(c) for c in counts),
        "--dtypes", ",".join(dtypes),
        "--workers", "4",
        "--cpu-affinity", "12-15",
        "--search-modes", "all",
        "--pprof",
        "--queries", str(queries),
        "--save-baseline", v["output_json"],
    ]
    if v["emlgo"]:
        cmd.append("--emlgo")
    
    print(f"Executing: {' '.join(cmd)}")
    sys.stdout.flush()
    start_t = time.time()
    res = subprocess.run(cmd, cwd=REPO_ROOT)
    elapsed = time.time() - start_t
    print(f"\nFinished {v['label']} in {elapsed:.1f}s with return code {res.returncode}")
    return res.returncode == 0

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", choices=["all", "cpu_main", "cuda_main", "cpu_emlgo", "cuda_emlgo"], default="all")
    parser.add_argument("--counts", default="10000,50000,250000,1000000")
    parser.add_argument("--dims", default="128,384")
    parser.add_argument("--dtypes", default="int8,float32,complex128,turboquant4")
    parser.add_argument("--queries", type=int, default=25)
    args = parser.parse_args()

    counts = [int(c) for c in args.counts.split(",")]
    dims = [int(d) for d in args.dims.split(",")]
    dtypes = args.dtypes.split(",")

    os.makedirs(BENCHMARKS_DIR, exist_ok=True)
    os.makedirs(PROFILES_DIR, exist_ok=True)

    for v in VARIANTS:
        if args.variant != "all" and v["name"] != args.variant:
            continue
        run_variant(v, counts, dims, dtypes, queries=args.queries)
