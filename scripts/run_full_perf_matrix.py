#!/usr/bin/env python3
"""
Full Performance Matrix Benchmark
Tests DoPut, DoGet, and Indexing Time for all dtype/dimension/count combinations
"""

import subprocess
import json
import time
import sys
import os
from datetime import datetime

DIMS = [128, 384]
DTYPES = [
    "float32",
    "float64",
    "int8",
    "int16",
    "int32",
    "uint32",
    "complex64",
    "complex128",
]
COUNTS = [1000, 3000, 5000, 7000, 10000, 15000, 20000, 25000]
URI = "127.0.0.1:3000"
BENCHMARK_TOOL = "./bin/benchmark-tool"

results = []


def run_benchmark(dim, dtype, count):
    cmd = [
        BENCHMARK_TOOL,
        "--uri",
        URI,
        "--dim",
        str(dim),
        "--dtype",
        dtype,
        "--scale",
        str(count),
        "--queries",
        "100",
        "--json",
        f"/tmp/bench_{dtype}_{dim}_{count}.json",
    ]

    print(f"Running: dtype={dtype}, dim={dim}, count={count}")
    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"  FAILED: {result.stderr}")
        return None

    try:
        with open(f"/tmp/bench_{dtype}_{dim}_{count}.json", "r") as f:
            data = json.load(f)
        os.remove(f"/tmp/bench_{dtype}_{dim}_{count}.json")
        return data
    except:
        return None


def main():
    print("=" * 80)
    print("FULL PERFORMANCE MATRIX BENCHMARK")
    print(f"Started: {datetime.now()}")
    print("=" * 80)

    total_runs = len(DIMS) * len(DTYPES) * len(COUNTS)
    current = 0

    for dim in DIMS:
        for dtype in DTYPES:
            for count in COUNTS:
                current += 1
                print(f"\n[{current}/{total_runs}] {dtype} dim={dim} count={count}")

                data = run_benchmark(dim, dtype, count)
                if data:
                    # Extract key metrics
                    result_entry = {
                        "dtype": dtype,
                        "dim": dim,
                        "count": count,
                    }

                    for item in data:
                        if item["name"] == "DoPut":
                            result_entry["doput_vec_s"] = item["throughput"]
                            result_entry["doput_mb_s"] = item["throughput_mbs"]
                            result_entry["doput_duration_s"] = item["duration_seconds"]
                        elif item["name"] == "DoGet":
                            result_entry["doget_vec_s"] = item["throughput"]
                            result_entry["doget_mb_s"] = item["throughput_mbs"]
                            result_entry["doget_duration_s"] = item["duration_seconds"]
                        elif "Search" in item["name"] and "Dense" in item["name"]:
                            result_entry["dense_qps"] = item["throughput"]
                            result_entry["dense_p50_ms"] = item["p50_latency_ms"]
                            result_entry["dense_p99_ms"] = item["p99_latency_ms"]

                    # Extract indexing time from log output (done via timing)
                    # We estimate indexing from total time minus DoPut minus DoGet
                    # But actually we should calculate it from when indexing starts
                    # For now, we capture the timing from the benchmark output

                    results.append(result_entry)
                    print(f"  DoPut: {result_entry.get('doput_mb_s', 'N/A'):.2f} MB/s")
                    print(f"  DoGet: {result_entry.get('doget_mb_s', 'N/A'):.2f} MB/s")
                    print(f"  Dense QPS: {result_entry.get('dense_qps', 'N/A'):.2f}")
                else:
                    results.append(
                        {
                            "dtype": dtype,
                            "dim": dim,
                            "count": count,
                            "error": "benchmark failed",
                        }
                    )

    # Save results
    with open("benchmark_results_full.json", "w") as f:
        json.dump(results, f, indent=2)

    print("\n" + "=" * 80)
    print(f"Completed: {datetime.now()}")
    print(f"Results saved to benchmark_results_full.json")
    print("=" * 80)


if __name__ == "__main__":
    main()
