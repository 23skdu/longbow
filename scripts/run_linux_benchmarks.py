#!/usr/bin/env python3
"""
Longbow Linux Performance Benchmark Suite
Tests: DoPut, DoGet, Vector Search (4 variants)
Dimensions: 128, 384, 768
Dataset Sizes: 3K, 7K, 15K, 25K
"""

import subprocess
import time
import json
import sys
import os

SIZES = [3000, 7000, 15000, 25000]
DIMS = [128, 384, 768]
DATA_URI = "grpc://localhost:3000"
META_URI = "grpc://localhost:3001"
DATASET = "perf_bench"


def check_cluster():
    """Check if cluster is running"""
    try:
        result = subprocess.run(
            [
                "python3",
                "-c",
                "import pyarrow.flight as flight; "
                "c = flight.FlightClient('grpc://localhost:3001'); "
                "print('OK')",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return "OK" in result.stdout
    except:
        return False


def run_benchmark(dim, rows, test_type):
    """Run a single benchmark test"""
    args = [
        "python3",
        "scripts/perf_test.py",
        "--dataset",
        f"{DATASET}_{dim}_{rows}",
        "--rows",
        str(rows),
        "--dim",
        str(dim),
        "--data-uri",
        DATA_URI,
        "--meta-uri",
        META_URI,
    ]

    if test_type == "doput":
        args.extend(["--skip-search", "--skip-get"])
    elif test_type == "doget":
        args.extend(["--skip-put", "--skip-search"])
    elif test_type == "search":
        args.extend(["--search"])
    elif test_type == "hybrid":
        args.extend(["--hybrid", "--search"])
    elif test_type == "global":
        args.extend(["--skip-put", "--search", "--global"])
    elif test_type == "id-search":
        args.extend(["--search", "--test-id-search"])

    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=300)
        return parse_output(result.stdout + result.stderr)
    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    except Exception as e:
        return {"error": str(e)}


def parse_output(output):
    """Parse benchmark output for metrics"""
    metrics = {}
    for line in output.split("\n"):
        if "throughput" in line.lower():
            # Try to extract throughput
            parts = line.split()
            for i, p in enumerate(parts):
                if "MB/s" in p or "vectors/sec" in p or "QPS" in p:
                    try:
                        val = float(parts[i - 1].replace(",", ""))
                        metrics[p] = val
                    except:
                        pass
    return metrics


def main():
    if not check_cluster():
        print("Error: Cluster not running. Start with ./scripts/start_local_cluster.sh")
        sys.exit(1)

    results = {}

    for dim in DIMS:
        for size in SIZES:
            key = f"d{dim}_s{size}"
            print(f"\n=== Benchmark: dim={dim}, size={size} ===")

            # DoPut
            print("  Running DoPut...")
            results[f"{key}_doput"] = run_benchmark(dim, size, "doput")

            # DoGet
            print("  Running DoGet...")
            results[f"{key}_doget"] = run_benchmark(dim, size, "doget")

            # Search
            print("  Running Search...")
            results[f"{key}_search"] = run_benchmark(dim, size, "search")

            # Hybrid Search
            print("  Running Hybrid Search...")
            results[f"{key}_hybrid"] = run_benchmark(dim, size, "hybrid")

            # Global Search
            print("  Running Global Search...")
            results[f"{key}_global"] = run_benchmark(dim, size, "global")

            # ID Search
            print("  Running ID Search...")
            results[f"{key}_id_search"] = run_benchmark(dim, size, "id-search")

    # Save results
    with open("benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print("\n\nResults saved to benchmark_results.json")
    print("Generate markdown with: python3 scripts/format_perf_tables.py")


if __name__ == "__main__":
    main()
