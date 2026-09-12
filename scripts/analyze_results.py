#!/usr/bin/env python3
"""Analyze benchmark results and generate performance docs."""
import json
import os
import glob
from datetime import datetime

def load_results(pattern):
    """Load all perf_matrix JSON files matching pattern."""
    results = {}
    for f in sorted(glob.glob(pattern)):
        with open(f) as fh:
            data = json.load(fh)
            label = data.get("config", {}).get("label", os.path.basename(f))
            results[f] = data
    return results

def extract_search_metrics(result):
    """Extract QPS for each search mode from a result entry."""
    search = result.get("search", {})
    return {mode: data.get("qps", 0) for mode, data in search.items()}

def main():
    log_dir = "data/perf_logs"
    # Find all perf_matrix files
    files = sorted(glob.glob(f"{log_dir}/perf_matrix_*.json"))
    print(f"Found {len(files)} result files")
    for f in files:
        with open(f) as fh:
            data = json.load(fh)
            n = len(data.get("results", []))
            label = os.path.basename(f)
            print(f"  {label}: {n} results")

    # Group by config label (cpu_standard, cpu_emlgo, gpu_standard, gpu_emlgo)
    configs = {}
    for f in files:
        with open(f) as fh:
            data = json.load(fh)
        mode = data.get("mode", "unknown")
        results = data.get("results", [])
        # Determine label from filename
        basename = os.path.basename(f)
        if "cpu_standard" in basename or ("cpu" in basename and "emlgo" not in basename):
            label = "CPU Standard"
        elif "cpu_emlgo" in basename:
            label = "CPU Emlgo"
        elif "gpu_standard" in basename or ("cuda" in basename and "emlgo" not in basename):
            label = "GPU Standard"
        elif "gpu_emlgo" in basename:
            label = "GPU Emlgo"
        else:
            label = basename
        if label not in configs:
            configs[label] = []
        configs[label].extend(results)

    for label, results in configs.items():
        print(f"\n{label}: {len(results)} result entries")
        for r in results:
            dtype = r.get("dtype", "?")
            count = r.get("count", "?")
            peak = r.get("peak_memory_mb", 0)
            search = r.get("search", {})
            modes = list(search.keys())
            print(f"  {dtype} {count}: peak={peak:.0f}MB modes={modes}")

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    main()
