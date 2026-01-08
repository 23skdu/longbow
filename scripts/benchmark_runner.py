#!/usr/bin/env python3
import argparse
import subprocess
import time
import json
import os
import signal
import sys
import glob

# Configuration
SIZES = [3000, 9000, 15000, 25000, 50000, 100000, 250000]
DIMS = [128, 384]
RUNS = 3
RESULTS_DIR = "benchmark_results"

def run_command(cmd, cwd=None, env=None):
    """Run a shell command and return output."""
    print(f"EXEC: {cmd}")
    try:
        subprocess.check_call(cmd, shell=True, cwd=cwd, env=env)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Command failed: {e}")
        return False

def manage_cluster(action):
    """Start or Stop/Wipe cluster."""
    if action == "start":
        print("--- Starting Cluster ---")
        # Ensure clean state
        run_command("pkill longbow || true")
        run_command("rm -rf data")
        
        # Start in background
        # We assume start_local_cluster.sh uses `go build` then runs 3 nodes.
        # We need to wait for it to be ready.
        proc = subprocess.Popen("./scripts/start_local_cluster.sh", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Wait for "Cluster started!" or similar in output, or just wait fixed time
        # The script prints "Cluster started!"
        # Let's just wait 15 seconds to be safe.
        time.sleep(15)
        return proc
    elif action == "stop":
        print("--- Stopping Cluster ---")
        run_command("pkill longbow || true")
        time.sleep(2)

def parse_benchmark_json(filepath):
    """Read perf_test.py JSON output."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to read results from {filepath}: {e}")
        return []

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true", help="Run only smallest size for testing runner")
    args = parser.parse_args()
    
    sizes = SIZES
    dims = DIMS
    if args.quick:
        sizes = [3000]
        dims = [128]

    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    summary_data = []

    for dim in dims:
        for size in sizes:
            print(f"\n=== BENCHMARK: Size={size}, Dim={dim} ===\n")
            
            run_results = {
                "size": size,
                "dim": dim,
                "runs": []
            }
            
            for run_idx in range(1, RUNS + 1):
                print(f"--- Run {run_idx}/{RUNS} ---")
                
                # 1. Restart Cluster (Fresh State)
                manage_cluster("stop")
                manage_cluster("start")
                
                # Define output files
                local_res_file = f"{RESULTS_DIR}/res_{size}_{dim}_run{run_idx}_local.json"
                global_res_file = f"{RESULTS_DIR}/res_{size}_{dim}_run{run_idx}_global.json"
                
                # 2. Run Load + Local Benchmarks
                # Measures: DoPut, DoGet, Local VectorSearch, HybridSearch
                cmd_local = (
                    f"source venv/bin/activate && python3 scripts/perf_test.py "
                    f"--rows {size} --dim {dim} "
                    f"--hybrid "    # Enable Hybrid
                    f"--search "    # Enable Vector Search
                    f"--json {local_res_file} "
                    f"--data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001"
                )
                if not run_command(cmd_local):
                    print("Run failed (Local), skipping...")
                    continue
                
                # 3. Run Global Search Benchmark
                # Skips data loading, uses existing data
                cmd_global = (
                    f"source venv/bin/activate && python3 scripts/perf_test.py "
                    f"--rows {size} --dim {dim} "
                    f"--skip-data " # Skip loading
                    f"--search "    # Enable Vector Search
                    f"--global "    # Force GLOBAL search
                    f"--json {global_res_file} "
                    f"--data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001"
                )
                if not run_command(cmd_global):
                    print("Run failed (Global), skipping...")
                    continue
                
                # Collect Results
                local_data = parse_benchmark_json(local_res_file)
                global_data = parse_benchmark_json(global_res_file)
                
                # Merge logic
                # expected keys in run_metrics: 'input_throughput', 'search_qps', 'hybrid_qps', 'global_qps', etc.
                metrics = {}
                
                # Extract from Local run
                for res in local_data:
                    if res["name"] == "DoPut":
                        metrics["put_mb_s"] = res["throughput"] # MB/s
                        metrics["put_rows_s"] = res["rows"] / res["duration_seconds"]
                    elif res["name"] == "DoGet":
                        metrics["get_mb_s"] = res["throughput"]
                    elif res["name"] == "VectorSearch":
                        metrics["local_search_qps"] = res["throughput"]
                        metrics["local_search_p95"] = res["p95_ms"]
                    elif res["name"] == "HybridSearch":
                        metrics["hybrid_search_qps"] = res["throughput"]
                        metrics["hybrid_search_p95"] = res["p95_ms"]

                # Extract from Global run
                for res in global_data:
                    if res["name"] == "VectorSearch":
                        metrics["global_search_qps"] = res["throughput"]
                        metrics["global_search_p95"] = res["p95_ms"]

                run_results["runs"].append(metrics)
                
            summary_data.append(run_results)
            
            # Save intermediate summary
            with open(f"{RESULTS_DIR}/summary.json", 'w') as f:
                json.dump(summary_data, f, indent=2)

    print("\nBenchmark Suite Complete.")
    manage_cluster("stop")

if __name__ == "__main__":
    main()
