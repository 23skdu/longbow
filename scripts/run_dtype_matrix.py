#!/usr/bin/env python3
import subprocess
import json
import os
import time
import sys
import pandas as pd
from datetime import datetime

TYPES = ["int8", "int16", "int32", "int64", "float16", "float32", "float64", "complex64", "complex128"]
DIMS = [128, 384]
ROWS = 15000
RESULTS_FILE = "matrix_results.json"

def run_test(dtype, dim):
    dataset = f"bench_{dtype}_{dim}"
    json_path = f"res_{dtype}_{dim}.json"
    
    # Check if result already exists and is valid
    if os.path.exists(json_path):
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
            print(f"SKIPPING RUN: {dtype} @ {dim}d (Found {json_path})")
            return data
        except:
            pass # re-run if corrupted

    cmd = [
        "python3", "scripts/perf_test.py",
        "--dataset", dataset,
        "--dtype", dtype,
        "--dim", str(dim),
        "--rows", str(ROWS),
        "--with-text",  # Enable search variants
        "--json", json_path,
        "--skip-get" if False else "", # Run everything
        # Just use defaults for k, queries
    ]
    # filter empty args
    cmd = [c for c in cmd if c]
    
    print(f"--> Running: {dtype} @ {dim}d ...")
    start = time.time()
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if res.returncode != 0:
            print(f"FAILED: {dtype} {dim}")
            print(res.stderr)
            return None
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT: {dtype} {dim}")
        return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None
        
    if not os.path.exists(json_path):
        print(f"NO RESULT: {json_path}")
        return None
        
    with open(json_path, "r") as f:
        data = json.load(f)
        
    return data

def main():
    print(f"Starting Matrix Benchmark: {len(TYPES)} types x {len(DIMS)} dims")
    
    # Load existing results if any
    all_records = []
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE, "r") as f:
                all_records = json.load(f)
            print(f"Loaded {len(all_records)} existing results.")
        except:
            print("Could not load existing results, starting fresh.")
            
    # Create set of completed (dtype, dim)
    completed = set()
    for r in all_records:
        completed.add((r["Type"], r["Dim"]))
    
    for dtype in TYPES:
        for dim in DIMS:
            if (dtype, dim) in completed:
                print(f"SKIPPING: {dtype} @ {dim}d (Already done)")
                continue

            # Run with timeout
            try:
                # Wrap run_test logic here to handle timeout/save
                # We reuse run_test but add timeout param inside? 
                # run_test uses subprocess.run. Let's make it accept timeout.
                # Or just modify run_test below.
                pass 
            except Exception as e:
                pass 
            
            # Actually, let's just call run_test and modify run_test to have timeout
            results = run_test(dtype, dim)
            
            rec = {
                "Type": dtype,
                "Dim": dim,
                "DoPut (MB/s)": 0,
                "DoGet (MB/s)": 0,
                "Dense QPS": 0,
                "Dense p95": 0,
                "Sparse QPS": 0,
                "Hybrid QPS": 0, 
                "Filtered QPS": 0,
                "Status": "OK"
            }
            
            if not results:
                rec["Status"] = "Failed/Timeout"
            else:
                for r in results:
                    if "DoPut" in r["name"]:
                        rec["DoPut (MB/s)"] = round(r["throughput"], 2)
                    elif "DoGet" in r["name"]:
                        rec["DoGet (MB/s)"] = round(r["throughput"], 2)
                    elif "VectorSearch" in r["name"] and "Filtered" not in r["name"] and "Sparse" not in r["name"]:
                        rec["Dense QPS"] = round(r["throughput"], 2)
                        rec["Dense p95"] = round(r["p95_ms"], 2)
                    elif "SparseSearch" in r["name"]:
                        rec["Sparse QPS"] = round(r["throughput"], 2)
                    elif "HybridSearch" in r["name"]:
                        rec["Hybrid QPS"] = round(r["throughput"], 2)
                    elif "FilteredSearch" in r["name"]:
                        rec["Filtered QPS"] = round(r["throughput"], 2)
                    
                    if r["errors"] > 0:
                        rec["Status"] = "Errors"
            
            print(f"   DONE: Put={rec['DoPut (MB/s)']} Get={rec['DoGet (MB/s)']} DenseQPS={rec['Dense QPS']}")
            all_records.append(rec)
            
            # Incremental Save
            with open(RESULTS_FILE, "w") as f:
                json.dump(all_records, f, indent=2)

    # Generate Markdown Table (same as before)
    df = pd.DataFrame(all_records)
    cols = ["Type", "Dim", "DoPut (MB/s)", "DoGet (MB/s)", "Dense QPS", "Dense p95", "Sparse QPS", "Hybrid QPS", "Filtered QPS", "Status"]
    # Ensure all cols exist
    for c in cols:
        if c not in df.columns: df[c] = 0
        
    md_table = df[cols].to_markdown(index=False)
    
    print("\n" + "="*80)
    print("RESULTS MATRIX")
    print("="*80)
    print(md_table)
    
    # Generate Output for Docs
    doc = f"""
# Performance Matrix - Data Types

**Date**: {datetime.now().strftime("%Y-%m-%d")}
**Vectors per Run**: {ROWS:,}
**Hardware**: 3-Node Cluster (Docker)

{md_table}

*Note: 'Errors' status indicates partial failure during the run (e.g. search timeouts or connection resets).*
"""
    with open("matrix_report.md", "w") as f:
        f.write(doc)
    print("\nReport saved to matrix_report.md")

if __name__ == "__main__":
    main()
