#!/usr/bin/env python3
import subprocess
import json
import os
import time

SCALES = [3000, 5000, 9000, 15000]
DIM = 384
PYTHON_EXE = "venv/bin/python" if os.path.exists("venv/bin/python") else "python3"

def run_bench(rows, name, extra_args):
    output_file = f"results_{name}_{rows}.json"
    cmd = [
        PYTHON_EXE, "scripts/perf_test.py",
        "--rows", str(rows),
        "--dim", str(DIM),
        "--name", f"bench_{rows}",
        "--json", output_file,
        "--search-k", "10",
        "--query-count", "100",
    ] + extra_args
    
    # We only want to run Put/Get once per scale to avoid redundant indexing
    # But perf_test.py runs them by default unless --skip-data is used.
    # However, for the first run of a scale, we MUST run data ops.
    
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            return json.load(f)
    return None

def main():
    final_results = {}
    
    for scale in SCALES:
        print(f"\n{'='*60}")
        print(f"SCALE: {scale}")
        print(f"{'='*60}")
        
        scale_results = {}
        
        # 1. Base run (Put, Get, Dense Search)
        # We'll use this to get Put/Get throughput and Dense QPS
        res = run_bench(scale, "base", ["--search", "--hybrid", "--hybrid-alpha", "1.0"])
        if res:
            scale_results["doput"] = next((r for r in res if r['name'] == 'DoPut'), None)
            scale_results["doget"] = next((r for r in res if r['name'] == 'DoGet'), None)
            scale_results["dense"] = next((r for r in res if r['name'] == 'VectorSearch'), None)

        # 2. Sparse Search (Hybrid with Alpha 0.0)
        # We use --skip-data to reuse the existing dataset
        res = run_bench(scale, "sparse", ["--hybrid", "--hybrid-alpha", "0.0", "--skip-data"])
        if res:
            scale_results["sparse"] = next((r for r in res if r['name'] == 'HybridSearch'), None)
            
        # 3. Filtered Search (Dense with Metadata Filter)
        # We'll assume the text field 'meta' from generate_vectors is indexed (via LONGBOW_HYBRID_TEXT_COLUMNS=meta)
        # We'll filter for a random word.
        # Note: perf_test.py generate_vectors puts words in 'meta' field.
        res = run_bench(scale, "filtered", ["--search", "--filter", "meta:=:machine", "--skip-data"])
        if res:
            scale_results["filtered"] = next((r for r in res if r['name'] == 'VectorSearch'), None)

        # 4. Hybrid Search (Dense + Sparse with Alpha 0.5)
        res = run_bench(scale, "hybrid", ["--hybrid", "--hybrid-alpha", "0.5", "--skip-data"])
        if res:
            scale_results["hybrid"] = next((r for r in res if r['name'] == 'HybridSearch'), None)
            
        final_results[scale] = scale_results

    # Print Summary Table
    print("\n" + "="*120)
    print(f"{'Scale':<8} | {'Put (MB/s)':<12} | {'Get (MB/s)':<12} | {'Dense QPS':<10} | {'Sparse QPS':<10} | {'Filtered QPS':<12} | {'Hybrid QPS':<10}")
    print("-" * 120)
    
    for scale in SCALES:
        r = final_results.get(scale, {})
        put = f"{r.get('doput', {}).get('throughput', 0):.2f}" if r.get('doput') else "N/A"
        get = f"{r.get('doget', {}).get('throughput', 0):.2f}" if r.get('doget') else "N/A"
        dense = f"{r.get('dense', {}).get('throughput', 0):.2f}" if r.get('dense') else "N/A"
        sparse = f"{r.get('sparse', {}).get('throughput', 0):.2f}" if r.get('sparse') else "N/A"
        filtered = f"{r.get('filtered', {}).get('throughput', 0):.2f}" if r.get('filtered') else "N/A"
        hybrid = f"{r.get('hybrid', {}).get('throughput', 0):.2f}" if r.get('hybrid') else "N/A"
        
        print(f"{scale:<8} | {put:<12} | {get:<12} | {dense:<10} | {sparse:<10} | {filtered:<12} | {hybrid:<10}")

    print("="*120)
    
    with open("benchmark_12gb_final.json", "w") as f:
        json.dump(final_results, f, indent=2)

if __name__ == "__main__":
    main()
