#!/usr/bin/env python3
import subprocess
import json
import os
import sys

SCALES = [3000, 5000, 9000, 15000, 25000]
DIM = 384
PYTHON_EXE = "venv/bin/python" if os.path.exists("venv/bin/python") else "python3"

def main():
    results = []
    
    print(f"Running benchmarks for scales: {SCALES} with dim {DIM}")
    
    for scale in SCALES:
        print(f"\n{'='*60}")
        print(f"Starting Benchmark: {scale} rows")
        print(f"{'='*60}")
        
        output_file = f"results_{scale}.json"
        
        # Using perf_test.py which handles Put, Get, Dense Search, Hybrid Search
        cmd = [
            PYTHON_EXE, "scripts/perf_test.py",
            "--rows", str(scale),
            "--dim", str(DIM),
            "--name", f"bench_{scale}",
            "--search",           # Dense Search
            "--hybrid",           # Hybrid Search
            "--json", output_file,
            "--search-k", "10",
            "--query-count", "100", # Fast enough for average
            "--hybrid-alpha", "0.5"
        ]
        
        try:
            subprocess.run(cmd, check=True)
            
            # Read results
            if os.path.exists(output_file):
                with open(output_file, 'r') as f:
                    data = json.load(f)
                    # Filter for relevant results
                    doput = next((r for r in data if r['name'] == 'DoPut'), None)
                    doget = next((r for r in data if r['name'] == 'DoGet'), None)
                    dense = next((r for r in data if r['name'] == 'VectorSearch'), None)
                    hybrid = next((r for r in data if r['name'] == 'HybridSearch'), None)
                    
                    results.append({
                        "scale": scale,
                        "doput": doput,
                        "doget": doget,
                        "dense": dense,
                        "hybrid": hybrid
                    })
        except subprocess.CalledProcessError as e:
            print(f"Error running benchmark for {scale}: {e}")
            
    # Print Summary Table
    print("\n" + "="*100)
    print(f"{'Scale':<10} {'DoPut (MB/s)':<15} {'DoGet (MB/s)':<15} {'Dense QPS':<12} {'Dense P95(ms)':<15} {'Hybrid QPS':<12} {'Hybrid P95(ms)':<15}")
    print("-" * 100)
    
    for r in results:
        scale = f"{r['scale']:,}"
        put = f"{r['doput']['throughput']:.2f}" if r['doput'] else "N/A"
        get = f"{r['doget']['throughput']:.2f}" if r['doget'] else "N/A"
        
        # Dense
        dense_qps = f"{r['dense']['throughput']:.2f}" if r['dense'] else "N/A"
        dense_p95 = f"{r['dense']['p95_ms']:.2f}" if r['dense'] else "N/A"
        
        # Hybrid
        hybrid_qps = f"{r['hybrid']['throughput']:.2f}" if r['hybrid'] else "N/A"
        hybrid_p95 = f"{r['hybrid']['p95_ms']:.2f}" if r['hybrid'] else "N/A"
        
        print(f"{scale:<10} {put:<15} {get:<15} {dense_qps:<12} {dense_p95:<15} {hybrid_qps:<12} {hybrid_p95:<15}")

    print("="*100)
    
    # Save final summary
    with open("final_benchmark_summary.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
