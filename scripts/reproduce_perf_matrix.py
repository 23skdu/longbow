#!/usr/bin/env python3
import subprocess
import json
import os
import time

TYPES = ["int8", "int16", "int32", "int64", "float32", "float16", "float64", "complex64", "complex128"]
DIMS = [128, 384]
ROWS = 15000
PYTHON_EXE = "venv/bin/python" if os.path.exists("venv/bin/python") else "python3"

def main():
    results = []
    
    print(f"Running Performance Matrix Reproduction")
    print(f"Scale: {ROWS} vectors")
    print(f"Types: {TYPES}")
    print(f"Dims: {DIMS}")
    print("="*60)
    
    for dtype in TYPES:
        for dim in DIMS:
            # Skip float16/float64 @ 128d if they are known to timeout? 
            # Docs say: "float16/float64 @ 128d timed out in the matrix run... Verified successfully in isolation".
            # We will try to run them. If they fail, we record N/A.
            
            print(f"\nBenchmark: {dtype} @ {dim}d")
            ds_name = f"bench_{dtype}_{dim}_{ROWS}"
            output_file = f"res_{dtype}_{dim}.json"
            
            cmd = [
                PYTHON_EXE, "scripts/perf_test.py",
                "--rows", str(ROWS),
                "--dim", str(dim),
                "--dtype", dtype,
                "--dataset", ds_name,
                "--search",
                "--json", output_file,
                "--k", "10",
                "--queries", "100"
            ]
            
            try:
                subprocess.run(cmd, check=True)
                
                with open(output_file, 'r') as f:
                    data = json.load(f)
                    
                    # Extract Metrics with suffix format " @ ROWS"
                    suffix = f" @ {ROWS}"
                    doput = next((r for r in data if r['name'] == f"DoPut{suffix}"), None)
                    doget = next((r for r in data if r['name'] == f"DoGet{suffix}"), None)
                    # For search, it might be VectorSearch_f32 @ ROWS or VectorSearch @ ROWS
                    search = next((r for r in data if r['name'].startswith('VectorSearch') and r['name'].endswith(suffix)), None)
                    
                    res_entry = {
                        "type": dtype,
                        "dim": dim,
                        "status": "Stable",
                        "doput_mb": doput['throughput'] if doput else 0,
                        "doget_mb": doget['throughput'] if doget else 0,
                        "dense_qps": search['throughput'] if search else 0
                    }
                    results.append(res_entry)
                    
                    print(f"RESULT: {dtype} {dim}d -> Put: {res_entry['doput_mb']:.2f} MB/s, Get: {res_entry['doget_mb']:.2f} MB/s, QPS: {res_entry['dense_qps']:.2f}")

            except subprocess.CalledProcessError as e:
                print(f"FAILED: {dtype} @ {dim}d - {e}")
                results.append({
                    "type": dtype,
                    "dim": dim,
                    "status": "Failed",
                    "doput_mb": 0, "doget_mb": 0, "dense_qps": 0
                })
            
            # Clean up
            if os.path.exists(output_file):
                os.remove(output_file)
            
            # Cool down
            time.sleep(1)

    # Save Results
    with open("matrix_results.json", "w") as f:
        json.dump(results, f, indent=2)
        
    print("\n" + "="*80)
    print("FINAL MATRIX")
    print(f"{'Type':<12} {'Dim':<5} {'DoPut (MB/s)':<15} {'DoGet (MB/s)':<15} {'QPS':<10} {'Status':<10}")
    print("-" * 80)
    for r in results:
        print(f"{r['type']:<12} {r['dim']:<5} {r['doput_mb']:<15.2f} {r['doget_mb']:<15.2f} {r['dense_qps']:<10.2f} {r['status']:<10}")
    print("="*80)

if __name__ == "__main__":
    main()
