#!/usr/bin/env python3
import subprocess
import json
import os
import time

DTYPES = ["int8", "int16", "int32", "int64", "float32", "float16", "float64", "complex64", "complex128"]
DIMS = [128, 384]
ROWS = 15000
PYTHON_EXE = "./venv/bin/python"

def run_bench(dtype, dim, rows):
    print(f"Benchmarking {dtype} @ {dim}d ({rows} vectors)...")
    output_file = f"matrix_{dtype}_{dim}.json"
    
    cmd = [
        PYTHON_EXE, "scripts/perf_test.py",
        "--dtype", dtype,
        "--dim", str(dim),
        "--rows", str(rows),
        "--dataset", f"matrix_{dtype}_{dim}",
        "--search",
        "--json", output_file,
        "--queries", "500",
        "--k", "10"
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                data = json.load(f)
                # Find metrics
                put = next((r for r in data if r['name'].startswith('DoPut')), None)
                get = next((r for r in data if r['name'].startswith('DoGet')), None)
                search = next((r for r in data if r['name'].startswith('VectorSearch')), None)
                
                return {
                    "put_mb": put['throughput'] if put else 0,
                    "get_mb": get['throughput'] if get else 0,
                    "search_qps": search['throughput'] if search else 0,
                    "status": "Stable"
                }
    except Exception as e:
        print(f"  Error {dtype}@{dim}d: {e}")
        return {"put_mb": 0, "get_mb": 0, "search_qps": 0, "status": "Error"}
    
    return {"put_mb": 0, "get_mb": 0, "search_qps": 0, "status": "Error"}

def main():
    matrix = []
    
    for dim in DIMS:
        for dtype in DTYPES:
            res = run_bench(dtype, dim, ROWS)
            matrix.append({
                "type": dtype,
                "dim": dim,
                **res
            })
            
    # Print results
    print("\n| Type | Dim | DoPut (MB/s) | DoGet (MB/s) | Dense QPS | Status |")
    print("|:---|---:|---:|---:|---:|:---|")
    for r in matrix:
        print(f"| **{r['type']}** | {r['dim']} | {r['put_mb']:.2f} | {r['get_mb']:.2f} | {r['search_qps']:.1f} | {r['status']} |")

    # Save summary
    with open("matrix_results.json", "w") as f:
        json.dump(matrix, f, indent=2)

if __name__ == "__main__":
    main()
