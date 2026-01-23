import subprocess
import json
import os
import sys
import time

# Ensure correct PYTHONPATH for SDK before importing it
current_dir = os.getcwd()
sdk_src = os.path.join(current_dir, "longbowclientsdk", "src")
if sdk_src not in sys.path:
    sys.path.insert(0, sdk_src)

from longbow.client import LongbowClient

# Matrix configuration
DTYPES = ["int8", "int16", "int32", "int64", "float32", "float16", "float64", "complex64", "complex128"]
DIMS = [128, 384, 1536, 3072]
ROWS = 15000

def calculate_rows(dtype, dim, target_mb=100, min_rows=15000, max_rows=50000):
    sizes = {
        "int8": 1, "int16": 2, "int32": 4, "int64": 8,
        "float16": 2, "float32": 4, "float64": 8,
        "complex64": 8, "complex128": 16
    }
    size_per_vec = dim * sizes.get(dtype, 4)
    target_bytes = target_mb * 1024 * 1024
    rows = int(target_bytes / size_per_vec)
    return max(min_rows, min(rows, max_rows))

PYTHON_EXE = sys.executable

def run_bench(dtype, dim, rows):
    dataset_name = f"matrix_{dtype}_{dim}"
    output_file = f"{dataset_name}.json"
    
    # Check if we already have this result
    if os.path.exists(output_file):
         print(f"Skipping {dtype} @ {dim}d (result exists)")
         with open(output_file, 'r') as f:
             data = json.load(f)
             put = next((r for r in data if r['name'].startswith('DoPut')), None)
             get = next((r for r in data if r['name'].startswith('DoGet')), None)
             search = next((r for r in data if r['name'].startswith('VectorSearch')), None)
             return {
                    "put_mb": put['throughput'] if put else 0,
                    "get_mb": get['throughput'] if get else 0,
                    "search_qps": search['throughput'] if search else 0,
                    "status": "Stable"
                }

    print(f"Benchmarking {dtype} @ {dim}d ({rows} vectors)...")
    
    cmd = [
        PYTHON_EXE, "scripts/perf_test.py",
        "--dataset", dataset_name,
        "--rows", str(rows),
        "--dim", str(dim),
        "--dtype", dtype,
        "--search",
        "--json", output_file,
        "--k", "10"
    ]
    
    try:
        # Run bench
        env = os.environ.copy()
        res_proc = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
        
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                data = json.load(f)
                # Find metrics
                put = next((r for r in data if r['name'].startswith('DoPut')), None)
                get = next((r for r in data if r['name'].startswith('DoGet')), None)
                search = next((r for r in data if r['name'].startswith('VectorSearch')), None)
                
                res = {
                    "put_mb": put['throughput'] if put else 0,
                    "get_mb": get['throughput'] if get else 0,
                    "search_qps": search['throughput'] if search else 0,
                    "status": "Stable"
                }

                # Cleanup
                try:
                    client = LongbowClient(uri="grpc://localhost:3000")
                    client.delete_namespace(dataset_name)
                    print(f"  Cleaned up {dataset_name}")
                except Exception as ce:
                    print(f"  Cleanup failed for {dataset_name}: {ce}")

                return res
    except Exception as e:
        print(f"  Error {dtype}@{dim}d: {e}")
        return {"put_mb": 0, "get_mb": 0, "search_qps": 0, "status": "Error"}
    
    return {"put_mb": 0, "get_mb": 0, "search_qps": 0, "status": "Error"}

def main():
    matrix = []
    
    # Ensure correct PYTHONPATH for SDK
    current_dir = os.getcwd()
    sdk_src = os.path.join(current_dir, "longbowclientsdk", "src")
    if sdk_src not in sys.path:
        sys.path.append(sdk_src)
    os.environ["PYTHONPATH"] = f"{sdk_src}:{os.environ.get('PYTHONPATH', '')}"

    start_time = time.time()
    
    for dim in DIMS:
        for dtype in DTYPES:
            rows = calculate_rows(dtype, dim)
            res = run_bench(dtype, dim, rows)
            matrix.append({
                "dim": dim,
                "dtype": dtype,
                **res
            })
            
            # Save intermediate results
            with open("matrix_results.json", "w") as f:
                json.dump(matrix, f, indent=2)

    total_time = time.time() - start_time
    print(f"\nMatrix generation completed in {total_time/60:.2f} minutes")
    
    # Print summary table
    print("\n" + "="*80)
    print(f"{'DType':<12} | {'Dim':<6} | {'Put MB/s':<10} | {'Get MB/s':<10} | {'Search QPS':<10} | {'Status'}")
    print("-" * 80)
    for r in matrix:
        print(f"{r['dtype']:<12} | {r['dim']:<6} | {r['put_mb']:<10.2f} | {r['get_mb']:<10.2f} | {r['search_qps']:<10.2f} | {r['status']}")
    print("="*80)

if __name__ == "__main__":
    main()
