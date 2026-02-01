#!/usr/bin/env python3
import subprocess
import json
import os
import time
from datetime import datetime

# Test Matrix
DIMS = 384
COUNTS = [1000, 3000, 5000, 10000, 15000, 25000]
DTYPES = ["complex128", "complex64", "float32", "float64", "float16", "int8", "int32", "uint8", "uint32"]

PYTHON_EXE = "python3"
PERF_SCRIPT = "scripts/perf_test.py"
# Set PYTHONPATH to include the SDK
SDK_PATH = os.path.abspath("longbowclientsdk/src")
os.environ["PYTHONPATH"] = f"{SDK_PATH}:{os.environ.get('PYTHONPATH', '')}"

def run_bench(client, dtype, count):
    ds_name = f"bench_{dtype}_{count}"
    json_file = f"results_{ds_name}.json"
    
    # Pre-cleanup in case of previous failures
    try:
        client.delete_namespace(ds_name)
    except:
        pass

    # Single comprehensive run
    cmd = [
        PYTHON_EXE, PERF_SCRIPT,
        "--dataset", ds_name,
        "--rows", str(count),
        "--dim", str(DIMS),
        "--dtype", dtype,
        "--queries", "100",
        "--with-text", # Enables Sparse, Hybrid, Filtered search in perf_test
        "--alpha", "0.5", # For Hybrid
        "--json", json_file
    ]
    
    print(f"\n>>> Running Bench: {dtype}, {count} vectors")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Benchmark failed: {e}")
        return {}

    results = {}
    if os.path.exists(json_file):
        with open(json_file) as f:
            data = json.load(f)
            # Use 'in' for loose matching as perf_test appends " @ size" to names
            results['put'] = next((r for r in data if 'DoPut' in r['name']), None)
            results['get'] = next((r for r in data if 'DoGet' in r['name']), None)
            results['dense'] = next((r for r in data if 'VectorSearch' in r['name']), None)
            results['sparse'] = next((r for r in data if 'SparseSearch' in r['name']), None)
            results['filtered'] = next((r for r in data if 'FilteredSearch' in r['name']), None)
            results['hybrid'] = next((r for r in data if 'HybridSearch' in r['name']), None)
        os.remove(json_file)
    
    # Cleanup dataset from server to free memory
    try:
        print(f"Cleaning up {ds_name}...")
        client.delete_namespace(ds_name)
    except Exception as e:
        print(f"Cleanup failed for {ds_name}: {e}")
    
    return results

def format_table(all_results):
    lines = []
    lines.append("# Performance Summary")
    lines.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("| DType | Count | Put (MB/s) | Get (MB/s) | Search Type | p50 (ms) | p95 (ms) | p99 (ms) | TPS |")
    lines.append("|-------|-------|------------|------------|-------------|----------|----------|----------|-----|")
    
    for res in all_results:
        dtype = res['dtype']
        count = res['count']
        m = res['metrics']
        
        put_speed = f"{m['put']['throughput']:.2f}" if m.get('put') else "N/A"
        get_speed = f"{m['get']['throughput']:.2f}" if m.get('get') else "N/A"
        
        for stype in ['dense', 'sparse', 'filtered', 'hybrid']:
            s = m.get(stype)
            if s:
                p50 = f"{s.get('p50_ms', 0):.2f}"
                p95 = f"{s.get('p95_ms', 0):.2f}"
                p99 = f"{s.get('p99_ms', 0):.2f}"
                tps = f"{s.get('throughput', 0):.2f}"
                lines.append(f"| {dtype} | {count} | {put_speed} | {get_speed} | {stype} | {p50} | {p95} | {p99} | {tps} |")
                # Avoid repeating Put/Get for same dtype/count
                put_speed = ""
                get_speed = ""
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        
    return "\n".join(lines)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit-dtypes", help="Comma separated dtypes")
    parser.add_argument("--limit-counts", help="Comma separated counts")
    args = parser.parse_args()
    
    from longbow import LongbowClient
    client = LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")
    client.connect()

    active_dtypes = DTYPES
    if args.limit_dtypes:
        active_dtypes = args.limit_dtypes.split(",")
    
    active_counts = COUNTS
    if args.limit_counts:
        active_counts = [int(x) for x in args.limit_counts.split(",")]

    all_results = []
    for dtype in active_dtypes:
        for count in active_counts:
            try:
                res = run_bench(client, dtype, count)
                all_results.append({
                    "dtype": dtype,
                    "count": count,
                    "metrics": res
                })
            except Exception as e:
                print(f"Failed {dtype}/{count}: {e}")
    
    summary = format_table(all_results)
    print("\n" + summary)
    
    with open("docs/performance.md", "w") as f:
        f.write("# Performance Metrics (Matrix Run)\n\n")
        f.write(summary)
        f.write("\n\n## Methodology\n- Dimension: 384\n- All tests run on single node bench-tool\n")

if __name__ == "__main__":
    main()
