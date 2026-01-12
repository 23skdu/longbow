#!/usr/bin/env python3
import subprocess
import json
import os
import time

# Matrix definition
DIMS = [128, 384, 1536, 3072]
COUNTS = {
    128: [3000, 5000, 9000, 10000],
    384: [3000, 5000, 9000, 10000],
    1536: [3000, 5000],
    3072: [3000, 5000]
}
PRECISIONS = ["FP32", "FP16"]
SEARCH_TYPES = ["dense", "sparse", "filtered", "hybrid"]

PYTHON_EXE = "venv/bin/python" if os.path.exists("venv/bin/python") else "python3"
PERF_SCRIPT = "scripts/perf_test.py"

def run_bench(dim, count, precision):
    results = {}
    ds_name = f"bench_{dim}_{count}_{precision.lower()}"
    
    # Base command for Put/Get
    base_cmd = [
        PYTHON_EXE, PERF_SCRIPT,
        "--dataset", ds_name,
        "--rows", str(count),
        "--dim", str(dim),
        "--queries", "100",  # Lower queries for faster testing of many permutations
        "--with-text"        # Always generate text for hybrid/sparse
    ]
    
    # 1. DO PUT & DO GET
    print(f"\n>>> Running PUT/GET for {dim}d, {count} rows, {precision}")
    try:
        put_get_json = f"pg_{ds_name}.json"
        subprocess.run(base_cmd + ["--json", put_get_json], check=True)
        with open(put_get_json) as f:
            pg_data = json.load(f)
            results['put'] = next((r for r in pg_data if r['name'] == 'DoPut'), None)
            results['get'] = next((r for r in pg_data if r['name'] == 'DoGet'), None)
        os.remove(put_get_json)
    except Exception as e:
        print(f"Error in PUT/GET: {e}")

    # 2. SEARCH PERMUTATIONS
    # We call perf_test again with skip-put to avoid re-uploading
    search_base = base_cmd + ["--skip-put", "--skip-get"]
    
    # Dense (alpha=1.0)
    print(f">>> Dense Search...")
    dense_json = f"dense_{ds_name}.json"
    subprocess.run(search_base + ["--search", "--alpha", "1.0", "--json", dense_json], check=True)
    with open(dense_json) as f:
        results['dense'] = next((r for r in json.load(f) if 'VectorSearch' in r['name']), None)
    os.remove(dense_json)

    # Sparse (alpha=0.0)
    print(f">>> Sparse Search...")
    sparse_json = f"sparse_{ds_name}.json"
    subprocess.run(search_base + ["--search", "--alpha", "0.0", "--json", sparse_json], check=True)
    with open(sparse_json) as f:
        results['sparse'] = next((r for r in json.load(f) if 'VectorSearch' in r['name']), None)
    os.remove(sparse_json)

    # Filtered (Dense with filter)
    print(f">>> Filtered Search...")
    filtered_json = f"filtered_{ds_name}.json"
    # Filter for approx 10% of data
    filter_val = count // 10
    subprocess.run(search_base + ["--search", "--alpha", "1.0", "--filter", f"id:lt:{filter_val}", "--json", filtered_json], check=True)
    with open(filtered_json) as f:
        results['filtered'] = next((r for r in json.load(f) if 'VectorSearch' in r['name']), None)
    os.remove(filtered_json)

    # Hybrid (alpha=0.5)
    print(f">>> Hybrid Search...")
    hybrid_json = f"hybrid_{ds_name}.json"
    subprocess.run(search_base + ["--hybrid", "--alpha", "0.5", "--json", hybrid_json], check=True)
    with open(hybrid_json) as f:
        results['hybrid'] = next((r for r in json.load(f) if r['name'] == 'HybridSearch'), None)
    os.remove(hybrid_json)
    
    return results

def main():
    all_results = []
    
    for dim in DIMS:
        for count in COUNTS.get(dim, []):
            for precision in PRECISIONS:
                print(f"\n{'='*80}")
                print(f"STARTING DIM={dim} COUNT={count} PRECISION={precision}")
                print(f"{'='*80}")
                
                try:
                    res = run_bench(dim, count, precision)
                    all_results.append({
                        "dim": dim,
                        "count": count,
                        "precision": precision,
                        "metrics": res
                    })
                except Exception as e:
                    print(f"FAILED permutation {dim}/{count}/{precision}: {e}")
                
                # Small cool down between substantial loads
                time.sleep(2)

    # Final Output
    output_path = "performance_results_matrix.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2)
    
    print(f"\nAll benchmarks complete. Results saved to {output_path}")

if __name__ == "__main__":
    main()
