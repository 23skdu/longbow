import json
import glob
import os

files = glob.glob('data/perf_logs/perf_matrix_*.json')

# We want to group by mode (CPU/Metal/CUDA), count, dim, dtype
summary = {}

for f in files:
    with open(f, 'r') as fh:
        try:
            data = json.load(fh)
            host = "ancalagon" if "ancalagon" in f else "local (Mac)"
            
            results = data.get("results", [])
            for r in results:
                # Group by environment and mode
                env_mode = f"{host} - {r['mode']}"
                if env_mode not in summary:
                    summary[env_mode] = []
                summary[env_mode].append(r)
        except Exception as e:
            pass

for env_mode, results in sorted(summary.items()):
    print(f"### {env_mode}")
    print("| Dtype | Dim | Count | Ingest (vec/s) | Dense Search (QPS) | P95 Latency (ms) |")
    print("|-------|-----|-------|----------------|--------------------|------------------|")
    
    # Sort results
    results.sort(key=lambda x: (x['dtype'], x['dim'], x['count']))
    
    for r in results:
        dtype = r['dtype']
        dim = r['dim']
        count = r['count']
        ingest = r['ingest']['vec_per_sec']
        
        # some json structures might have "search" object
        search = r.get("search", {})
        dense = search.get("dense", {})
        qps = dense.get("qps", 0)
        p95 = dense.get("p95", 0)
        
        print(f"| {dtype} | {dim} | {count} | {ingest:,.0f} | {qps:,.0f} | {p95:.2f} |")
    print("\n")
