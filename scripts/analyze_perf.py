import json
import glob
import os
import pandas as pd

def analyze():
    # Load all results from data/perf_logs
    all_results = []
    json_files = glob.glob("data/perf_logs/result_*.json")
    
    for f in json_files:
        fname = os.path.basename(f)
        # Parse result_{mode}_{dtype}_{dim}_{count}.json
        parts = fname.replace('.json', '').split('_')
        if len(parts) >= 5:
            mode = parts[1]
            dtype = parts[2]
            dim = int(parts[3])
            count = int(parts[4])
        else:
            continue
            
        try:
            with open(f, 'r') as jf:
                data = json.load(jf)
                for res in data:
                    all_results.append({
                        "Mode": mode,
                        "DType": dtype,
                        "Dim": dim,
                        "Count": count,
                        "Action": res.get("name", "unknown"),
                        "QPS": res.get("throughput", 0),
                        "MBs": res.get("throughput_mbs", 0),
                        "P50": res.get("p50_latency_ms", 0),
                        "P95": res.get("p95_latency_ms", 0)
                    })
        except Exception as e:
            pass
            
    if not all_results:
        print("No results found.")
        return
        
    df = pd.DataFrame(all_results)
    
    print("=== DENSE SEARCH QPS (CPU vs CUDA-fallback) ===")
    dense = df[(df['Action'] == 'Search_Dense') & (df['Count'] == 5000) & (df['DType'].isin(['float32', 'complex64', 'int8', 'turboquant8']))]
    pivot_qps = dense.pivot_table(
        index=['Dim', 'DType'],
        columns='Mode',
        values='QPS',
        aggfunc='mean'
    )
    print(pivot_qps.round(1))
    print("\n")
    
    print("=== INGESTION SPEED MB/s (CPU vs CUDA-fallback) ===")
    ingest = df[(df['Action'] == 'DoPut') & (df['Count'] == 5000) & (df['DType'].isin(['float32', 'complex64', 'int8', 'turboquant8']))]
    pivot_ingest = ingest.pivot_table(
        index=['Dim', 'DType'],
        columns='Mode',
        values='MBs',
        aggfunc='mean'
    )
    print(pivot_ingest.round(1))
    print("\n")
    
    print("=== SEARCH MODES LATENCY SCALING (CPU, float32, count=5000) ===")
    modes = df[(df['Mode'] == 'cpu') & (df['DType'] == 'float32') & (df['Count'] == 5000) & (df['Action'].str.startswith('Search_'))]
    pivot_modes = modes.pivot_table(
        index='Action',
        columns='Dim',
        values='P95',
        aggfunc='mean'
    )
    print(pivot_modes.round(3))

if __name__ == "__main__":
    analyze()
