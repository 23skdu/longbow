#!/usr/bin/env python3
import json
import glob
import re
import os
from datetime import datetime

def main():
    results = []
    files = glob.glob("results_*.json")
    
    print(f"Found {len(files)} result files.")
    
    for fpath in files:
        # results_dtype_dim_count.json
        match = re.match(r"results_(.+)_([0-9]+)_([0-9]+)\.json", os.path.basename(fpath))
        if not match:
            print(f"Skipping {fpath} (no match)")
            continue
        
        dtype = match.group(1)
        dim = int(match.group(2))
        count = int(match.group(3))
        
        try:
            with open(fpath, "r") as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {fpath}: {e}")
            continue
            
        # Extract metrics
        doput = next((r for r in data if r['name'] == 'DoPut'), None)
        doget = next((r for r in data if r['name'] == 'DoGet'), None)
        dense = next((r for r in data if r['name'] == 'Search_Dense'), None)
        hybrid = next((r for r in data if r['name'] == 'Search_Hybrid'), None)
        
        results.append({
            "dtype": dtype,
            "dim": dim,
            "count": count,
            "put_vec": doput['throughput'] if doput else 0.0,
            "put_mb": doput['throughput_mbs'] if doput else 0.0,
            "get_vec": doget['throughput'] if doget else 0.0,
            "get_mb": doget['throughput_mbs'] if doget else 0.0,
            "dense_qps": dense['throughput'] if dense else 0.0,
            "hybrid_qps": hybrid['throughput'] if hybrid else 0.0,
        })
        
    # Sort results
    results.sort(key=lambda x: (x['dtype'], x['dim'], x['count']))
    
    # Generate Markdown
    lines = [
        "# Performance Validation Matrix",
        "",
        f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Results Table",
        "",
        "| DType | Dim | Count | DoPut (vec/s) | DoPut (MB/s) | DoGet (vec/s) | DoGet (MB/s) | Dense QPS | Hybrid QPS |",
        "|-------|-----|-------|---------------|--------------|---------------|--------------|-----------|------------|",
    ]
    
    for r in results:
        lines.append(
            f"| {r['dtype']} | {r['dim']} | {r['count']} | {r['put_vec']:.2f} | {r['put_mb']:.2f} | {r['get_vec']:.2f} | {r['get_mb']:.2f} | {r['dense_qps']:.2f} | {r['hybrid_qps']:.2f} |"
        )
        
    lines.extend([
        "",
        "## Analysis",
        "",
        "<!-- TODO: Add analysis of results here -->",
        "",
        "## Next Steps Plan",
        "",
        "<!-- TODO: Add plan items based on analysis -->",
    ])
        
    output_file = "docs/performance.md"
    with open(output_file, "w") as f:
        f.write("\n".join(lines) + "\n")
        
    print(f"{output_file} updated with {len(results)} rows.")

if __name__ == "__main__":
    main()
