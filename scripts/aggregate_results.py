import json
import os
import glob
import re

def main():
    results = []
    # Identify all matrix files
    files = glob.glob("matrix_*.json")
    
    # Exclude matrix_results.json itself
    if "matrix_results.json" in files:
        files.remove("matrix_results.json")
    
    print(f"Found {len(files)} result files.")
    
    for fpath in files:
        # matrix_dtype_dim.json
        # Regex to parse filename
        match = re.match(r"matrix_(.+)_(.+)\.json", fpath)
        if not match:
            print(f"Skipping {fpath}")
            continue
            
        dtype = match.group(1)
        dim_str = match.group(2)
        try:
            dim = int(dim_str)
        except:
            print(f"Skipping {fpath} (invalid dim)")
            continue
            
        try:
            with open(fpath, "r") as f:
                data = json.load(f)
        except Exception as e:
             print(f"Error reading {fpath}: {e}")
             continue
             
        # Extract metrics
        put = next((r for r in data if r['name'].startswith('DoPut')), None)
        get = next((r for r in data if r['name'].startswith('DoGet')), None)
        search = next((r for r in data if r['name'].startswith('VectorSearch')), None)
        
        put_mb = put['throughput'] if put else 0.0
        get_mb = get['throughput'] if get else 0.0
        search_qps = search['throughput'] if search else 0.0
        
        status = "Stable"
        if put_mb == 0 and get_mb == 0 and search_qps == 0:
             status = "Failed"
             
        results.append({
            "dim": dim,
            "dtype": dtype,
            "put_mb": put_mb,
            "get_mb": get_mb,
            "search_qps": search_qps,
            "status": status
        })
        
    # Write aggregated
    with open("matrix_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Aggregated matrix_results.json written.")

if __name__ == "__main__":
    main()
