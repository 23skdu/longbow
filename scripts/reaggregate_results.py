import json
import glob
import os

def reaggregate(pattern, output_file):
    results = []
    files = glob.glob(pattern)
    print(f"Found {len(files)} result files for pattern {pattern}")
    
    for f in files:
        try:
            with open(f, 'r') as f_in:
                data = json.load(f_in)
                
            # Extract dim, dtype, count from filename if not in data
            # Filename format: result_metal_float32_128_5000.json
            base = os.path.basename(f).replace("result_", "").replace(".json", "")
            parts = base.split("_")
            # mode = parts[0]
            # dtype = parts[1]
            # dim = int(parts[2])
            # count = int(parts[3])
            
            # The JSON from bench-tool is a list of metrics
            metrics = {}
            if isinstance(data, list):
                for entry in data:
                    name = entry.get("name", "")
                    if name == "DoPut":
                        metrics["ingest_vec_per_sec"] = entry.get("throughput", 0)
                    elif name.startswith("Search_"):
                        prefix = name.replace("Search_", "").lower()
                        metrics[f"{prefix}_qps"] = entry.get("throughput", 0)
                        metrics[f"{prefix}_p50_ms"] = entry.get("p50_latency_ms", 0)
            
            # Construct the entry
            # We need to find the mode, dtype, dim, count from the filename more reliably
            # metal_float32_128_5000
            mode = parts[0]
            dtype = parts[1]
            dim = int(parts[2])
            count = int(parts[3])
            
            search_metrics = {}
            for key, val in metrics.items():
                if "_qps" in key:
                    p = key.replace("_qps", "")
                    search_metrics[p] = {
                        "qps": val,
                        "p50": metrics.get(f"{p}_p50_ms", 0)
                    }

            results.append({
                "mode": mode,
                "dtype": dtype,
                "dim": dim,
                "count": count,
                "ingest": {"vec_per_sec": metrics.get("ingest_vec_per_sec", 0)},
                "search": search_metrics
            })
        except Exception as e:
            # print(f"Error parsing {f}: {e}")
            continue

    with open(output_file, 'w') as f_out:
        json.dump({"results": results}, f_out, indent=2)
    print(f"Aggregated {len(results)} results to {output_file}")

if __name__ == "__main__":
    reaggregate("data/perf_logs/result_metal_*.json", "data/perf_logs/aggregated_metal.json")
    reaggregate("data/perf_logs/result_cpu_*.json", "data/perf_logs/aggregated_cpu.json")
