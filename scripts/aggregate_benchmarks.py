import json
import glob
import os
import sys

def aggregate_results(results_dir):
    all_results = []
    files = glob.glob(os.path.join(results_dir, "*.json"))
    for f in files:
        if "bench_" not in os.path.basename(f):
            continue
        try:
            with open(f, 'r') as jf:
                data = json.load(jf)
                # Filename format: bench_{dtype}_{dim}_{count}.json
                parts = os.path.basename(f).replace(".json", "").split("_")
                if len(parts) < 4:
                    continue
                dtype = parts[1]
                dim = parts[2]
                count = parts[3]
                
                for res in data:
                    res['dtype'] = dtype
                    res['dim'] = dim
                    res['count'] = count
                    all_results.append(res)
        except Exception as e:
            print(f"Error parsing {f}: {e}")
    return all_results

def format_markdown(results):
    if not results:
        return "No results found."
    
    # Group by Name (Mode)
    modes = sorted(list(set(r['name'] for r in results)))
    
    output = "# Benchmark Results Matrix\n\n"
    
    for mode in modes:
        output += f"## {mode}\n\n"
        output += "| DType | Dim | Count | Throughput | P50 (ms) | P95 (ms) | P99 (ms) |\n"
        output += "|-------|-----|-------|------------|----------|----------|----------|\n"
        
        mode_results = [r for r in results if r['name'] == mode]
        # Sort by count, then dim, then dtype
        mode_results.sort(key=lambda x: (int(x['count']), int(x['dim']), x['dtype']))
        
        for r in mode_results:
            throughput = f"{r['throughput']:.2f} {r['throughput_unit']}"
            p50 = f"{r.get('p50_latency_ms', 0):.2f}"
            p95 = f"{r.get('p95_latency_ms', 0):.2f}"
            p99 = f"{r.get('p99_latency_ms', 0):.2f}"
            output += f"| {r['dtype']} | {r['dim']} | {r['count']} | {throughput} | {p50} | {p95} | {p99} |\n"
        output += "\n"
    
    return output

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 aggregate_benchmarks.py <results_dir>")
        sys.exit(1)
    
    results = aggregate_results(sys.argv[1])
    print(format_markdown(results))
