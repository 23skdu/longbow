import json
import os
import glob
import pandas as pd

def aggregate_benchmarks(results_dir):
    all_results = []
    
    # Find all JSON files in the output directory
    json_files = glob.glob(os.path.join(results_dir, "**/*.json"), recursive=True)
    
    for f in json_files:
        # Infer host and mode from directory name
        parts = os.path.basename(os.path.dirname(f)).split('_')
        if len(parts) >= 2:
            host = parts[0]
            mode = parts[1]
        else:
            host = "unknown"
            mode = "unknown"
            
        # Parse dim/dtype/count from filename: bench_<dtype>_<dim>_<count>.json
        fname = os.path.basename(f)
        fparts = fname.replace('.json', '').split('_')
        # bench, dtype, dim, count
        if len(fparts) >= 4:
            dtype = fparts[1]
            dim = int(fparts[2])
            count = int(fparts[3])
        else:
            dtype = "unknown"
            dim = 0
            count = 0

        try:
            with open(f, 'r') as jf:
                data = json.load(jf)
                for res in data:
                    all_results.append({
                        "Host": host,
                        "Mode": mode,
                        "Dataset": fname,
                        "DType": dtype,
                        "Dim": dim,
                        "Count": count,
                        "Action": res.get("name", "unknown"),
                        "Throughput_QPS": res.get("throughput", 0),
                        "Throughput_MBs": res.get("throughput_mbs", 0),
                        "P50_ms": res.get("p50_latency_ms", 0),
                        "P95_ms": res.get("p95_latency_ms", 0),
                        "P99_ms": res.get("p99_latency_ms", 0)
                    })
        except Exception as e:
            print(f"Error parsing {f}: {e}")
            
    if not all_results:
        return None
        
    df = pd.DataFrame(all_results)
    return df

def generate_markdown_report(df, output_file):
    with open(output_file, 'w') as f:
        f.write("# Longbow v0.2.2-rc2 Performance Matrix\n\n")
        
        # Summary by Search Mode
        f.write("## Search Performance Summary (QPS)\n\n")
        summary = df[df['Action'].str.startswith('Search_')].pivot_table(
            index=['Host', 'Mode', 'Dim', 'DType'],
            columns='Action',
            values='Throughput_QPS',
            aggfunc='mean'
        ).round(2)
        f.write(summary.to_markdown())
        f.write("\n\n")
        
        # Ingestion Performance
        f.write("## Ingestion Performance (MB/s)\n\n")
        ingest = df[df['Action'] == 'DoPut'].pivot_table(
            index=['Host', 'Mode', 'Dim', 'DType'],
            values='Throughput_MBs',
            aggfunc='mean'
        ).round(2)
        f.write(ingest.to_markdown())
        f.write("\n\n")
        
        # Detailed results for each host/mode
        for (host, mode), group in df.groupby(['Host', 'Mode']):
            f.write(f"### Details: {host} ({mode})\n\n")
            f.write(group.to_markdown(index=False))
            f.write("\n\n")

if __name__ == "__main__":
    df = aggregate_benchmarks("bench_results")
    if df is not None:
        generate_markdown_report(df, "docs/performance_new.md")
        print("Generated docs/performance_new.md")
    else:
        print("No results found.")
