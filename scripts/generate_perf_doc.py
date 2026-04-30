import os
import json
import pandas as pd
from datetime import datetime
import platform

def parse_results(log_dir):
    all_results = []
    # results_local_cpu_float32_128_1k.json
    for f in os.listdir(log_dir):
        if f.endswith('.json') and f.startswith('results_'):
            try:
                parts = f.replace('.json', '').split('_')
                if len(parts) < 6: continue
                host = parts[1]
                device = parts[2]
                dtype = parts[3]
                dim = int(parts[4])
                count_str = parts[5]
                
                with open(os.path.join(log_dir, f)) as j:
                    data = json.load(j)
                    # data is a list of BenchmarkResult
                    for item in data:
                        item['platform'] = host
                        item['mode'] = device
                        item['dtype'] = dtype
                        item['dim'] = dim
                        item['count'] = count_str
                        all_results.append(item)
            except Exception as e:
                print(f"Error parsing {f}: {e}")
    return all_results

def generate_markdown(results, output_file):
    if not results:
        print("No results found.")
        return
    
    df = pd.DataFrame(results)
    
    with open(output_file, 'w') as f:
        f.write("# Longbow Performance Benchmark Matrix (v0.2.0)\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # 1. Ingest Performance
        f.write("## 1. Ingestion Throughput (vec/s)\n\n")
        ingest_df = df[df['Name'] == 'DoPut'].pivot_table(
            index=['platform', 'mode', 'dtype'],
            columns=['dim', 'count'],
            values='Throughput'
        )
        f.write(ingest_df.to_markdown())
        f.write("\n\n")
        
        # 2. Search Performance (QPS)
        f.write("## 2. Search Performance (QPS)\n\n")
        search_df = df[df['Name'].str.startswith('Search_')]
        modes = sorted(search_df['Name'].unique())
        for mode in modes:
            f.write(f"### {mode.replace('Search_', '')} QPS\n\n")
            mode_df = search_df[search_df['Name'] == mode].pivot_table(
                index=['platform', 'mode', 'dtype'],
                columns=['dim', 'count'],
                values='Throughput'
            )
            f.write(mode_df.to_markdown())
            f.write("\n\n")

if __name__ == "__main__":
    results = parse_results(".")
    generate_markdown(results, "docs/performance.md")
