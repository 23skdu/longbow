#!/usr/bin/env python3
import json
import os
import glob
from datetime import datetime

def get_latest_file(pattern):
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getctime)

def format_num(n):
    if n >= 1000:
        return f"{n:,.0f}"
    return f"{n:.2f}"

def generate_markdown_table(results, title):
    if not results:
        return ""
    
    md = f"### {title}\n\n"
    md += "| DType | Dim | Count | Ingest (vec/s) | Dense QPS | Dense P50 | Hybrid QPS | Hybrid P50 | Filtered QPS | Filtered P50 | ByID QPS | ByID P50 |\n"
    md += "|-------|-----|-------|----------------|-----------|-----------|------------|------------|--------------|--------------|----------|----------|\n"
    
    # Sort results by count, then dtype, then dim
    sorted_results = sorted(results, key=lambda x: (x['count'], x['dtype'], x['dim']))
    
    for r in sorted_results:
        search = r.get("search", {})
        dense = search.get("dense", {"qps": 0, "p50": 0})
        hybrid = search.get("hybrid", {"qps": 0, "p50": 0})
        filtered = search.get("filtered", {"qps": 0, "p50": 0})
        byid = search.get("byid", {"qps": 0, "p50": 0})
        
        md += f"| {r['dtype']} | {r['dim']} | {r['count']:,} | {format_num(r['ingest']['vec_per_sec'])} | "
        md += f"{format_num(dense['qps'])} | {dense['p50']:.3f}ms | "
        md += f"{format_num(hybrid['qps'])} | {hybrid['p50']:.3f}ms | "
        md += f"{format_num(filtered['qps'])} | {filtered['p50']:.3f}ms | "
        md += f"{format_num(byid['qps'])} | {byid['p50']:.3f}ms |\n"
    
    return md + "\n"

def main():
    cpu_file = get_latest_file("data/perf_logs/perf_matrix_cpu_*.json")
    metal_file = get_latest_file("data/perf_logs/perf_matrix_metal_*.json")
    
    if cpu_file:
        print(f"Analyzing CPU results: {cpu_file}")
        with open(cpu_file) as f:
            cpu_data = json.load(f)
            cpu_md = "# Longbow CPU Performance Matrix\n\n"
            cpu_md += f"**Timestamp**: {cpu_data['timestamp']}\n"
            cpu_md += f"**Platform**: {cpu_data['platform']}\n\n"
            
            # Group by count for better readability
            counts = sorted(cpu_data['config']['counts'])
            for count in counts:
                count_results = [r for r in cpu_data['results'] if r['count'] == count]
                cpu_md += generate_markdown_table(count_results, f"Results for {count:,} vectors")
            
            with open("performance.md", "w") as out:
                out.write(cpu_md)
            print("Generated performance.md")

    if metal_file:
        print(f"Analyzing Metal results: {metal_file}")
        with open(metal_file) as f:
            metal_data = json.load(f)
            metal_md = "# Longbow Metal GPU Performance Matrix\n\n"
            metal_md += f"**Timestamp**: {metal_data['timestamp']}\n"
            metal_md += f"**Platform**: {metal_data['platform']}\n\n"
            
            # Group by count
            counts = sorted(metal_data['config']['counts'])
            for count in counts:
                count_results = [r for r in metal_data['results'] if r['count'] == count]
                metal_md += generate_markdown_table(count_results, f"Results for {count:,} vectors")
            
            with open("performance_metal.md", "w") as out:
                out.write(metal_md)
            print("Generated performance_metal.md")

if __name__ == "__main__":
    main()
