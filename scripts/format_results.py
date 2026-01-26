import json
import datetime

def main():
    with open("detailed_matrix_results.json", "r") as f:
        data = json.load(f)

    # Group by Type
    by_type = {}
    for entry in data:
        t = entry["type"]
        if t not in by_type:
            by_type[t] = []
        by_type[t].append(entry)

    md_lines = []
    md_lines.append("# Performance Validation Matrix")
    md_lines.append(f"\nGenerated on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md_lines.append("\n**Configuration**:")
    md_lines.append("- Node: 1 Node (Local)")
    md_lines.append("- Latencies: P95 (ms)")
    md_lines.append("- Ingest: MB/s and Vectors/s")

    for dtype in sorted(by_type.keys()):
        rows = by_type[dtype]
        # Sort by Dim, then Count
        rows.sort(key=lambda x: (x["dim"], x["count"]))

        md_lines.append(f"\n## Data Type: {dtype}")
        md_lines.append(f"| Dim | Vectors | Put (MB/s) | Put (Vec/s) | Get (MB/s) | Get (Vec/s) | Dense (ms) | Sparse (ms) | Filtered (ms) | Hybrid (ms) |")
        md_lines.append(f"|-----|---------|------------|-------------|------------|-------------|------------|-------------|---------------|-------------|")

        for r in rows:
            # Format numbers
            ingest_mb = f"{r['ingest_mb_s']:.2f}"
            ingest_vec = f"{int(r['ingest_vec_s']):,}"
            doget_mb = f"{r['doget_mb_s']:.2f}"
            doget_vec = f"{int(r['doget_vec_s']):,}"
            lat_dense = f"{r['latency_dense']:.2f}"
            lat_sparse = f"{r['latency_sparse']:.2f}"
            lat_filt = f"{r['latency_filtered']:.2f}"
            lat_hyb = f"{r['latency_hybrid']:.2f}"
            
            line = f"| {r['dim']} | {r['count']:,} | {ingest_mb} | {ingest_vec} | {doget_mb} | {doget_vec} | {lat_dense} | {lat_sparse} | {lat_filt} | {lat_hyb} |"
            md_lines.append(line)

    content = "\n".join(md_lines)
    with open("docs/performance.md", "w") as f:
        f.write(content)
    
    print("Updated docs/performance.md")

if __name__ == "__main__":
    main()
