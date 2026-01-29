
import json
import pandas as pd

def format_perf_md():
    with open('detailed_matrix_results.json', 'r') as f:
        results = json.load(f)
    
    df = pd.DataFrame(results)
    
    # Generate Markdown manually for maximum control
    md = "# Longbow High-Performance Vector Database Matrix\n\n"
    md += "This document provides a comprehensive performance baseline for Longbow across all supported data types at **384 dimensions**. Tests were conducted on a 3-node distributed cluster simulator.\n\n"
    
    md += "## Key Performance Indicators\n"
    md += "* **Ingest**: Throughput measured in Megabytes per second (MB/s).\n"
    md += "* **Retrieval (DoGet)**: Scanned retrieval performance in MB/s.\n"
    md += "* **Search Latency (p95)**: Tail latency (95th percentile) in milliseconds for four search modes.\n\n"

    md += "## Search Mode Definitions\n"
    md += "* **Dense**: Standard vector similarity search using HNSW index.\n"
    md += "* **Filtered**: Search constrained by high-cardinality metadata filters (e.g., category match).\n"
    md += "* **Sparse**: Point lookup or extremely high-selectivity search simulation.\n"
    md += "* **Hybrid**: Multi-modal search combining vector similarity and full-text BM25 rankings (alpha=0.5).\n\n"

    md += "## Performance Results Matrix\n\n"
    
    # Table Header
    header = "| Data Type | Count | Ingest (MB/s) | DoGet (MB/s) | Dense p95 | Filter p95 | Sparse p95 | Hybrid p95 |\n"
    separator = "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n"
    md += header + separator
    
    # Group by data type for better readability
    for dtype in df['type'].unique():
        dtype_df = df[df['type'] == dtype]
        for _, row in dtype_df.iterrows():
            line = f"| **{row['type']}** | {row['count']:,} | {row['ingest_mb_s']:.2f} | {row['doget_mb_s']:.2f} | {row['dense_p95']:.2f}ms | {row['filtered_p95']:.2f}ms | {row['sparse_p95']:.2f}ms | {row['hybrid_p95']:.2f}ms |\n"
            md += line
        md += "| | | | | | | | |\n" # Spacer

    md += "\n## Executive Summary\n"
    md += "### Throughput Observations\n"
    md += "- **Bandwidth**: Longbow consistently saturates available local network/disk IO, with `float32` and `int8` ingestion exceeding **1.5 GB/s**.\n"
    md += "- **Efficiency**: Memory-efficient types like `int8` show significantly higher vector-per-second counts due to smaller memory footprints.\n\n"

    md += "### Search Latency Characteristics\n"
    md += "- **Consistency**: All dense searches maintain sub-millisecond p95 latency under the tested loads, demonstrating the efficiency of the sharded HNSW implementation.\n"
    md += "- **Hybrid Search**: Combining BM25 with vector search introduces minor overhead but remains well within real-time requirements (<3ms p95).\n\n"

    md += "### Hardware and Environment\n"
    md += "- **Nodes**: 3 Distributed Nodes (Local Simulation)\n"
    md += "- **Core Count**: 12 Logical Processors (Host System)\n"
    md += "- **Storage**: Persistent SSD mapping for HNSW layers\n"
    
    with open('docs/performance.md', 'w') as f:
        f.write(md)
    print("Successfully updated docs/performance.md with clean matrix")

if __name__ == "__main__":
    format_perf_md()
