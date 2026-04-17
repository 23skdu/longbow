#!/usr/bin/env python3
"""
Generate performance.md from benchmark JSON results.
"""

import json
import os
import glob

DTYPES = [
    "float32",
    "float64",
    "float16",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "complex64",
    "complex128",
    "turboquant",
]
DIMS = [128, 384, 768, 1536, 3072]
COUNTS = [1000, 3000, 5000, 7000, 10000, 15000]


def parse_result_file(filepath):
    try:
        with open(filepath) as f:
            data = json.load(f)
    except:
        return None

    metrics = {}
    for entry in data:
        name = entry.get("name", "")
        if name == "DoPut":
            metrics["ingest_vec_per_sec"] = entry.get("throughput", 0)
            metrics["ingest_mb_per_sec"] = entry.get("throughput_mbs", 0)
        elif name == "DoGet":
            metrics["get_vec_per_sec"] = entry.get("throughput", 0)
            metrics["get_mb_per_sec"] = entry.get("throughput_mbs", 0)
        elif name.startswith("Search_"):
            search_type = name.replace("Search_", "").lower()
            metrics[f"{search_type}_qps"] = entry.get("throughput", 0)
            metrics[f"{search_type}_p50"] = entry.get("p50_latency_ms", 0)
            metrics[f"{search_type}_p95"] = entry.get("p95_latency_ms", 0)
            metrics[f"{search_type}_p99"] = entry.get("p99_latency_ms", 0)

    return metrics


def get_results(mode):
    """Get all results for a given mode (cpu or metal)."""
    results = {}
    pattern = f"/Users/rsd/REPOS/longbow/data/perf_logs/result_{mode}_*.json"

    for filepath in glob.glob(pattern):
        filename = os.path.basename(filepath)
        # Parse: result_cpu_float32_128_1000.json
        parts = filename.replace("result_", "").replace(".json", "").split("_")
        if len(parts) >= 4:
            dtype = parts[1]
            dim = int(parts[2])
            count = int(parts[3])

            key = (dtype, dim, count)
            results[key] = parse_result_file(filepath)

    return results


def generate_table(results, mode, count):
    """Generate a table for a specific count."""
    rows = []
    rows.append(f"### {count:,} Vectors\n")
    rows.append("| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |")
    rows.append("|-------|---------|---------|---------|----------|----------|")

    for dtype in DTYPES:
        values = []
        for dim in DIMS:
            key = (dtype, dim, count)
            if key in results and results[key]:
                vec_per_sec = results[key].get("ingest_vec_per_sec", 0)
                values.append(f"{vec_per_sec:,.0f}")
            else:
                values.append("N/A")
        rows.append(f"| **{dtype}** | {' | '.join(values)} |")

    return "\n".join(rows)


def generate_mb_table(results, mode, count):
    rows = []
    rows.append(f"### {count:,} Vectors - MB/s (Ingest)\n")
    rows.append("| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |")
    rows.append("|-------|---------|---------|---------|----------|----------|")

    for dtype in DTYPES:
        values = []
        for dim in DIMS:
            key = (dtype, dim, count)
            if key in results and results[key]:
                mb_per_sec = results[key].get("ingest_mb_per_sec", 0)
                values.append(f"{mb_per_sec:,.1f}")
            else:
                values.append("N/A")
        rows.append(f"| **{dtype}** | {' | '.join(values)} |")

    return "\n".join(rows)


def generate_get_mb_table(results, mode, count):
    rows = []
    rows.append(f"### {count:,} Vectors - MB/s (Retrieve)\n")
    rows.append("| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |")
    rows.append("|-------|---------|---------|---------|----------|----------|")

    for dtype in DTYPES:
        values = []
        for dim in DIMS:
            key = (dtype, dim, count)
            if key in results and results[key]:
                mb_per_sec = results[key].get("get_mb_per_sec", 0)
                values.append(f"{mb_per_sec:,.1f}")
            else:
                values.append("N/A")
        rows.append(f"| **{dtype}** | {' | '.join(values)} |")

    return "\n".join(rows)


def generate_search_table(results, mode, count, search_type):
    """Generate a search performance table for a specific count."""
    rows = []
    rows.append(f"### {count:,} Vectors - {search_type.title()} Search QPS\n")
    rows.append("| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |")
    rows.append("|-------|---------|---------|---------|----------|----------|")

    for dtype in DTYPES:
        values = []
        for dim in DIMS:
            key = (dtype, dim, count)
            if key in results and results[key]:
                qps = results[key].get(f"{search_type}_qps", 0)
                values.append(f"{qps:,.0f}")
            else:
                values.append("N/A")
        rows.append(f"| **{dtype}** | {' | '.join(values)} |")

    return "\n".join(rows)


def generate_latency_table(results, mode, count, search_type, percentile):
    """Generate a latency table for a specific count and search type."""
    rows = []
    rows.append(
        f"### {count:,} Vectors - {search_type.title()} Search P{percentile} Latency (ms)\n"
    )
    rows.append("| DType | Dim 128 | Dim 384 | Dim 768 | Dim 1536 | Dim 3072 |")
    rows.append("|-------|---------|---------|---------|----------|----------|")

    for dtype in DTYPES:
        values = []
        for dim in DIMS:
            key = (dtype, dim, count)
            if key in results and results[key]:
                latency = results[key].get(f"{search_type}_{percentile}", 0)
                values.append(f"{latency:.3f}")
            else:
                values.append("N/A")
        rows.append(f"| **{dtype}** | {' | '.join(values)} |")

    return "\n".join(rows)


def main():
    cpu_results = get_results("cpu")
    metal_results = get_results("metal")

    print(f"Loaded {len(cpu_results)} CPU results")
    print(f"Loaded {len(metal_results)} Metal results")

    def get_latest_matrix(prefix):
        files = glob.glob(f"/Users/rsd/REPOS/longbow/data/perf_logs/perf_matrix_{prefix}_*.json")
        if not files:
            return None
        return max(files, key=os.path.getmtime)

    recommend_file = get_latest_matrix("recommend")
    graphrag_file = get_latest_matrix("graphrag")
    deletion_file = get_latest_matrix("deletion")

    recommend_results = []
    if recommend_file and os.path.exists(recommend_file):
        with open(recommend_file) as f:
            recommend_results = json.load(f).get("results", [])

    graphrag_results = []
    if graphrag_file and os.path.exists(graphrag_file):
        with open(graphrag_file) as f:
            graphrag_results = json.load(f).get("results", [])

    deletion_results = []
    if deletion_file and os.path.exists(deletion_file):
        with open(deletion_file) as f:
            deletion_results = json.load(f).get("results", [])

    print(f"Loaded {len(recommend_results)} Recommend results")
    print(f"Loaded {len(graphrag_results)} GraphRAG results")
    print(f"Loaded {len(deletion_results)} Deletion results")

    # Generate the markdown
    md = []
    md.append("# Performance Documentation")
    md.append("")
    md.append(f"**Generated**: 2026-04-17")
    md.append("**Platform**: Darwin arm64 (Apple Silicon)")
    md.append("**Test Tool**: Longbow Unified Benchmark Script")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Test Configuration")
    md.append("")
    md.append("| Parameter | Value |")
    md.append("|-----------|-------|")
    md.append("| Dimensions | 128, 384, 768, 1536, 3072 |")
    md.append("| Batch Sizes | 1,000, 3,000, 5,000, 7,000, 10,000, 15,000 |")
    md.append(
        "| Data Types | float32, float64, float16, int8, int16, int32, int64, uint8, uint16, uint32, uint64, complex64, complex128, turboquant |"
    )
    md.append("| Build Modes | CPU (CGO_ENABLED=0), Metal GPU (CGO_ENABLED=1) |")
    md.append("| Queries per Test | 30 |")
    md.append("| Duration per Test | 3 seconds |")
    md.append("")
    md.append("---")

    # CPU Ingest Performance
    md.append("## CPU Build - Ingest Performance (Vectors/Second)")
    md.append("")
    for count in COUNTS:
        md.append(generate_table(cpu_results, "cpu", count))
        md.append("")

    md.append("## CPU Build - Ingest Performance (MB/Second)")
    md.append("")
    for count in COUNTS:
        md.append(generate_mb_table(cpu_results, "cpu", count))
        md.append("")

    md.append("## CPU Build - Retrieve Performance (MB/Second)")
    md.append("")
    for count in COUNTS:
        md.append(generate_get_mb_table(cpu_results, "cpu", count))
        md.append("")

    # CPU Search Performance
    md.append("## CPU Build - Search Performance (QPS)")
    md.append("")
    for count in COUNTS:
        md.append(generate_search_table(cpu_results, "cpu", count, "dense"))
        md.append("")

    # CPU Latency
    md.append("## CPU Build - Search Latency (P50 ms)")
    md.append("")
    for count in COUNTS:
        md.append(generate_latency_table(cpu_results, "cpu", count, "dense", "p50"))
        md.append("")

    md.append("## CPU Build - Search Latency (P95 ms)")
    md.append("")
    for count in COUNTS:
        md.append(generate_latency_table(cpu_results, "cpu", count, "dense", "p95"))
        md.append("")

    md.append("## CPU Build - Search Latency (P99 ms)")
    md.append("")
    for count in COUNTS:
        md.append(generate_latency_table(cpu_results, "cpu", count, "dense", "p99"))
        md.append("")

    # Metal Ingest Performance
    md.append("---")
    md.append("")
    md.append("## Metal Build - Ingest Performance (Vectors/Second)")
    md.append("")
    for count in COUNTS:
        md.append(generate_table(metal_results, "metal", count))
        md.append("")

    md.append("## Metal Build - Ingest Performance (MB/Second)")
    md.append("")
    for count in COUNTS:
        md.append(generate_mb_table(metal_results, "metal", count))
        md.append("")

    md.append("## Metal Build - Retrieve Performance (MB/Second)")
    md.append("")
    for count in COUNTS:
        md.append(generate_get_mb_table(metal_results, "metal", count))
        md.append("")

    # Metal Search Performance
    md.append("## Metal Build - Search Performance (QPS)")
    md.append("")
    for count in COUNTS:
        md.append(generate_search_table(metal_results, "metal", count, "dense"))
        md.append("")

    # Metal Latency
    md.append("## Metal Build - Search Latency (P50 ms)")
    md.append("")
    for count in COUNTS:
        md.append(generate_latency_table(metal_results, "metal", count, "dense", "p50"))
        md.append("")

    md.append("## Metal Build - Search Latency (P95 ms)")
    md.append("")
    for count in COUNTS:
        md.append(generate_latency_table(metal_results, "metal", count, "dense", "p95"))
        md.append("")

    md.append("## Metal Build - Search Latency (P99 ms)")
    md.append("")
    for count in COUNTS:
        md.append(generate_latency_table(metal_results, "metal", count, "dense", "p99"))
        md.append("")

    # Recommend Performance
    if recommend_results:
        md.append("---")
        md.append("")
        md.append("## Recommend Performance (Hybrid vs ANN)")
        md.append("")
        md.append("| Alpha | K | QPS | P50 (ms) | P95 (ms) | P99 (ms) |")
        md.append("|-------|---|-----|----------|----------|----------|")
        for r in recommend_results:
            md.append(
                f"| {r.get('alpha', 'N/A')} | {r.get('k', 'N/A')} | {r.get('qps', 0):,.1f} | {r.get('p50', 0):.3f} | {r.get('p95', 0):.3f} | {r.get('p99', 0):.3f} |"
            )
        md.append("")

    # GraphRAG Performance
    md.append("---")
    md.append("")
    md.append("## GraphRAG Performance (Graph Spreading)")
    md.append("")
    md.append("| Alpha | K | QPS | P50 (ms) | P95 (ms) | P99 (ms) |")
    md.append("|-------|---|-----|----------|----------|----------|")
    for r in graphrag_results:
        md.append(
            f"| {r.get('alpha', 'N/A')} | {r.get('k', 'N/A')} | {r.get('qps', 0):,.1f} | {r.get('p50', 0):.3f} | {r.get('p95', 0):.3f} | {r.get('p99', 0):.3f} |"
        )

    # Deletion Performance
    md.append("")
    md.append("## Deletion Performance (Tombstone Operations)")
    md.append("")
    md.append("| Total Vectors | Deleted | Delete Time (ms) | Search Time (ms) |")
    md.append("|---------------|---------|------------------|------------------|")
    for r in deletion_results:
        md.append(
            f"| {r.get('count', 'N/A')} | {r.get('deleted', 'N/A')} | {r.get('delete_time_ms', 0):.2f} | {r.get('search_time_ms', 0):.2f} |"
        )

    # Write to file
    output = "\n".join(md)
    with open("/Users/rsd/REPOS/longbow/docs/performance.md", "w") as f:
        f.write(output)

    print("Written to docs/performance.md")


if __name__ == "__main__":
    main()
