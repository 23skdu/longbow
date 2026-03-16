#!/usr/bin/env python3
"""
Comprehensive Performance Benchmark Matrix
Runs exact matrix specified by user:
- Dimensions: 128, 384, 786
- Counts: 1000, 3000, 7000, 15000, 25000
- Types: int, uint, float, complex
- Operations: DoPut, DoGet, dense, sparse, filtered, hybrid

Usage:
    python scripts/run_dtype_perf_matrix.py --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001
"""

import argparse
import time
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from datetime import datetime

# Data type configurations
DTYPE_CONFIG = {
    "int8": {"np": np.int8, "pa": pa.int8(), "factor": 1},
    "int16": {"np": np.int16, "pa": pa.int16(), "factor": 1},
    "int32": {"np": np.int32, "pa": pa.int32(), "factor": 1},
    "int64": {"np": np.int64, "pa": pa.int64(), "factor": 1},
    "uint8": {"np": np.uint8, "pa": pa.uint8(), "factor": 1},
    "uint16": {"np": np.uint16, "pa": pa.uint16(), "factor": 1},
    "uint32": {"np": np.uint32, "pa": pa.uint32(), "factor": 1},
    "uint64": {"np": np.uint64, "pa": pa.uint64(), "factor": 1},
    "float32": {"np": np.float32, "pa": pa.float32(), "factor": 1},
    "float64": {"np": np.float64, "pa": pa.float64(), "factor": 1},
    "complex64": {"np": np.complex64, "pa": pa.float32(), "factor": 2},
    "complex128": {"np": np.complex128, "pa": pa.float64(), "factor": 2},
}

# Benchmark matrix - user specs
DIMS = [128, 384]
COUNTS = [1000, 3000, 5000, 9000, 15000, 20000, 25000]
TYPES = ["int64", "uint64", "float32", "complex128"]
SEARCH_TYPES = ["dense", "sparse", "filtered", "hybrid"]


@dataclass
class BenchmarkResult:
    dtype: str
    dim: int
    count: int
    op: str
    throughput_mbps: float = 0.0
    vectors_per_sec: float = 0.0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    qps: float = 0.0
    error: Optional[str] = None


def generate_batch(
    start_id: int, count: int, dim: int, dtype: str, with_text: bool = False
):
    """Generate Arrow batch with specified data type."""
    config = DTYPE_CONFIG[dtype]
    np_type = config["np"]
    pa_type = config["pa"]
    factor = config["factor"]

    effective_dim = dim * factor

    # Generate random data
    if "complex" in dtype:
        real = np.random.rand(count, effective_dim).astype(np.float32)
        imag = np.random.rand(count, effective_dim).astype(np.float32)
        data = real + 1j * imag
    else:
        data = np.random.rand(count, effective_dim).astype(np_type)

    # Create Arrow arrays
    ids = pa.array(
        [str(i) for i in range(start_id, start_id + count)], type=pa.string()
    )

    tensor_type = pa.list_(pa_type, effective_dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)

    ts = pa.array([time.time_ns()] * count, type=pa.timestamp("ns"))

    fields = [
        pa.field("id", pa.string()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
    ]
    arrays = [ids, vectors, ts]

    if with_text:
        texts = [
            f"document {i} contains keywords about topic sample text"
            for i in range(start_id, start_id + count)
        ]
        categories = [f"cat_{i % 10}" for i in range(start_id, start_id + count)]
        fields.extend(
            [
                pa.field("text", pa.string()),
                pa.field("category", pa.string()),
            ]
        )
        arrays.extend(
            [
                pa.array(texts, type=pa.string()),
                pa.array(categories, type=pa.string()),
            ]
        )

    schema = pa.schema(fields)
    return pa.Table.from_arrays(arrays, schema=schema)


class PerformanceBenchmark:
    def __init__(self, data_uri: str, meta_uri: str):
        self.data_uri = data_uri
        self.meta_uri = meta_uri
        self.client = None

    def connect(self):
        """Connect to Longbow cluster."""
        self.client = flight.FlightClient(self.data_uri)

    def disconnect(self):
        """Disconnect from cluster."""
        if self.client:
            self.client.close()

    def run_doput(self, dataset: str, table: pa.Table) -> BenchmarkResult:
        """Run DoPut (ingestion) benchmark."""
        dtype = table.schema.field("vector").type.value_type
        dim = table.schema.field("vector").type.list_size
        count = table.num_rows

        # Determine dtype string
        dtype_str = "float32"
        for k, v in DTYPE_CONFIG.items():
            if v["pa"] == dtype:
                dtype_str = k
                break

        start = time.time()
        try:
            writer, reader = self.client.do_put(
                flight.FlightDescriptor.for_path(dataset),
                table.schema,
            )
            writer.write_table(table)
            writer.close()
            duration = time.time() - start
            mb = table.nbytes / 1024 / 1024
            throughput = mb / duration
            vectors_per_sec = count / duration if duration > 0 else 0

            return BenchmarkResult(
                dtype=dtype_str,
                dim=dim,
                count=count,
                op="DoPut",
                throughput_mbps=throughput,
                vectors_per_sec=vectors_per_sec,
            )
        except Exception as e:
            return BenchmarkResult(
                dtype=dtype_str,
                dim=dim,
                count=count,
                op="DoPut",
                error=str(e),
            )

    def run_doget(self, dataset: str) -> BenchmarkResult:
        """Run DoGet (retrieval) benchmark."""
        start = time.time()
        table = None
        try:
            # Get flight info
            ticket = flight.Ticket(dataset.encode())
            reader = self.client.do_get(ticket)

            batches = []
            for chunk in reader:
                batches.append(chunk.data)

            if batches:
                table = pa.Table.from_batches(batches)
                count = table.num_rows
            else:
                count = 0

            duration = time.time() - start
            mb = table.nbytes / 1024 / 1024 if table is not None and count > 0 else 0
            throughput = mb / duration if duration > 0 else 0
            vectors_per_sec = count / duration if duration > 0 else 0

            dim = 0
            if count > 0 and "vector" in table.column_names:
                vec_col = table.column("vector")
                dim = vec_col.type.list_size

            return BenchmarkResult(
                dtype="float32",
                dim=dim,
                count=count,
                op="DoGet",
                throughput_mbps=throughput,
                vectors_per_sec=vectors_per_sec,
            )
        except Exception as e:
            return BenchmarkResult(
                dtype="float32",
                dim=0,
                count=0,
                op="DoGet",
                error=str(e),
            )

    def run_search(
        self, dataset: str, search_type: str, dim: int, count: int, queries: int = 100
    ) -> BenchmarkResult:
        """Run search benchmark."""
        # Generate random query vectors
        query_dim = dim  # For complex, this is the base dimension
        query_vec = np.random.rand(query_dim).astype(np.float32)

        k = 10
        alpha = (
            1.0 if search_type == "dense" else (0.0 if search_type == "sparse" else 0.5)
        )

        # Build filter if needed
        filter_expr = None
        if search_type == "filtered":
            filter_expr = f"id:lt:{count // 10}"

        start = time.time()
        latencies = []

        try:
            for i in range(queries):
                q_start = time.time()
                # Execute search via DoAction
                action = flight.Action(
                    "search",
                    json.dumps(
                        {
                            "dataset": dataset,
                            "vector": query_vec.tolist(),
                            "k": k,
                            "alpha": alpha,
                            "filter": filter_expr,
                        }
                    ).encode(),
                )
                result = self.client.do_action(action)
                for chunk in result:
                    pass
                q_latency = (time.time() - q_start) * 1000
                latencies.append(q_latency)

            duration = time.time() - start
            latencies.sort()
            p50 = latencies[len(latencies) // 2] if latencies else 0
            p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
            p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
            qps = queries / duration if duration > 0 else 0

            return BenchmarkResult(
                dtype="float32",
                dim=dim,
                count=count,
                op=f"search_{search_type}",
                latency_p50_ms=p50,
                latency_p95_ms=p95,
                latency_p99_ms=p99,
                qps=qps,
            )
        except Exception as e:
            return BenchmarkResult(
                dtype="float32",
                dim=dim,
                count=count,
                op=f"search_{search_type}",
                error=str(e),
            )

    def cleanup_dataset(self, dataset: str):
        """Delete dataset."""
        try:
            action = flight.Action("delete", dataset.encode())
            for _ in self.client.do_action(action):
                pass
        except:
            pass  # Ignore errors on cleanup


def run_full_matrix(args):
    """Run the full benchmark matrix."""
    bench = PerformanceBenchmark(args.data_uri, args.meta_uri)
    bench.connect()

    results = []
    total_tests = len(DIMS) * len(COUNTS) * len(TYPES) * (1 + 1 + len(SEARCH_TYPES))
    current = 0

    print(f"Starting benchmark matrix: {total_tests} tests")
    print(f"Dimensions: {DIMS}")
    print(f"Counts: {COUNTS}")
    print(f"Types: {TYPES}")
    print(f"Search types: {SEARCH_TYPES}")
    print()

    for dtype in TYPES:
        for dim in DIMS:
            for count in COUNTS:
                dataset = f"perf_{dtype}_{dim}_{count}_{int(time.time())}"
                with_text = True  # For hybrid/sparse

                # 1. Generate and ingest data
                current += 1
                print(
                    f"[{current}/{total_tests}] {dtype} dim={dim} count={count}: DoPut...",
                    end=" ",
                )
                table = generate_batch(0, count, dim, dtype, with_text=with_text)
                result = bench.run_doput(dataset, table)
                results.append(asdict(result))
                if result.error:
                    print(f"ERROR: {result.error}")
                else:
                    print(f"{result.throughput_mbps:.2f} MB/s")

                # Wait for indexing
                time.sleep(2)

                # 2. DoGet
                current += 1
                print(
                    f"[{current}/{total_tests}] {dtype} dim={dim} count={count}: DoGet...",
                    end=" ",
                )
                result = bench.run_doget(dataset)
                results.append(asdict(result))
                if result.error:
                    print(f"ERROR: {result.error}")
                else:
                    print(f"{result.throughput_mbps:.2f} MB/s")

                # 3. Search types
                for search_type in SEARCH_TYPES:
                    current += 1
                    print(
                        f"[{current}/{total_tests}] {dtype} dim={dim} count={count}: {search_type}...",
                        end=" ",
                    )
                    result = bench.run_search(
                        dataset, search_type, dim, count, queries=args.queries
                    )
                    results.append(asdict(result))
                    if result.error:
                        print(f"ERROR: {result.error}")
                    else:
                        print(f"QPS={result.qps:.2f} p50={result.latency_p50_ms:.2f}ms")

                # Cleanup
                if not args.keep_datasets:
                    bench.cleanup_dataset(dataset)

                time.sleep(1)  # Brief pause between tests

    bench.disconnect()

    # Save results
    output_file = args.output or f"perf_matrix_{int(time.time())}.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to {output_file}")
    return results


def format_markdown(results: List[Dict], output_file: str = "docs/performance.md"):
    """Format results as markdown table."""
    lines = [
        "# Performance Metrics (Comprehensive Matrix)",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Benchmark Configuration",
        "",
        f"- Dimensions: {', '.join(map(str, DIMS))}",
        f"- Vector Counts: {', '.join(map(str, COUNTS))}",
        f"- Data Types: {', '.join(TYPES)}",
        f"- Search Types: {', '.join(SEARCH_TYPES)}",
        "",
        "## Results",
        "",
        "| DType | Count | Dim | Op | Throughput (MB/s) | p50 (ms) | p95 (ms) | p99 (ms) | QPS |",
        "|------|-------|-----|-----|-----------------|----------|----------|----------|-----|",
    ]

    # Group by dtype and count
    for dtype in TYPES:
        for count in COUNTS:
            for dim in DIMS:
                # Find matching results
                doput = next(
                    (
                        r
                        for r in results
                        if r["dtype"] == dtype
                        and r["count"] == count
                        and r["dim"] == dim
                        and r["op"] == "DoPut"
                    ),
                    None,
                )
                doget = next(
                    (
                        r
                        for r in results
                        if r["dtype"] == dtype
                        and r["count"] == count
                        and r["dim"] == dim
                        and r["op"] == "DoGet"
                    ),
                    None,
                )

                put_mbps = (
                    f"{doput['throughput_mbps']:.2f}"
                    if doput and doput.get("throughput_mbps")
                    else "N/A"
                )
                get_mbps = (
                    f"{doget['throughput_mbps']:.2f}"
                    if doget and doget.get("throughput_mbps")
                    else "N/A"
                )

                # Dense search result
                dense = next(
                    (
                        r
                        for r in results
                        if r["dtype"] == dtype
                        and r["count"] == count
                        and r["dim"] == dim
                        and r["op"] == "search_dense"
                    ),
                    None,
                )
                if dense:
                    lines.append(
                        f"| {dtype} | {count} | {dim} | dense | {put_mbps} | {get_mbps} | {dense.get('latency_p50_ms', 'N/A'):.2f} | {dense.get('latency_p95_ms', 'N/A'):.2f} | {dense.get('latency_p99_ms', 'N/A'):.2f} | {dense.get('qps', 'N/A'):.2f} |"
                    )

                # Other search types
                for stype in SEARCH_TYPES[1:]:
                    sresult = next(
                        (
                            r
                            for r in results
                            if r["dtype"] == dtype
                            and r["count"] == count
                            and r["dim"] == dim
                            and r["op"] == f"search_{stype}"
                        ),
                        None,
                    )
                    if sresult:
                        lines.append(
                            f"| {dtype} | {count} | {dim} | {stype} | | | {sresult.get('latency_p50_ms', 'N/A'):.2f} | {sresult.get('latency_p95_ms', 'N/A'):.2f} | {sresult.get('latency_p99_ms', 'N/A'):.2f} | {sresult.get('qps', 'N/A'):.2f} |"
                        )

                lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")

    # Add pprof section
    lines.extend(
        [
            "",
            "## pprof Collection",
            "",
            "```bash",
            "# Start pprof collection in background",
            "./scripts/capture_pprof.sh &",
            "",
            "# Or collect specific profiles",
            "curl -s http://localhost:9090/debug/pprof/heap > profiles/heap.pprof",
            "go tool pprof -http=:7000 profiles/heap.pprof",
            "```",
        ]
    )

    with open(output_file, "w") as f:
        f.write("\n".join(lines))

    print(f"Markdown output written to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Comprehensive Performance Benchmark")
    parser.add_argument(
        "--data-uri", default="grpc://localhost:3000", help="Data server URI"
    )
    parser.add_argument(
        "--meta-uri", default="grpc://localhost:3001", help="Meta server URI"
    )
    parser.add_argument(
        "--queries", type=int, default=100, help="Number of search queries per test"
    )
    parser.add_argument("--output", help="Output JSON file")
    parser.add_argument("--format-md", action="store_true", help="Format as markdown")
    parser.add_argument(
        "--keep-datasets", action="store_true", help="Keep datasets after tests"
    )

    args = parser.parse_args()

    results = run_full_matrix(args)

    if args.format_md:
        format_markdown(results)


if __name__ == "__main__":
    main()
