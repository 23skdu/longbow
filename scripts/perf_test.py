#!/usr/bin/env python3
"""Longbow Performance Test Suite

Comprehensive benchmarking for Longbow vector store including:
- Basic DoPut/DoGet throughput
- Vector similarity search (HNSW)
- Hybrid search (dense + sparse)
- Concurrent load testing
- Large dimension vectors
- S3 snapshot operations
- Memory pressure scenarios
"""
import argparse
import concurrent.futures
import json
import os
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

try:
    import dask.dataframe as dd
    HAS_DASK = True
except ImportError:
    HAS_DASK = False

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class BenchmarkResult:
    """Container for benchmark results."""
    name: str
    duration_seconds: float
    throughput: float
    throughput_unit: str
    rows: int = 0
    bytes_processed: int = 0
    latencies_ms: list = field(default_factory=list)
    errors: int = 0

    @property
    def p50_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        return statistics.median(self.latencies_ms)

    @property
    def p95_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = int(len(sorted_lat) * 0.95)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    @property
    def p99_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = int(len(sorted_lat) * 0.99)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]


# =============================================================================
# Data Generation
# =============================================================================

def generate_vectors(num_rows: int, dim: int, with_text: bool = False) -> pa.Table:
    """Generate random vectors with optional text field for hybrid search."""
    print(f"Generating {num_rows:,} vectors of dimension {dim}...")

    # Vector data
    data = np.random.rand(num_rows, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)

    # IDs
    ids = pa.array(np.arange(num_rows), type=pa.int64())

    # Timestamps
    now = pd.Timestamp.now()
    timestamps = [now - pd.Timedelta(minutes=i) for i in range(num_rows)]
    ts_array = pa.array(timestamps, type=pa.timestamp("ns"))

    fields = [
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
    ]
    arrays = [ids, vectors, ts_array]

    # Optional text field for hybrid search
    if with_text:
        words = ["machine", "learning", "vector", "database", "search",
                 "embedding", "neural", "network", "model", "data"]
        texts = [" ".join(np.random.choice(words, size=10)) for _ in range(num_rows)]
        text_array = pa.array(texts, type=pa.string())
        fields.append(pa.field("meta", pa.string()))
        arrays.append(text_array)

    # Schema: id (int64), vector (FixedSizeList<float32>[dim]), timestamp (Timestamp[ns]), meta (string)
    # The server strictly validates that NumColumns matches NumFields in schema.
    schema = pa.schema(fields)
    return pa.Table.from_arrays(arrays, schema=schema)


def generate_query_vectors(num_queries: int, dim: int) -> np.ndarray:
    """Generate random query vectors."""
    return np.random.rand(num_queries, dim).astype(np.float32)


# =============================================================================
# Basic Benchmarks
# =============================================================================

def benchmark_put(client: flight.FlightClient, table: pa.Table, name: str) -> BenchmarkResult:
    """Benchmark DoPut operation."""
    print(f"\n[PUT] Uploading dataset '{name}'...")
    descriptor = flight.FlightDescriptor.for_path(name)

    start_time = time.time()
    writer, _ = client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.close()
    duration = time.time() - start_time

    mb = table.nbytes / 1024 / 1024
    throughput = mb / duration
    throughput_rows = table.num_rows / duration

    print(f"[PUT] Completed in {duration:.4f}s ({mb:.2f} MB)")
    print(f"[PUT] Throughput: {throughput:.2f} MB/s ({throughput_rows:.1f} rows/s)")

    return BenchmarkResult(
        name="DoPut",
        duration_seconds=duration,
        throughput=throughput,
        throughput_unit="MB/s",
        rows=table.num_rows,
        bytes_processed=table.nbytes,
    )


def benchmark_get(client: flight.FlightClient, name: str,
                  filters: Optional[list] = None) -> BenchmarkResult:
    """Benchmark DoGet operation."""
    filter_desc = f"with filters: {filters}" if filters else "(full scan)"
    print(f"\n[GET] Downloading dataset '{name}' {filter_desc}...")

    query = {"name": name}
    if filters:
        query["filters"] = filters

    ticket = flight.Ticket(json.dumps(query).encode("utf-8"))

    start_time = time.time()
    try:
        reader = client.do_get(ticket)
        table = reader.read_all()
    except flight.FlightError as e:
        print(f"[GET] Error: {e}")
        return BenchmarkResult(
            name="DoGet", duration_seconds=0, throughput=0,
            throughput_unit="MB/s", errors=1
        )

    duration = time.time() - start_time
    mb = table.nbytes / 1024 / 1024
    throughput = mb / duration if duration > 0 else 0
    throughput_rows = table.num_rows / duration if duration > 0 else 0

    print(f"[GET] Retrieved {table.num_rows:,} rows in {duration:.4f}s")
    print(f"[GET] Throughput: {throughput:.2f} MB/s ({throughput_rows:.1f} rows/s)")

    return BenchmarkResult(
        name="DoGet",
        duration_seconds=duration,
        throughput=throughput,
        throughput_unit="MB/s",
        rows=table.num_rows,
        bytes_processed=table.nbytes,
    )


# =============================================================================
# Vector Search Benchmarks
# =============================================================================

def benchmark_vector_search(client: flight.FlightClient, name: str,
                            query_vectors: np.ndarray, k: int,
                            filters: Optional[list] = None) -> BenchmarkResult:
    """Benchmark HNSW vector similarity search using DoAction(VectorSearch)."""
    num_queries = len(query_vectors)
    print(f"\n[SEARCH] Running {num_queries:,} vector searches (k={k})...")

    latencies = []
    errors = 0
    total_results = 0

    for i, qvec in enumerate(query_vectors):
        # NOTE: Using DoAction "VectorSearch" as per server implementation
        # Payload must match internal/store/vector_search_action.go: VectorSearchRequest
        # {"dataset": "name", "vector": [...], "k": 10}
        body = {
            "dataset": name,
            "vector": qvec.tolist(),
            "k": k,
        }
        if filters:
            body["filters"] = filters
            
        request_body = json.dumps(body).encode("utf-8")
        
        start = time.time()
        try:
            action = flight.Action("VectorSearch", request_body)
            # DoAction returns an iterator of FlightResult
            results_iter = client.do_action(action)
            
            # The server sends back one JSON result with "ids", "scores", etc.
            # Example response: {"ids": [1, 2], "scores": [0.9, 0.8], "vectors": [...]}
            for result in results_iter:
                # Just consuming the result for benchmarking
                payload = json.loads(result.body.to_pybytes())
                if "ids" in payload:
                    total_results += len(payload["ids"])
                    
        except flight.FlightError as e:
            errors += 1
            if errors <= 3:
                print(f"[SEARCH] Error on query {i}: {e}")
            continue
        except Exception as e:
            errors += 1
            print(f"[SEARCH] Unexpected error on query {i}: {e}")
            continue

        latencies.append((time.time() - start) * 1000)  # ms

        if (i + 1) % 100 == 0:
            print(f"[SEARCH] Completed {i + 1}/{num_queries} queries...")

    duration = sum(latencies) / 1000  # total seconds
    qps = num_queries / duration if duration > 0 else 0

    result = BenchmarkResult(
        name="VectorSearch",
        duration_seconds=duration,
        throughput=qps,
        throughput_unit="queries/s",
        rows=total_results,
        latencies_ms=latencies,
        errors=errors,
    )

    print(f"[SEARCH] Completed: {qps:.2f} queries/s")
    print(f"[SEARCH] Latency p50={result.p50_ms:.2f}ms p95={result.p95_ms:.2f}ms p99={result.p99_ms:.2f}ms")
    print(f"[SEARCH] Errors: {errors}")

    return result


def benchmark_hybrid_search(client: flight.FlightClient, name: str,
                            query_vectors: np.ndarray, k: int,
                            text_queries: list) -> BenchmarkResult:
    """Benchmark hybrid search (dense vectors + sparse text) using DoAction(VectorSearch)."""
    num_queries = len(query_vectors)
    print(f"\n[HYBRID] Running {num_queries:,} hybrid searches (k={k})...")

    latencies = []
    errors = 0
    total_results = 0

    for i, (qvec, text_query) in enumerate(zip(query_vectors, text_queries)):
        # NOTE: Check if server supports "HybridSearch" action or if it's integrated into "VectorSearch"
        # Since this is a stress test, we'll try "VectorSearch" with extra fields if supported,
        # otherwise, this benchmarking function might need server-side support adjustment.
        # Assuming typical JSON payload extension:
        request_body = json.dumps({
            "dataset": name,
            "vector": qvec.tolist(),
            "text_query": text_query,
            "k": k,
            "alpha": 0.5,
            # NOTE: Current server implementation of "VectorSearch" action ignores "text_query" and "alpha".
        }).encode("utf-8")

        start = time.time()
        try:
            # Note: Changed from "hybrid_search" to "VectorSearch" to match standard pattern
            # If a separate action type is needed, it should be defined in valid server actions.
            # Here we assume "VectorSearch" handles optional hybrid fields.
            action = flight.Action("VectorSearch", request_body)
            results_iter = client.do_action(action)
            
            for result in results_iter:
                payload = json.loads(result.body.to_pybytes())
                if "ids" in payload:
                    total_results += len(payload["ids"])

        except flight.FlightError as e:
            errors += 1
            if errors <= 3:
                print(f"[HYBRID] Error on query {i}: {e}")
            continue

        latencies.append((time.time() - start) * 1000)

        if (i + 1) % 100 == 0:
            print(f"[HYBRID] Completed {i + 1}/{num_queries} queries...")

    duration = sum(latencies) / 1000
    qps = num_queries / duration if duration > 0 else 0

    result = BenchmarkResult(
        name="HybridSearch",
        duration_seconds=duration,
        throughput=qps,
        throughput_unit="queries/s",
        rows=total_results,
        latencies_ms=latencies,
        errors=errors,
    )

    print(f"[HYBRID] Completed: {qps:.2f} queries/s")
    print(f"[HYBRID] Latency p50={result.p50_ms:.2f}ms p95={result.p95_ms:.2f}ms p99={result.p99_ms:.2f}ms")

    return result


def benchmark_delete(client: flight.FlightClient, name: str, ids: list) -> BenchmarkResult:
    """Benchmark vector deletion via DoAction(delete-vector)."""
    print(f"\n[DELETE] Deleting {len(ids):,} vectors from '{name}'...")
    
    latencies = []
    errors = 0
    success = 0
    
    start_total = time.time()
    for i, vid in enumerate(ids):
        # NOTE: Current server implementation handles single ID
        body = {
            "dataset": name,
            "vector_id": float(vid)
        }
        request_body = json.dumps(body).encode("utf-8")
        
        start = time.time()
        try:
            action = flight.Action("delete-vector", request_body)
            # Fetch results to ensure completion
            list(client.do_action(action))
            success += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"[DELETE] Error on ID {vid}: {e}")
            continue
            
        latencies.append((time.time() - start) * 1000)
        
        if (i + 1) % 1000 == 0:
            print(f"[DELETE] Completed {i + 1}/{len(ids)} deletions...")
            
    duration = time.time() - start_total
    throughput = success / duration if duration > 0 else 0
    
    result = BenchmarkResult(
        name="Delete",
        duration_seconds=duration,
        throughput=throughput,
        throughput_unit="ops/s",
        rows=success,
        latencies_ms=latencies,
        errors=errors,
    )
    
    print(f"[DELETE] Completed: {throughput:.2f} ops/s")
    print(f"[DELETE] Latency p50={result.p50_ms:.2f}ms p95={result.p95_ms:.2f}ms p99={result.p99_ms:.2f}ms")
    print(f"[DELETE] Errors: {errors}")
    
    return result


# =============================================================================
# Concurrent Load Testing
# =============================================================================

def benchmark_concurrent_load(data_uri: str, meta_uri: str, name: str, dim: int,
                              num_workers: int, duration_seconds: int,
                              operation: str = "mixed") -> BenchmarkResult:
    """Benchmark concurrent load with multiple clients."""
    print(f"\n[CONCURRENT] Running {num_workers} workers for {duration_seconds}s ({operation})...")

    stop_event = threading.Event()
    results_lock = threading.Lock()
    all_latencies = []
    total_ops = 0
    total_errors = 0

    def worker(worker_id: int):
        nonlocal total_ops, total_errors
        # Each worker creates its own clients
        data_client = flight.FlightClient(data_uri)
        # Meta client unused for put/get but here for completeness if op expanded
        # meta_client = flight.FlightClient(meta_uri) 
        
        local_latencies = []
        local_ops = 0
        local_errors = 0

        # Small batch for concurrent testing
        small_table = generate_vectors(100, dim)
        worker_name = f"{name}_worker_{worker_id}"

        while not stop_event.is_set():
            start = time.time()
            try:
                if operation == "put" or (operation == "mixed" and local_ops % 2 == 0):
                    descriptor = flight.FlightDescriptor.for_path(worker_name)
                    writer, _ = data_client.do_put(descriptor, small_table.schema)
                    writer.write_table(small_table)
                    writer.close()
                else:
                    query = {"name": worker_name}
                    ticket = flight.Ticket(json.dumps(query).encode("utf-8"))
                    reader = data_client.do_get(ticket)
                    _ = reader.read_all()

                local_latencies.append((time.time() - start) * 1000)
                local_ops += 1
            except Exception:
                local_errors += 1
                # Small sleep on error to avoid tight spin loop
                time.sleep(0.01)

        with results_lock:
            all_latencies.extend(local_latencies)
            total_ops += local_ops
            total_errors += local_errors

    # Start workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker, i) for i in range(num_workers)]
        time.sleep(duration_seconds)
        stop_event.set()
        concurrent.futures.wait(futures)

    ops_per_sec = total_ops / duration_seconds if duration_seconds > 0 else 0

    result = BenchmarkResult(
        name=f"Concurrent_{operation}",
        duration_seconds=duration_seconds,
        throughput=ops_per_sec,
        throughput_unit="ops/s",
        rows=total_ops,
        latencies_ms=all_latencies,
        errors=total_errors,
    )

    print(f"[CONCURRENT] Total ops: {total_ops:,} ({ops_per_sec:.2f} ops/s)")
    print(f"[CONCURRENT] Latency p50={result.p50_ms:.2f}ms p95={result.p95_ms:.2f}ms p99={result.p99_ms:.2f}ms")
    print(f"[CONCURRENT] Errors: {total_errors}")

    return result


# =============================================================================
# S3 Snapshot Benchmarks
# =============================================================================

def benchmark_s3_snapshot(client: flight.FlightClient, name: str,
                          s3_endpoint: str, s3_bucket: str) -> BenchmarkResult:
    """Benchmark S3 snapshot write/read operations using Meta client."""
    if not HAS_BOTO3:
        print("[S3] boto3 not installed, skipping S3 benchmark")
        return BenchmarkResult(
            name="S3Snapshot", duration_seconds=0, throughput=0,
            throughput_unit="MB/s", errors=1
        )

    print(f"\n[S3] Benchmarking snapshot to {s3_endpoint}/{s3_bucket}...")

    # Trigger snapshot via DoAction on Meta Server
    snapshot_request = json.dumps({
        "action": "snapshot",
        "collection": name,
        "backend": "s3",
        "endpoint": s3_endpoint,
        "bucket": s3_bucket,
    }).encode("utf-8")

    start = time.time()
    try:
        # Use simple "snapshot" or "force_snapshot" based on server impl. 
        # Server code shows "force_snapshot" in DoAction switch (store.go)
        action = flight.Action("force_snapshot", snapshot_request)
        results = list(client.do_action(action))
        duration = time.time() - start

        # Parse response for bytes written
        response = json.loads(results[0].body.to_pybytes()) if results else {}
        bytes_written = response.get("bytes", 0)  # Metric might need server support
        mb = bytes_written / 1024 / 1024
        throughput = mb / duration if duration > 0 else 0

        print(f"[S3] Snapshot completed in {duration:.4f}s")
        
        return BenchmarkResult(
            name="S3Snapshot",
            duration_seconds=duration,
            throughput=throughput,
            throughput_unit="MB/s",
            bytes_processed=bytes_written,
        )
    except Exception as e:
        print(f"[S3] Error: {e}")
        return BenchmarkResult(
            name="S3Snapshot", duration_seconds=0, throughput=0,
            throughput_unit="MB/s", errors=1
        )


# =============================================================================
# Memory Pressure Testing
# =============================================================================

def benchmark_memory_pressure(data_loc: str, memory_limit_mb: int,
                              dim: int) -> BenchmarkResult:
    """Benchmark behavior under memory pressure."""
    print(f"\n[MEMORY] Testing with {memory_limit_mb}MB limit...")

    client = flight.FlightClient(data_loc)

    # Calculate rows to exceed memory limit
    bytes_per_row = dim * 4 + 8 + 8  # vector + id + timestamp
    target_bytes = memory_limit_mb * 1024 * 1024 * 2  # 2x limit
    target_rows = target_bytes // bytes_per_row
    batch_rows = min(10000, max(100, target_rows // 100))

    print(f"[MEMORY] Inserting {target_rows:,} rows in batches of {batch_rows:,}...")

    latencies = []
    total_rows = 0
    errors = 0
    evictions = 0

    start_total = time.time()
    batch_num = 0

    # Safety break
    while total_rows < target_rows:
        batch = generate_vectors(batch_rows, dim)
        name = f"memory_test_{batch_num}"

        start = time.time()
        try:
            descriptor = flight.FlightDescriptor.for_path(name)
            writer, _ = client.do_put(descriptor, batch.schema)
            writer.write_table(batch)
            writer.close()
            latencies.append((time.time() - start) * 1000)
            total_rows += batch_rows
        except flight.FlightError as e:
            if "memory" in str(e).lower() or "limit" in str(e).lower():
                evictions += 1
                # print(f"[MEMORY] Limit hit at {total_rows:,} rows")
                # Break early if we hit the limit hard
                break
            else:
                errors += 1
                print(f"[MEMORY] Error: {e}")

        batch_num += 1
        if batch_num % 10 == 0:
            print(f"[MEMORY] Progress: {total_rows:,}/{target_rows:,} rows")

    duration = time.time() - start_total
    rows_per_sec = total_rows / duration if duration > 0 else 0

    result = BenchmarkResult(
        name="MemoryPressure",
        duration_seconds=duration,
        throughput=rows_per_sec,
        throughput_unit="rows/s",
        rows=total_rows,
        latencies_ms=latencies,
        errors=errors,
    )

    print(f"[MEMORY] Completed: {total_rows:,} rows in {duration:.2f}s")
    print(f"[MEMORY] Throughput: {rows_per_sec:.2f} rows/s")
    print(f"[MEMORY] Evictions/Limits: {evictions}, Errors: {errors}")

    return result


# =============================================================================
# Report Generation
# =============================================================================

def print_report(results: list):
    """Print benchmark summary report."""
    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY")
    print("=" * 70)

    header = f'{"Benchmark":<20} {"Duration":>10} {"Throughput":>15} {"p50":>8} {"p95":>8} {"p99":>8}'
    print(header)
    print("-" * 70)

    for r in results:
        p50 = f"{r.p50_ms:.1f}ms" if r.latencies_ms else "N/A"
        p95 = f"{r.p95_ms:.1f}ms" if r.latencies_ms else "N/A"
        p99 = f"{r.p99_ms:.1f}ms" if r.latencies_ms else "N/A"
        tput = f"{r.throughput:.2f}"
        print(f"{r.name:<20} {r.duration_seconds:>9.2f}s {tput:>10} {r.throughput_unit:<7} {p50:>8} {p95:>8} {p99:>8}")

    print("=" * 70)


def export_json(results: list, filepath: str):
    """Export results to JSON file."""
    data = []
    for r in results:
        data.append({
            "name": r.name,
            "duration_seconds": r.duration_seconds,
            "throughput": r.throughput,
            "throughput_unit": r.throughput_unit,
            "rows": r.rows,
            "bytes_processed": r.bytes_processed,
            "p50_ms": r.p50_ms,
            "p95_ms": r.p95_ms,
            "p99_ms": r.p99_ms,
            "errors": r.errors,
        })

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nResults exported to {filepath}")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Longbow Performance Test Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  # Basic throughput test
  %(prog)s --rows 100000 --dim 128

  # With separate ports
  %(prog)s --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001

  # Vector search benchmark
  %(prog)s --search --search-k 10 --query-count 1000
"""
    )

    # Connection
    parser.add_argument("--data-uri", default="grpc://0.0.0.0:3000", help="Data Server URI")
    parser.add_argument("--meta-uri", default="grpc://0.0.0.0:3001", help="Meta Server URI")

    # Data generation
    parser.add_argument("--rows", default=10000, type=int, help="Number of rows")
    parser.add_argument("--dim", default=128, type=int,
                        help="Vector dimension (128, 768, 1536, 3072)")
    parser.add_argument("--name", default="perf_test", help="Dataset name")

    # Vector search
    parser.add_argument("--search", action="store_true", help="Run vector search benchmark")
    parser.add_argument("--search-k", default=10, type=int, help="Top-k results")
    parser.add_argument("--query-count", default=1000, type=int, help="Number of queries")

    # Vector delete
    parser.add_argument("--delete", action="store_true", help="Run vector delete benchmark")
    parser.add_argument("--delete-count", default=1000, type=int, help="Number of deletions")

    # Hybrid search
    parser.add_argument("--hybrid", action="store_true", help="Run hybrid search benchmark")
    parser.add_argument("--text-field", default="meta", help="Text field for sparse search")

    # Concurrent load
    parser.add_argument("--concurrent", default=0, type=int,
                        help="Parallel clients for load test")
    parser.add_argument("--duration", default=60, type=int,
                        help="Duration in seconds for concurrent test")
    parser.add_argument("--operation", default="mixed",
                        choices=["put", "get", "mixed"], help="Operation type")

    # Memory pressure
    parser.add_argument("--memory-pressure", action="store_true",
                        help="Run memory pressure test")
    parser.add_argument("--memory-limit", default=512, type=int,
                        help="Memory limit in MB for pressure test")

    # S3 snapshot
    parser.add_argument("--s3", action="store_true", help="Run S3 snapshot benchmark")
    parser.add_argument("--s3-endpoint", default="localhost:9000", help="S3 endpoint")
    parser.add_argument("--s3-bucket", default="longbow-snapshots", help="S3 bucket")

    # Output
    parser.add_argument("--json", help="Export results to JSON file")
    parser.add_argument("--all", action="store_true", help="Run all benchmarks")

    # Filters (legacy support)
    parser.add_argument("--filter", action="append",
                        help="Filter in format field:op:value")

    args = parser.parse_args()

    print("Longbow Performance Test Suite")
    print("=" * 40)
    print(f"Data Server: {args.data_uri}")
    print(f"Meta Server: {args.meta_uri}")
    print(f"Vector Dimension: {args.dim}")
    print(f"Rows: {args.rows:,}")

    # Parse filters
    filters = []
    if args.filter:
        for f in args.filter:
            parts = f.split(':')
            if len(parts) == 3:
                filters.append({"field": parts[0], "operator": parts[1], "value": parts[2]})
    
    results = []

    try:
        data_client = flight.FlightClient(args.data_uri)
        meta_client = flight.FlightClient(args.meta_uri)
    except Exception as e:
        print(f"Failed to connect: {e}")
        sys.exit(1)

    # Always run basic PUT/GET
    include_text = args.hybrid or args.all
    table = generate_vectors(args.rows, args.dim, with_text=include_text)

    # Data Plane operations
    results.append(benchmark_put(data_client, table, args.name))
    results.append(benchmark_get(data_client, args.name, filters=filters if filters else None))

    # Meta Plane operations (Search)
    if args.search or args.all:
        query_vectors = generate_query_vectors(args.query_count, args.dim)
        # Search goes to Meta Server
        results.append(benchmark_vector_search(
            meta_client, args.name, query_vectors, args.search_k, filters=filters if filters else None
        ))

    # Meta Plane operations (Hybrid Search)
    if args.hybrid or args.all:
        query_vectors = generate_query_vectors(args.query_count, args.dim)
        text_queries = ["machine learning neural"] * args.query_count
        # Hybrid Search goes to Meta Server
        results.append(benchmark_hybrid_search(
            meta_client, args.name, query_vectors, args.search_k, text_queries
        ))

    # Meta Plane operations (Delete)
    if args.delete or args.all:
        count = min(args.delete_count, args.rows)
        # Delete first N IDs
        ids_to_delete = list(range(count))
        results.append(benchmark_delete(meta_client, args.name, ids_to_delete))

    # Concurrent load (Data Plane focus)
    if args.concurrent > 0 or args.all:
        workers = args.concurrent if args.concurrent > 0 else 8
        results.append(benchmark_concurrent_load(
            args.data_uri, args.meta_uri, args.name, args.dim, workers, args.duration, args.operation
        ))

    # S3 snapshot (Meta Plane trigger)
    if args.s3 or args.all:
        results.append(benchmark_s3_snapshot(
            meta_client, args.name, args.s3_endpoint, args.s3_bucket
        ))

    # Memory pressure (Data Plane focus)
    if args.memory_pressure or args.all:
        results.append(benchmark_memory_pressure(
            args.data_uri, args.memory_limit, args.dim
        ))

    # Report
    print_report(results)

    if args.json:
        export_json(results, args.json)

    return 0


if __name__ == "__main__":
    sys.exit(main())
