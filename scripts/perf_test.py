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
import random
import sys
import time
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


def check_cluster_health(client: flight.FlightClient) -> bool:
    """Check cluster status before running benchmarks."""
    print("Checking cluster health...")
    try:
        action = flight.Action("cluster-status", b"")
        results = list(client.do_action(action))
        if not results:
            print("WARN: No status returned from cluster")
            return False
            
        status = json.loads(results[0].body.to_pybytes())
        count = status.get("count", 0)
        print(f"Cluster Healthy: {count} active members")
        for m in status.get("members", []):
            print(f" - {m.get('ID')} ({m.get('Addr')}) [{m.get('Status', 'Unknown')}]")
            
        return count > 0
    except Exception as e:
        print(f"Cluster check failed: {e}")
        return False


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
    
    # To distribute data in a cluster, we send multiple PUTs with different routing keys
    # The PartitionProxyInterceptor will forward these to different nodes.
    num_shards = 15 # More shards ensure better distribution across nodes
    chunk_size = max(1, table.num_rows // num_shards)
    
    for i in range(0, table.num_rows, chunk_size):
        chunk = table.slice(i, chunk_size)
        routing_key = f"shard-{i//chunk_size}".encode("utf-8")
        options = flight.FlightCallOptions(headers=[(b"x-longbow-key", routing_key)])
        
        writer, _ = client.do_put(descriptor, chunk.schema, options=options)
        writer.write_table(chunk)
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
                            filters: Optional[list] = None,
                            global_search: bool = False) -> BenchmarkResult:
    """Benchmark HNSW vector similarity search using DoAction(VectorSearch)."""
    num_queries = len(query_vectors)
    print(f"\n[SEARCH] Running {num_queries:,} vector searches (k={k})...")

    latencies = []
    errors = {
        'resource_exhausted': 0,
        'unavailable': 0,
        'other': 0
    }
    total_results = 0
    backpressure_warnings = []

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
        retry_count = 0
        max_retries = 2
        
        while retry_count <= max_retries:
            try:
                # Use DoGet with Search Ticket
                ticket_payload = {
                    "search": {
                        "dataset": name,
                        "vector": qvec.tolist(),
                        "k": k,
                        "filters": filters,
                        "local_only": False # Search benchmark tests distributed by default
                    }
                }
                ticket = flight.Ticket(json.dumps(ticket_payload).encode("utf-8"))
                
                options = flight.FlightCallOptions()
                if global_search:
                    headers = [(b"x-longbow-global", b"true")]
                    # Include routing key so load balancer can hit the owner node as coordinator
                    headers.append((b"x-longbow-key", name.encode("utf-8")))
                    options = flight.FlightCallOptions(headers=headers)
                    
                reader = client.do_get(ticket, options=options)
                table = reader.read_all()
                total_results += table.num_rows
                
                # Success - break retry loop
                break
                        
            except flight.FlightError as e:
                # Check for backpressure signals
                error_msg = str(e)
                
                if "RESOURCE_EXHAUSTED" in error_msg or "memory limit" in error_msg.lower():
                    errors['resource_exhausted'] += 1
                    if errors['resource_exhausted'] == 1:
                        print(f"\n⚠️  [BACKPRESSURE] Server memory limit hit on query {i}")
                        backpressure_warnings.append(f"Memory limit hit at query {i}")
                    time.sleep(0.1)  # Back off
                    retry_count += 1
                    if retry_count <= max_retries:
                        continue
                    
                elif "UNAVAILABLE" in error_msg or "evicting" in error_msg.lower():
                    errors['unavailable'] += 1
                    if errors['unavailable'] == 1:
                        print(f"\n⚠️  [EVICTION] Dataset evicting on query {i}, retrying...")
                        backpressure_warnings.append(f"Dataset eviction at query {i}")
                    time.sleep(0.05)  # Shorter backoff for eviction
                    retry_count += 1
                    if retry_count <= max_retries:
                        continue
                else:
                    errors['other'] += 1
                    if errors['other'] <= 3:
                        print(f"[SEARCH] Error on query {i}: {e}")
                
                break  # Exit retry loop on non-retryable error
                
            except Exception as e:
                errors['other'] += 1
                print(f"[SEARCH] Unexpected error on query {i}: {e}")
                break

        latencies.append((time.time() - start) * 1000)  # ms

        if (i + 1) % 100 == 0:
            print(f"[SEARCH] Completed {i + 1}/{num_queries} queries...")

    duration = sum(latencies) / 1000  # total seconds
    qps = num_queries / duration if duration > 0 else 0
    
    total_errors = sum(errors.values())

    result = BenchmarkResult(
        name="VectorSearch",
        duration_seconds=duration,
        throughput=qps,
        throughput_unit="queries/s",
        rows=total_results,
        latencies_ms=latencies,
        errors=total_errors,
    )

    print(f"[SEARCH] Completed: {qps:.2f} queries/s")
    print(f"[SEARCH] Latency p50={result.p50_ms:.2f}ms p95={result.p95_ms:.2f}ms p99={result.p99_ms:.2f}ms")
    print(f"[SEARCH] Errors: {total_errors}")
    
    # Report backpressure stats
    if errors['resource_exhausted'] > 0:
        print(f"\n⚠️  BACKPRESSURE DETECTED: {errors['resource_exhausted']} queries hit memory limit")
    if errors['unavailable'] > 0:
        print(f"\n⚠️  EVICTIONS DETECTED: {errors['unavailable']} queries hit evicting dataset")

    return result


def benchmark_search_by_id(client: flight.FlightClient, name: str,
                           ids: list, k: int) -> BenchmarkResult:
    """Benchmark VectorSearchByID operation."""
    num_queries = len(ids)
    print(f"\n[SEARCH-ID] Running {num_queries:,} ID searches (k={k})...")

    latencies = []
    errors = 0
    total_results = 0

    for i, query_id in enumerate(ids):
        # Payload: {"dataset": "name", "id": "id_val", "k": k}
        body = {
            "dataset": name,
            "id": str(query_id),
            "k": k,
        }
        request_body = json.dumps(body).encode("utf-8")

        start = time.time()
        try:
            action = flight.Action("VectorSearchByID", request_body)
            results_iter = client.do_action(action)
            
            for result in results_iter:
                payload = json.loads(result.body.to_pybytes())
                if "ids" in payload:
                    total_results += len(payload["ids"])

        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"[SEARCH-ID] Error on ID {query_id}: {e}")
            continue

        latencies.append((time.time() - start) * 1000)

        if (i + 1) % 100 == 0:
            print(f"[SEARCH-ID] Completed {i + 1}/{num_queries} queries...")

    duration = sum(latencies) / 1000
    qps = num_queries / duration if duration > 0 else 0

    result = BenchmarkResult(
        name="SearchByID",
        duration_seconds=duration,
        throughput=qps,
        throughput_unit="queries/s",
        rows=total_results,
        latencies_ms=latencies,
        errors=errors,
    )

    print(f"[SEARCH-ID] Completed: {qps:.2f} queries/s")
    print(f"[SEARCH-ID] Latency p50={result.p50_ms:.2f}ms p95={result.p95_ms:.2f}ms p99={result.p99_ms:.2f}ms")

    return result


def benchmark_hybrid_search(client: flight.FlightClient, name: str,
                            query_vectors: np.ndarray, k: int,
                            text_queries: list, alpha: float = 0.5,
                            global_search: bool = False) -> BenchmarkResult:
    """Benchmark hybrid search (dense vectors + sparse text) using DoAction(VectorSearch)."""
    num_queries = len(query_vectors)
    print(f"\n[HYBRID] Running {num_queries:,} hybrid searches (k={k})...")

    latencies = []
    errors = 0
    total_results = 0

    for i, (qvec, text_query) in enumerate(zip(query_vectors, text_queries)):
        ticket_payload = {
            "search": {
                "dataset": name,
                "vector": qvec.tolist(),
                "text_query": text_query,
                "k": k,
                "alpha": alpha
            }
        }
        ticket = flight.Ticket(json.dumps(ticket_payload).encode("utf-8"))

        start = time.time()
        try:
            headers = []
            if global_search:
                headers.append((b"x-longbow-global", b"true"))
                headers.append((b"x-longbow-key", name.encode("utf-8")))
            
            options = flight.FlightCallOptions(headers=headers)
            reader = client.do_get(ticket, options=options)
            table = reader.read_all()
            total_results += table.num_rows

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
            except Exception as e:
                local_errors += 1
                if local_errors == 1:
                    print(f"Worker {worker_id} Error: {e}")
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
# GraphRAG Benchmarks
# =============================================================================

def benchmark_graph_traversal(client: flight.FlightClient, name: str,
                              start_nodes: list, max_hops: int) -> BenchmarkResult:
    """Benchmark graph traversal using DoAction(traverse-graph)."""
    print(f"\n[GRAPH] Running {len(start_nodes):,} graph traversals (hops={max_hops})...")

    latencies = []
    errors = 0
    total_paths = 0

    for i, start_node in enumerate(start_nodes):
        req = {
            "dataset": name,
            "start": start_node,
            "max_hops": max_hops
        }
        request_body = json.dumps(req).encode("utf-8")

        start = time.time()
        try:
            action = flight.Action("traverse-graph", request_body)
            results = list(client.do_action(action))
            if results:
                paths = json.loads(results[0].body.to_pybytes())
                total_paths += len(paths)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"[GRAPH] Error on node {start_node}: {e}")
            continue

        latencies.append((time.time() - start) * 1000)

        if (i + 1) % 100 == 0:
            print(f"[GRAPH] Completed {i + 1}/{len(start_nodes)} traversals...")

    duration = sum(latencies) / 1000
    qps = len(start_nodes) / duration if duration > 0 else 0
    
    result = BenchmarkResult(
        name="GraphTraversal",
        duration_seconds=duration,
        throughput=qps,
        throughput_unit="ops/s",
        rows=total_paths,
        latencies_ms=latencies,
        errors=errors,
    )

    print(f"[GRAPH] Completed: {qps:.2f} ops/s")
    print(f"[GRAPH] Latency p50={result.p50_ms:.2f}ms p95={result.p95_ms:.2f}ms p99={result.p99_ms:.2f}ms")
    
    return result


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

    # Search By ID
    parser.add_argument("--search-id", action="store_true", help="Run Search By ID benchmark")

    # Vector delete
    parser.add_argument("--delete", action="store_true", help="Run vector delete benchmark")
    parser.add_argument("--delete-count", default=1000, type=int, help="Number of deletions")

    # Hybrid search
    parser.add_argument("--hybrid", action="store_true", help="Run hybrid search benchmark")
    parser.add_argument("--hybrid-alpha", default=0.5, type=float, help="Alpha for hybrid search (-1.0 for adaptive)")
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

    # GraphRAG
    parser.add_argument("--graph", action="store_true", help="Run graph traversal benchmark")
    parser.add_argument("--graph-hops", default=2, type=int, help="Max hops for traversal")
    parser.add_argument("--graph-nodes", default=100, type=int, help="Number of start nodes to query")

    # Output
    parser.add_argument("--json", help="Export results to JSON file")
    parser.add_argument("--all", action="store_true", help="Run all benchmarks")
    parser.add_argument("--check-cluster", action="store_true", help="Verify cluster health before start")

    # Filters (legacy support)
    parser.add_argument("--filter", action="append",
                        help="Filter in format field:op:value")
    # Global Distributed Search
    parser.add_argument("--global", dest="global_search", action="store_true", help="Force GLOBAL distributed search")

    # Data Plane operations
    parser.add_argument("--skip-data", action="store_true", help="Skip default Put/Get operations")

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

    if args.check_cluster:
        if not check_cluster_health(meta_client):
            print("Cluster check failed. Aborting.")
            sys.exit(1)

    # Use args.skip_data to control Put/Get
    if not args.skip_data:
        # Always run basic PUT/GET
        include_text = args.hybrid or args.all
        table = generate_vectors(args.rows, args.dim, with_text=include_text)

        # Data Plane operations
        results.append(benchmark_put(data_client, table, args.name))
        results.append(benchmark_get(data_client, args.name, filters=filters if filters else None))

        print("\n[INFO] Waiting 20s for async indexing...")
        time.sleep(20)

    # Meta Plane operations (Search)
    if args.search or args.all:
        query_vectors = generate_query_vectors(args.query_count, args.dim)
        # Search goes to Meta Server
        r_search = benchmark_vector_search(
            meta_client, args.name, query_vectors, args.search_k, filters=filters if filters else None, global_search=args.global_search
        )
        results.append(r_search)

    # Meta Plane operations (Search By ID)
    if args.search_id or args.all:
        ids_to_query = [random.randint(0, args.rows - 1) for _ in range(args.query_count)]
        results.append(benchmark_search_by_id(
            meta_client, args.name, ids_to_query, args.search_k
        ))

    # Meta Plane operations (Hybrid Search)
    if args.hybrid or args.all:
        query_vectors = generate_query_vectors(args.query_count, args.dim)
        text_queries = ["machine learning neural"] * args.query_count
        # Hybrid Search goes to Meta Server
        # Hybrid Search goes to Meta Server
        results.append(benchmark_hybrid_search(
            meta_client, args.name, query_vectors, args.search_k, text_queries, args.hybrid_alpha,
            global_search=args.global_search
        ))

    # Meta Plane operations (Graph Traversal)
    if args.graph or args.all:
        nodes_to_query = [random.randint(0, args.rows - 1) for _ in range(args.graph_nodes)]
        results.append(benchmark_graph_traversal(
            meta_client, args.name, nodes_to_query, args.graph_hops
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
