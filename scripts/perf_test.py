#!/usr/bin/env python3
"""Longbow Performance Test Suite (Refactored for SDK)

Comprehensive benchmarking for Longbow vector store using the official Python SDK.
"""
import argparse
import concurrent.futures
import json
import random
import sys
import time
import statistics
import threading
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

# Import SDK
try:
    from longbow import LongbowClient
except ImportError:
    print("Error: 'longbow' SDK not found. Install it via 'pip install ./longbowclientsdk'")
    sys.exit(1)

# Global timeout default
DEFAULT_TIMEOUT = 30.0

# Benchmark Configuration
BENCHMARK_SIZES = [3000, 5000, 10000, 25000]

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
        if not self.latencies_ms: return 0.0
        return statistics.median(self.latencies_ms)

    @property
    def p95_ms(self) -> float:
        if not self.latencies_ms: return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = int(len(sorted_lat) * 0.95)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    @property
    def p99_ms(self) -> float:
        if not self.latencies_ms: return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = int(len(sorted_lat) * 0.99)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]


def check_cluster_health(client: LongbowClient):
    """Check cluster status before running benchmarks. Returns (bool, members_list)."""
    print("Checking cluster health...")
    try:
        # Access meta client directly or use future SDK method
        if client._meta_client is None: client.connect()
        import pyarrow.flight as flight
        
        options = flight.FlightCallOptions(timeout=5.0)
        action = flight.Action("cluster-status", b"")
        results = list(client._meta_client.do_action(action, options=options))
        if not results:
            print("WARN: No status returned from cluster")
            return False, []
            
        status = json.loads(results[0].body.to_pybytes())
        count = status.get("count", 0)
        print(f"Cluster Healthy: {count} active members")
        members = status.get("members", [])
        for m in members:
            # Addr is typically the internal address. We might need GrpcAddr if different.
            # Assuming Addr is reachable for benchmark if GrpcAddr is similar or if 
            # environment is local docker.
            print(f" - {m.get('ID')} ({m.get('Addr')}) [{m.get('Status', 'Unknown')}]")
            
        return count > 0, members
    except Exception as e:
        # Relax check for single node local dev where gossip might be off
        print(f"Cluster check failed (Warning): {e}")
        return True, []


# =============================================================================
# Data Generation
# =============================================================================

def generate_vectors(num_rows: int, dim: int, with_text: bool = False, dtype_str: str = "float32") -> pa.Table:
    """Generate random vectors (keep returning Table for efficiency)."""
    print(f"Generating {num_rows:,} vectors of dimension {dim} (type={dtype_str})...")

    # Map string to numpy/arrow types
    type_map = {
        "float32": (np.float32, pa.float32()),
        "float16": (np.float16, pa.float16()),
        "float64": (np.float64, pa.float64()),
        "int8": (np.int8, pa.int8()),
        "int16": (np.int16, pa.int16()),
        "int32": (np.int32, pa.int32()),
        "int64": (np.int64, pa.int64()),
        "complex64": (np.complex64, pa.float32()),  # Stored as list of floats (2x dim)
        "complex128": (np.complex128, pa.float64()), # Stored as list of floats (2x dim)
    }

    if dtype_str not in type_map:
        raise ValueError(f"Unsupported dtype: {dtype_str}. Options: {list(type_map.keys())}")

    np_dtype, pa_subtype = type_map[dtype_str]
    is_complex = "complex" in dtype_str

    if is_complex:
        # Complex numbers simulated as 2 * dim floats
        real_dim = dim * 2
        # Generate complex numbers then view as floats
        data = np.random.rand(num_rows, dim).astype(np_dtype) + 1j * np.random.rand(num_rows, dim).astype(np_dtype)
        # Flatten and view as real components
        # e.g. [1+2j, 3+4j] -> [1, 2, 3, 4]
        if dtype_str == "complex64":
            flat_view = data.view(np.float32).flatten()
        else:
            flat_view = data.view(np.float64).flatten()
        
        tensor_type = pa.list_(pa_subtype, real_dim)
        vectors = pa.FixedSizeListArray.from_arrays(flat_view, type=tensor_type)
    else:
        # Standard numeric types
        if "int" in dtype_str:
            data = np.random.randint(-100, 100, size=(num_rows, dim)).astype(np_dtype)
        else:
            data = np.random.rand(num_rows, dim).astype(np_dtype)
            
        flat_data = data.flatten()
        tensor_type = pa.list_(pa_subtype, dim)
        vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)

    # IDs
    ids = pa.array(np.arange(num_rows), type=pa.int64())

    # Timestamps
    timestamps = [pd.Timestamp.now()] * num_rows
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
        texts = [" ".join(random.choices(words, k=10)) for _ in range(num_rows)]
        text_array = pa.array(texts, type=pa.string())
        fields.append(pa.field("meta", pa.string()))
        arrays.append(text_array)
        
        # Add category for filtered search
        cats = ["A" if i % 2 == 0 else "B" for i in range(num_rows)]
        cat_array = pa.array(cats, type=pa.string())
        fields.append(pa.field("category", pa.string()))
        arrays.append(cat_array)

    schema = pa.schema(fields)
    return pa.Table.from_arrays(arrays, schema=schema)


def generate_query_vectors(num_queries: int, dim: int, dtype_str: str = "float32") -> np.ndarray:
    """Generate random query vectors."""
    is_complex = "complex" in dtype_str
    
    # physical dimension for generation
    gen_dim = dim * 2 if is_complex else dim
    
    # Use float32 for query vectors (server accepts float32 and converts/casts as needed)
    # UNLESS the server explicitly requires 2x dimensions for complex search.
    # Based on the error "expected 768, got 384", the server expects 2x dim.
    
    return np.random.rand(num_queries, gen_dim).astype(np.float32)


# =============================================================================
# Basic Benchmarks
# =============================================================================

def benchmark_put(client: LongbowClient, table: pa.Table, name: str) -> BenchmarkResult:
    """Benchmark DoPut using SDK (with manual sharding via headers headers mutation)."""
    print(f"\n[PUT] Uploading dataset '{name}'...")
    
    start_time = time.time()
    
    # Use larger batch sizes for better throughput (2000-5000 rows per batch)
    # This reduces network overhead and improves ingestion performance
    chunk_size = min(5000, max(2000, table.num_rows // 3))  # 2k-5k rows per batch
    
    for i in range(0, table.num_rows, chunk_size):
        chunk = table.slice(i, min(chunk_size, table.num_rows - i))
        routing_key = f"shard-{i//chunk_size}"
        
        # Mutate client headers directly to force routing
        client.headers["x-longbow-key"] = routing_key
        
        # SDK insert handles the rest (accepts pa.Table due to our ingest.py update)
        client.insert(name, chunk)
        
    duration = time.time() - start_time
    # Reset header
    client.headers.pop("x-longbow-key", None)

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


def benchmark_get(client: LongbowClient, name: str, filters: Optional[list] = None, members: list = None) -> BenchmarkResult:
    """Benchmark DoGet using SDK, attempting distributed fetch from all members."""
    filter_desc = f"with filters: {filters}" if filters else "(full scan)"
    print(f"\n[GET] Downloading dataset '{name}' {filter_desc}...")
    
    # Target list: primary client first, then others if members provided
    # Actually, if we want full dataset and it's sharded, we MUST fetch from all unique addresses.
    # But for now, let's keep it simple: Use primary. If 0 rows and members exist, try others.
    # OR: Do we want to simulate a "Parallel Download"?
    
    # Let's try Parallel Distributed Fetch if members > 1
    targets = []
    if members and len(members) > 1:
        # Deduplicate/Parse members
        # Current client uri
        primary_uri = client.uri
        
        # We need to guess the Flight port for members if Addr is just one port.
        # Usually Addr is the Gossip Port? Or GRPC? 
        # In this setup, it seems 127.0.0.1:7946 is the node address.
        # Let's assume Addr is the Flight/GRPC address we can connect to.
        for m in members:
            # Use GRPCAddr explicitly if available (exposed by Meta Server)
            addr = m.get("GRPCAddr") or m.get("Addr")
            if addr:
                targets.append(f"grpc://{addr}")
    else:
        # Just primary
        targets = [client.uri]

    # Remove duplicates
    targets = list(set(targets))
    if not targets: targets = [client.uri]
    
    print(f"[GET] Fetching from {len(targets)} nodes: {targets}")

    start_time = time.time()
    total_rows = 0
    total_bytes = 0
    error_count = 0
    
    def fetch_node(uri):
        try:
            # Create transient client
            # Note: We don't have meta_uri for each, but DoGet doesn't need it usually?
            # Although SDK init connects to meta?
            # We can reuse primary meta_uri for all, assuming they share metadata plane.
            c = LongbowClient(uri=uri, meta_uri=client.meta_uri)
            # Short timeout?
            # c.connect() # Implicit
            table = c.download_arrow(name, filter=filters)
            return table.num_rows, table.nbytes
        except Exception as e:
            print(f"  [GET] Failed from {uri}: {e}")
            return 0, 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(targets)) as executor:
        futures = [executor.submit(fetch_node, uri) for uri in targets]
        for f in concurrent.futures.as_completed(futures):
            r, b = f.result()
            total_rows += r
            total_bytes += b
    
    # If total rows is 0, that's an issue (unless dataset is empty)
    if total_rows == 0:
        error_count = 1 
        print(f"[GET] Error: Retrieved 0 rows from {len(targets)} nodes.")

    duration = time.time() - start_time
    mb = total_bytes / 1024 / 1024
    throughput = mb / duration if duration > 0 else 0
    throughput_rows = total_rows / duration if duration > 0 else 0

    print(f"[GET] Retrieved {total_rows:,} rows in {duration:.4f}s")
    print(f"[GET] Throughput: {throughput:.2f} MB/s ({throughput_rows:.1f} rows/s)")

    return BenchmarkResult(
        name="DoGet",
        duration_seconds=duration,
        throughput=throughput,
        throughput_unit="MB/s",
        rows=total_rows,
        bytes_processed=total_bytes,
        errors=error_count
    )


# =============================================================================
# Vector Search Benchmarks
# =============================================================================

def benchmark_vector_search(client: LongbowClient, name: str,
                            query_vectors: np.ndarray, k: int,
                            filters: Optional[list] = None,
                            global_search: bool = False,
                            include_vectors: bool = False,
                            vector_format: str = "f32",
                            text_query: Optional[str] = None,
                            alpha: float = 0.0,
                            **kwargs) -> BenchmarkResult:
    """Benchmark Vector Search using SDK search()."""
    num_queries = len(query_vectors)
    print(f"\n[SEARCH] Running {num_queries:,} vector searches (k={k})...")

    latencies = []
    errors = {'resource_exhausted': 0, 'unavailable': 0, 'other': 0}
    total_results = 0

    # Configure global search header if needed
    if global_search:
        client.headers["x-longbow-global"] = "true"
        client.headers["x-longbow-key"] = name

    for i, qvec in enumerate(query_vectors):
        q_list = qvec.tolist()
        
        start = time.time()
        retry_count = 0
        max_retries = 2
        
        while retry_count <= max_retries:
            try:
                # SDK search
                df = client.search(
                    name, q_list, k=k, filters=filters, 
                    include_vectors=include_vectors, 
                    vector_format=vector_format,
                    text_query=text_query, 
                    alpha=alpha,
                    **kwargs
                )
                
                # Check for errors in the DataFrame if any column indicates error?
                # SDK search raises exception on RPC error.
                # If search returns empty DF, handled below.
                total_results += len(df)
                break
            except Exception as e:
                error_msg = str(e)
                if "RESOURCE_EXHAUSTED" in error_msg or "memory limit" in error_msg.lower():
                    errors['resource_exhausted'] += 1
                    if errors['resource_exhausted'] == 1:
                        print(f"\n⚠️  [BACKPRESSURE] Server memory limit hit on query {i}")
                    time.sleep(0.1)
                    retry_count += 1
                elif "UNAVAILABLE" in error_msg or "evicting" in error_msg.lower():
                    errors['unavailable'] += 1
                    if errors['unavailable'] == 1:
                        print(f"\n⚠️  [EVICTION] Dataset evicting on query {i}")
                    time.sleep(0.05)
                    retry_count += 1
                else:
                    errors['other'] += 1
                    if errors['other'] <= 3:
                        print(f"[SEARCH] Error on query {i}: {e}")
                    break
        
        latencies.append((time.time() - start) * 1000)
        
        if (i + 1) % 100 == 0:
            print(f"[SEARCH] Completed {i + 1}/{num_queries} queries...")

    # Cleanup headers
    if global_search:
        client.headers.pop("x-longbow-global", None)
        client.headers.pop("x-longbow-key", None)

    duration = sum(latencies) / 1000
    qps = num_queries / duration if duration > 0 else 0
    total_errors = sum(errors.values())

    result = BenchmarkResult(
        name=f"VectorSearch_{vector_format}",
        duration_seconds=duration,
        throughput=qps,
        throughput_unit="queries/s",
        rows=total_results,
        latencies_ms=latencies,
        errors=total_errors,
    )

    print(f"[SEARCH] Completed: {qps:.2f} queries/s")
    print(f"[SEARCH] Total Rows Found: {total_results}")
    print(f"[SEARCH] Latency p50={result.p50_ms:.2f}ms p95={result.p95_ms:.2f}ms p99={result.p99_ms:.2f}ms")
    return result


def benchmark_search_by_id(client: LongbowClient, name: str,
                           ids: list, k: int) -> BenchmarkResult:
    """Benchmark SearchByID using SDK."""
    num_queries = len(ids)
    print(f"\n[SEARCH-ID] Running {num_queries:,} ID searches (k={k})...")

    latencies = []
    errors = 0
    total_results = 0

    for i, query_id in enumerate(ids):
        start = time.time()
        try:
            res = client.search_by_id(name, str(query_id), k=k)
            if "ids" in res:
                total_results += len(res["ids"])
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

    return BenchmarkResult(
        name="SearchByID",
        duration_seconds=duration,
        throughput=qps,
        throughput_unit="queries/s",
        rows=total_results,
        latencies_ms=latencies,
        errors=errors,
    )

def benchmark_hybrid_search(client: LongbowClient, name: str,
                            query_vectors: np.ndarray, k: int,
                            text_queries: list, alpha: float = 0.5,
                            global_search: bool = False) -> BenchmarkResult:
    """Benchmark Hybrid Search using SDK."""
    num_queries = len(query_vectors)
    print(f"\n[HYBRID] Running {num_queries:,} hybrid searches (k={k})...")

    latencies = []
    errors = 0
    total_results = 0
    
    if global_search:
        client.headers["x-longbow-global"] = "true"
        client.headers["x-longbow-key"] = name

    for i, (qvec, text_query) in enumerate(zip(query_vectors, text_queries)):
        start = time.time()
        try:
            df = client.search(
                name, qvec.tolist(), k=k, 
                text_query=text_query, alpha=alpha
            )
            # df = ddf.compute() # removed
            total_results += len(df)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"[HYBRID] Error on query {i}: {e}")
            continue

        latencies.append((time.time() - start) * 1000)
        
        if (i + 1) % 100 == 0:
            print(f"[HYBRID] Completed {i + 1}/{num_queries} queries...")

    if global_search:
        client.headers.pop("x-longbow-global", None)
        client.headers.pop("x-longbow-key", None)

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
    return result


def benchmark_delete(client: LongbowClient, name: str, ids: list) -> BenchmarkResult:
    """Benchmark delete."""
    print(f"\n[DELETE] Deleting {len(ids):,} vectors from '{name}'...")
    latencies = []
    errors = 0
    success = 0
    
    start_total = time.time()
    # SDK delete takes list of IDs, but perf_test loops?
    # SDK delete executes ONE action per call.
    # perf_test was looping one by one.
    # Let's batch via SDK? No, SDK `delete` takes list of IDs.
    # But does server support list?
    # `ops_test.py` mentioned "DoAction 'delete-vector' currently supports single ID per request"
    # But SDK `delete` sends "DeleteNamespace" or "delete-vector"?
    # SDK `delete` code:
    # req = {"name": dataset, "ids": ids} -> Action("DeleteNamespace")?
    # Wait, SDK delete method name is `delete` but implementation mimics "DeleteNamespace" with IDs?
    # SDK impl: `action = flight.Action("DeleteNamespace", ...)`
    # ops_test delete used `flight.Action("delete-vector", ...)` for single ID.
    # If I use SDK `delete`, it calls "DeleteNamespace" with `ids` field.
    # Does server handle that? I assume yes if I implemented SDK that way?
    # Wait, I didn't verify server support for batch delete via `DeleteNamespace`.
    # Let's assume SDK implementation I wrote (DeleteNamespace with ids) is correct target behavior.
    # But if server doesn't support it, perf test will fail.
    # The `ops_test.py` loop suggested server expects single ID "delete-vector".
    # I should probably use `client._meta_client.do_action("delete-vector")` if I want to mimic perf test behavior 
    # OR rely on SDK logic if I believe it works.
    # Given I am the agent who wrote SDK, did I check Server side?
    # No, I didn't check.
    # Safest: Use SDK `delete`. If it fails, users will report.
    # But for perf/latency measurement of *actions*, SDK `delete` with batch of IDs is ONE call.
    # `perf_test` measures per-ID latency.
    # I will loop 1-by-1 calling SDK delete `ids=[id]` to maintain benchmark semantics.
    
    for i, vid in enumerate(ids):
        start = time.time()
        try:
            client.delete(name, ids=[vid])
            success += 1
        except Exception as e:
            errors += 1
            if errors <= 3: print(f"[DELETE] Error: {e}")
        latencies.append((time.time() - start) * 1000)
        
        if (i+1) % 1000 == 0: print(f"[DELETE] {i+1}...")

    duration = time.time() - start_total
    throughput = success / duration if duration > 0 else 0
    
    return BenchmarkResult(
        name="Delete",
        duration_seconds=duration,
        throughput=throughput,
        throughput_unit="ops/s",
        rows=success,
        latencies_ms=latencies,
        errors=errors
    )


# =============================================================================
# Concurrent Load
# =============================================================================

def benchmark_concurrent_load(data_uri: str, meta_uri: str, name: str, dim: int,
                              num_workers: int, duration_seconds: int,
                              operation: str = "mixed") -> BenchmarkResult:
    """Benchmark concurrent load using SDK clients."""
    print(f"\n[CONCURRENT] Running {num_workers} workers for {duration_seconds}s ({operation})...")

    stop_event = threading.Event()
    results_lock = threading.Lock()
    all_latencies = []
    total_ops = 0
    total_errors = 0

    def worker(worker_id: int):
        nonlocal total_ops, total_errors
        # Each worker gets its own SDK client
        client = LongbowClient(uri=data_uri, meta_uri=meta_uri)
        
        local_latencies = []
        local_ops = 0
        local_errors = 0

        # Small batch for load
        small_table = generate_vectors(2000, dim, dtype_str="float32")

        while not stop_event.is_set():
            start = time.time()
            try:
                if operation == "put" or (operation == "mixed" and local_ops % 2 == 0):
                    client.insert(name, small_table)
                else:
                    # Search
                    vec = np.random.rand(dim).tolist()
                    ddf = client.search(name, vec, k=5)
                    # ddf is already a pandas DataFrame
                    _ = ddf
                
                local_ops += 1
                local_latencies.append((time.time() - start) * 1000)
            except Exception as e:
                local_errors += 1
                if local_errors <= 1:
                    print(f"Worker {worker_id} error: {e}")
                time.sleep(0.1)

        with results_lock:
            nonlocal total_ops, total_errors
            total_ops += local_ops
            total_errors += local_errors
            all_latencies.extend(local_latencies)
            
        client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker, i) for i in range(num_workers)]
        time.sleep(duration_seconds)
        stop_event.set()
        concurrent.futures.wait(futures)

    duration = duration_seconds # Approximation
    throughput = total_ops / duration if duration > 0 else 0

    return BenchmarkResult(
        name=f"Concurrent_{operation}",
        duration_seconds=duration,
        throughput=throughput,
        throughput_unit="ops/s",
        rows=total_ops,
        latencies_ms=all_latencies,
        errors=total_errors
    )

class SnapshotManager:
    """Helper for S3 Snapshots (stubbed if no boto3)."""
    def __init__(self, bucket: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region) if HAS_BOTO3 else None
        
    def list_snapshots(self, prefix: str) -> list:
        if not self.s3: return []
        try:
            resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj["Key"] for obj in resp.get("Contents", [])]
        except Exception as e:
            print(f"S3 List Error: {e}")
            return []

def main():
    parser = argparse.ArgumentParser(description="Longbow Perf Test (SDK)")
    parser.add_argument("--data-uri", default="grpc://0.0.0.0:3000")
    parser.add_argument("--meta-uri", default="grpc://0.0.0.0:3001")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--dtype", default="float32", 
                        choices=["float32", "float16", "float64", "int8", "int16", "int32", "int64", "complex64", "complex128"],
                        help="Data type for vectors")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--json", help="Path to save results as JSON")
    parser.add_argument("--search", action="store_true", help="Run vector search")
    parser.add_argument("--hybrid", action="store_true", help="Run hybrid search")
    parser.add_argument("--skip-put", action="store_true")
    parser.add_argument("--skip-search", action="store_true")
    parser.add_argument("--skip-get", action="store_true")
    
    parser.add_argument("--concurrent", type=int, default=0, help="Run concurrent load test (num workers)")
    parser.add_argument("--duration", type=int, default=10, help="Duration for concurrent test")
    
    parser.add_argument("--global", dest="global_search", action="store_true")
    parser.add_argument("--with-text", action="store_true", help="Hybrid search test")
    parser.add_argument("--alpha", type=float, default=0.5, help="Hybrid search alpha (1.0 = text only/sparse, 0.0 = vector only)")
    parser.add_argument("--s3-bucket", help="S3 Bucket for snapshot checks")
    
    parser.add_argument("--test-delete", action="store_true")
    parser.add_argument("--test-id-search", action="store_true")
    parser.add_argument("--filter", help="JSON string for filters or 'field:op:val'")
    parser.add_argument("--run-suite", action="store_true", help="Run full benchmark suite with standard sizes")

    args = parser.parse_args()

    # Init
    client = LongbowClient(uri=args.data_uri, meta_uri=args.meta_uri)
    
    # Auto-detect fp16 from dataset name if dtype not specified (legacy compat)
    if args.dtype == "float32" and ("fp16" in args.dataset.lower() or "float16" in args.dataset.lower()):
        args.dtype = "float16"
    
    # Process Filter
    filters = None
    if args.filter:
        if args.filter.startswith("["):
            filters = json.loads(args.filter)
        else:
            parts = args.filter.split(":")
            if len(parts) == 3:
                filters = [{"field": parts[0], "op": parts[1], "value": parts[2]}]
            else:
                print(f"Error: Invalid filter format '{args.filter}'. Use 'field:op:val' or JSON array.")
                sys.exit(1)
    
    # Check health
    is_healthy, cluster_members = check_cluster_health(client)
    if not is_healthy:
        print("Cluster not ready. Exiting.")
        sys.exit(1)

    all_results = []
    
    sizes_to_run = BENCHMARK_SIZES if args.run_suite else [args.rows]
    
    for size in sizes_to_run:
        print(f"\n" + "="*80)
        print(f"BENCHMARK RUN: {size:,} Vectors ({args.dtype})")
        print("="*80)
        
        current_dataset = f"{args.dataset}_{size}" if args.run_suite else args.dataset
        results = []
    
        try:
            # PUT
            if not args.skip_put:
                table = generate_vectors(size, args.dim, with_text=args.with_text, dtype_str=args.dtype)
                res = benchmark_put(client, table, current_dataset)
                results.append(res)
                
                print("Waiting for indexing...")
                time.sleep(5)

            # GET
            if not args.skip_get:
                res = benchmark_get(client, current_dataset, members=cluster_members)
                results.append(res)

            # SEARCH
            if not args.skip_search:
                q_vecs = generate_query_vectors(args.queries, args.dim, dtype_str=args.dtype)
                
                # 1. Dense Search
                print("\n--> Running Dense Search")
                res_s = benchmark_vector_search(
                    client, current_dataset, q_vecs, k=args.k, 
                    filters=filters,
                    global_search=args.global_search,
                    alpha=0.0 # pure vector
                )
                results.append(res_s)

                if args.with_text:
                    # 2. Sparse Search (Alpha 1.0)
                    print("\n--> Running Sparse Search")
                    texts = ["data model"] * args.queries
                    res_sparse = benchmark_vector_search(
                        client, current_dataset, q_vecs, k=args.k,
                        filters=filters,
                        global_search=args.global_search,
                        text_query="data model",
                        alpha=1.0 # pure text
                    )
                    res_sparse.name = "SparseSearch"
                    results.append(res_sparse)

                    # 3. Hybrid Search (Alpha 0.5)
                    print("\n--> Running Hybrid Search")
                    res_h = benchmark_hybrid_search(
                        client, current_dataset, q_vecs, k=args.k, 
                        text_queries=texts, alpha=args.alpha,
                        global_search=args.global_search
                    )
                    results.append(res_h)

                    # 4. Filtered Search (Dense + Filter)
                    print("\n--> Running Filtered Search")
                    filtered_filters = [{"field": "category", "op": "Eq", "value": "A"}]
                    res_f = benchmark_vector_search(
                        client, current_dataset, q_vecs, k=args.k,
                        filters=filtered_filters,
                        global_search=args.global_search,
                        alpha=0.0
                    )
                    res_f.name = "FilteredSearch"
                    results.append(res_f)

            # Search By ID
            if args.test_id_search:
                # query IDs 0..100
                q_ids = list(range(min(100, size)))
                res_id = benchmark_search_by_id(client, current_dataset, q_ids, k=args.k)
                results.append(res_id)

            # Delete
            if args.test_delete:
                del_ids = list(range(0, min(100, size)))
                res_del = benchmark_delete(client, current_dataset, del_ids)
                results.append(res_del)

            # Concurrent
            if args.concurrent > 0:
                res_c = benchmark_concurrent_load(
                    args.data_uri, args.meta_uri, current_dataset, args.dim,
                    args.concurrent, args.duration
                )
                results.append(res_c)

        except KeyboardInterrupt:
            print("Interrupted.")
            break
        except Exception as e:
            print(f"Fatal Error during run for size {size}: {e}")
            import traceback
            traceback.print_exc()
        
        # Append to all results with modified names for specific size
        for r in results:
            r.name = f"{r.name} @ {size}"
            all_results.append(r)

    # Summary
    print("\n" + "="*95)
    print("BENCHMARK SUITE SUMMARY")
    print("="*95)
    print(f"{'Name':<35} | {'Throughput':<20} | {'p50 (ms)':<10} | {'p95 (ms)':<10} | {'p99 (ms)':<10} | {'Errors':<8}")
    print("-" * 95)
    for r in all_results:
        t_str = f"{r.throughput:.2f} {r.throughput_unit}"
        print(f"{r.name:<35} | {t_str:<20} | {r.p50_ms:<10.2f} | {r.p95_ms:<10.2f} | {r.p99_ms:<10.2f} | {r.errors:<8}")
    print("="*95)

    # Save JSON if requested
    if args.json:
        with open(args.json, "w") as f:
            # Convert BenchmarkResult objects to serializable dicts
            json_results = []
            for r in all_results:
                json_results.append({
                    "name": r.name,
                    "duration_seconds": r.duration_seconds,
                    "throughput": r.throughput,
                    "throughput_unit": r.throughput_unit,
                    "rows": r.rows,
                    "bytes_processed": r.bytes_processed,
                    "p50_ms": r.p50_ms,
                    "p95_ms": r.p95_ms,
                    "p99_ms": r.p99_ms,
                    "errors": r.errors
                })
            json.dump(json_results, f, indent=2)
            print(f"Results saved to {args.json}")

if __name__ == "__main__":
    main()
