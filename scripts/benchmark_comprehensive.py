#!/usr/bin/env python3
"""
Comprehensive 3-Node Performance Test
Tests: DoGet, DoPut, DoExchange, Dense/Sparse/Filtered/Hybrid Search
Vector sizes: 3k, 5k, 7k, 9k, 15k, 20k (dim 384)
Metrics: Throughput, P50/P95/P99 latency, tombstone deletion effects
"""
import time
import argparse
import sys
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import requests
import json
import os
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# Config
DATASET = "perf_test_v4"
DIM = 384
SIZES = [3000, 7000, 15000, 25000, 50000]
PROFILES_DIR = "profiles_comprehensive"
DTYPE = "float32"

TYPE_MAP = {
    "float32": (np.float32, pa.float32(), 1),
    "float64": (np.float64, pa.float64(), 1),
    "int8": (np.int8, pa.int8(), 1),
    "complex64": (np.float32, pa.float32(), 2),
    "complex128": (np.float64, pa.float64(), 2),
}

class BenchmarkResults:
    def __init__(self):
        self.results = defaultdict(dict)
    
    def add(self, phase, metric, value):
        self.results[phase][metric] = value
    
    def get_markdown(self):
        lines = []
        for phase in sorted(self.results.keys()):
            lines.append(f"\n### {phase}")
            for metric, value in sorted(self.results[phase].items()):
                lines.append(f"- **{metric}**: {value}")
        return "\n".join(lines)

results = BenchmarkResults()

def get_client(uri):
    return flight.FlightClient(uri)

def generate_batch(start_id, count, dim, include_metadata=False):
    """Generate Arrow batch with optional metadata for filtered/hybrid search"""
    np_type, pa_type, dim_factor = TYPE_MAP[DTYPE]
    effective_dim = dim * dim_factor
    
    ids = pa.array([str(i) for i in range(start_id, start_id + count)], type=pa.string())
    data = np.random.rand(count, effective_dim).astype(np_type)
    tensor_type = pa.list_(pa_type, effective_dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)
    ts = pa.array([time.time_ns()] * count, type=pa.timestamp("ns"))
    
    fields = [
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
    ]
    arrays = [ids, vectors, ts]
    
    if include_metadata:
        # Add category for filtered search
        categories = pa.array([f"cat_{i % 10}" for i in range(count)], type=pa.string())
        # Add text for hybrid search
        texts = pa.array([f"document {i} about topic {i % 5}" for i in range(count)], type=pa.string())
        fields.extend([pa.field("category", pa.string()), pa.field("text", pa.string())])
        arrays.extend([categories, texts])
    
    schema = pa.schema(fields)
    return pa.Table.from_arrays(arrays, schema=schema)

def benchmark_do_put(clients, start_id, count, batch_size=1000):
    """Benchmark DoPut throughput"""
    print(f"  DoPut: Ingesting {count} vectors...")
    total = 0
    start = time.time()
    
    for i in range(0, count, batch_size):
        end = min(i + batch_size, count)
        batch = generate_batch(start_id + i, end - i, DIM, include_metadata=True)
        client = clients[i % len(clients)]
        try:
            descriptor = flight.FlightDescriptor.for_path(DATASET)
            writer, _ = client.do_put(descriptor, batch.schema)
            writer.write_table(batch)
            writer.close()
            total += (end - i)
        except Exception as e:
            print(f"    DoPut error: {e}")
    
    duration = time.time() - start
    throughput = total / duration if duration > 0 else 0
    
    # Calculate bytes per vector
    np_type, _, dim_factor = TYPE_MAP[DTYPE]
    bytes_per_elem = np.dtype(np_type).itemsize
    vec_size = DIM * dim_factor * bytes_per_elem
    
    bandwidth_mb = (throughput * vec_size) / (1024 * 1024)
    bandwidth_mbits = bandwidth_mb * 8
    print(f"    DoPut: {total} vectors in {duration:.2f}s ({throughput:.0f} vectors/s, {bandwidth_mb:.2f} MB/s, {bandwidth_mbits:.2f} Mb/s)")
    return throughput, bandwidth_mb, bandwidth_mbits, duration

def benchmark_do_get(clients, num_queries=100):
    """Benchmark DoGet throughput"""
    print(f"  DoGet: Retrieving {num_queries} batches...")
    start = time.time()
    total_records = 0
    errors = 0
    
    for i in range(num_queries):
        client = clients[i % len(clients)]
        try:
            ticket = flight.Ticket(json.dumps({"dataset": DATASET, "limit": 10}).encode())
            reader = client.do_get(ticket)
            for chunk in reader:
                total_records += chunk.data.num_rows
        except Exception as e:
            errors += 1
    
    duration = time.time() - start
    throughput = total_records / duration if duration > 0 else 0
    
    np_type, _, dim_factor = TYPE_MAP[DTYPE]
    bytes_per_elem = np.dtype(np_type).itemsize
    vec_size = DIM * dim_factor * bytes_per_elem
    
    bandwidth_gb = (throughput * vec_size) / (1024 * 1024 * 1024)
    bandwidth_mbits = (bandwidth_gb * 1024 * 8)
    print(f"    DoGet: {total_records} records in {duration:.2f}s ({throughput:.0f} records/s, {bandwidth_gb:.2f} GB/s, {bandwidth_mbits:.2f} Mb/s), Errors: {errors}")
    return throughput, bandwidth_gb, bandwidth_mbits, duration, errors

def benchmark_do_exchange(clients, num_queries=500):
    """Benchmark DoExchange (binary search protocol)"""
    print(f"  DoExchange: Running {num_queries} binary searches...")
    latencies = []
    errors = 0
    
    # Pre-build schema
    # Pre-build schema
    np_type, pa_type, dim_factor = TYPE_MAP[DTYPE]
    effective_dim = DIM * dim_factor
    tensor_type = pa.list_(pa_type, effective_dim)
    query_schema = pa.schema([
        pa.field("query_vector", tensor_type),
        pa.field("k", pa.int32()),
        pa.field("dataset", pa.string()),
    ])
    
    for i in range(num_queries):
        client = clients[i % len(clients)]
        try:
            # Create query batch
            vec_data = np.random.rand(1, effective_dim).astype(np_type).flatten()
            vectors = pa.FixedSizeListArray.from_arrays(vec_data, type=tensor_type)
            
            table = pa.Table.from_arrays([
                vectors,
                pa.array([10], type=pa.int32()),
                pa.array([DATASET], type=pa.string())
            ], schema=query_schema)
            
            t0 = time.time()
            descriptor = flight.FlightDescriptor.for_command(b"search")
            writer, reader = client.do_exchange(descriptor)
            
            writer.begin(query_schema)
            writer.write_table(table)
            writer.done_writing()
            
            # Read results
            for chunk in reader:
                pass
                
            latencies.append((time.time() - t0) * 1000)
        except Exception as e:
            errors += 1
            if errors <= 1:
                print(f"    DoExchange error: {e}")
    
    if latencies:
        p50 = np.percentile(latencies, 50)
        p95 = np.percentile(latencies, 95)
        p99 = np.percentile(latencies, 99)
        print(f"    DoExchange: P50={p50:.2f}ms, P95={p95:.2f}ms, P99={p99:.2f}ms, Errors={errors}")
        return p50, p95, p99, errors
    return 0, 0, 0, errors

def benchmark_dense_search(clients, k=10, num_queries=1000, concurrency=4):
    """Benchmark dense vector search"""
    print(f"  Dense Search: {num_queries} queries (c={concurrency})...")
    latencies = []
    errors = 0
    
    def run_query():
        nonlocal errors
        try:
            np_type, _, dim_factor = TYPE_MAP[DTYPE]
            vec = np.random.rand(DIM * dim_factor).astype(np_type).tolist()
            req = json.dumps({
                "dataset": DATASET,
                "vector": vec,
                "k": k
            }).encode("utf-8")
            
            client = clients[np.random.randint(len(clients))]
            t0 = time.time()
            action = flight.Action("VectorSearch", req)
            list(client.do_action(action))
            latencies.append((time.time() - t0) * 1000)
        except Exception as e:
            errors += 1
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(run_query) for _ in range(num_queries)]
        for f in futures:
            f.result()
    
    if latencies:
        p50 = np.percentile(latencies, 50)
        p95 = np.percentile(latencies, 95)
        p99 = np.percentile(latencies, 99)
        print(f"    Dense: P50={p50:.2f}ms, P95={p95:.2f}ms, P99={p99:.2f}ms, Errors={errors}")
        return p50, p95, p99, errors
    return 0, 0, 0, errors

def benchmark_sparse_search(clients, k=10, num_queries=500):
    """Benchmark sparse search (using filters to reduce candidate set)"""
    print(f"  Sparse Search: {num_queries} queries...")
    latencies = []
    errors = 0
    
    for i in range(num_queries):
        try:
            np_type, _, dim_factor = TYPE_MAP[DTYPE]
            vec = np.random.rand(DIM * dim_factor).astype(np_type).tolist()
            # Sparse: filter to specific category (reduces search space)
            req = json.dumps({
                "dataset": DATASET,
                "vector": vec,
                "k": k,
                "filters": [{"field": "category", "operator": "==", "value": f"cat_{i % 10}"}]
            }).encode("utf-8")
            
            client = clients[i % len(clients)]
            t0 = time.time()
            action = flight.Action("VectorSearch", req)
            list(client.do_action(action))
            latencies.append((time.time() - t0) * 1000)
        except Exception as e:
            errors += 1
    
    if latencies:
        p50 = np.percentile(latencies, 50)
        p95 = np.percentile(latencies, 95)
        p99 = np.percentile(latencies, 99)
        print(f"    Sparse: P50={p50:.2f}ms, P95={p95:.2f}ms, P99={p99:.2f}ms, Errors={errors}")
        return p50, p95, p99, errors
    return 0, 0, 0, errors

def benchmark_filtered_search(clients, k=10, num_queries=500):
    """Benchmark filtered search"""
    print(f"  Filtered Search: {num_queries} queries...")
    latencies = []
    errors = 0
    
    for i in range(num_queries):
        try:
            np_type, _, dim_factor = TYPE_MAP[DTYPE]
            vec = np.random.rand(DIM * dim_factor).astype(np_type).tolist()
            req = json.dumps({
                "dataset": DATASET,
                "vector": vec,
                "k": k,
                "filters": [{"field": "category", "operator": "==", "value": f"cat_{i % 3}"}]  # Filter to 30% of data
            }).encode("utf-8")
            
            client = clients[i % len(clients)]
            t0 = time.time()
            action = flight.Action("VectorSearch", req)
            list(client.do_action(action))
            latencies.append((time.time() - t0) * 1000)
        except Exception as e:
            errors += 1
    
    if latencies:
        p50 = np.percentile(latencies, 50)
        p95 = np.percentile(latencies, 95)
        p99 = np.percentile(latencies, 99)
        print(f"    Filtered: P50={p50:.2f}ms, P95={p95:.2f}ms, P99={p99:.2f}ms, Errors={errors}")
        return p50, p95, p99, errors
    return 0, 0, 0, errors

def benchmark_hybrid_search(clients, k=10, num_queries=500):
    """Benchmark hybrid search (vector + BM25)"""
    print(f"  Hybrid Search: {num_queries} queries...")
    latencies = []
    errors = 0
    
    for i in range(num_queries):
        try:
            np_type, _, dim_factor = TYPE_MAP[DTYPE]
            vec = np.random.rand(DIM * dim_factor).astype(np_type).tolist()
            req = json.dumps({
                "dataset": DATASET,
                "vector": vec,
                "k": k,
                "text_query": f"topic {i % 5}",  # BM25 component
                "alpha": 0.5  # 50% vector, 50% BM25
            }).encode("utf-8")
            
            client = clients[i % len(clients)]
            t0 = time.time()
            action = flight.Action("HybridSearch", req)
            list(client.do_action(action))
            latencies.append((time.time() - t0) * 1000)
        except Exception as e:
            errors += 1
    
    if latencies:
        p50 = np.percentile(latencies, 50)
        p95 = np.percentile(latencies, 95)
        p99 = np.percentile(latencies, 99)
        print(f"    Hybrid: P50={p50:.2f}ms, P95={p95:.2f}ms, P99={p99:.2f}ms, Errors={errors}")
        return p50, p95, p99, errors
    return 0, 0, 0, errors

def benchmark_tombstone_deletion(clients, ids_to_delete):
    """Benchmark tombstone deletion and verify query results"""
    print(f"  Tombstone Deletion: Deleting {len(ids_to_delete)} IDs...")
    start = time.time()
    errors = 0
    
    def del_one(id_val):
        nonlocal errors
        try:
            req = json.dumps({"dataset": DATASET, "id": str(id_val)}).encode("utf-8")
            action = flight.Action("Delete", req)
            # Broadcast to all nodes - data is sharded
            deleted = False
            last_err = None
            for client in clients:
                try:
                    list(client.do_action(action))
                    deleted = True
                    break
                except Exception as e:
                    last_err = e
            
            if not deleted:
                raise last_err or Exception("Unknown error")
        except Exception as e:
            if errors < 5:
                print(f"    Delete error: {e}")
            errors += 1
    
    with ThreadPoolExecutor(max_workers=10) as ex:
        ex.map(del_one, ids_to_delete)
    
    duration = time.time() - start
    throughput = len(ids_to_delete) / duration if duration > 0 else 0
    print(f"    Deletion: {len(ids_to_delete)} IDs in {duration:.2f}s ({throughput:.0f} deletes/s), Errors={errors}")
    
    # Verify search after deletion
    time.sleep(1)  # Allow propagation
    print(f"  Verifying search after deletion...")
    p50, p95, p99, search_errors = benchmark_dense_search(clients, num_queries=100, concurrency=2)
    
    return throughput, duration, errors, p50, p95, p99, search_errors

def collect_pprof(urls, label=""):
    """Collect pprof data from all nodes"""
    print(f"  Collecting pprof ({label})...")
    if not os.path.exists(PROFILES_DIR):
        os.makedirs(PROFILES_DIR)
    
    for i, url in enumerate(urls):
        try:
            # Heap
            res = requests.get(f"{url}/debug/pprof/heap", timeout=10)
            with open(f"{PROFILES_DIR}/heap_{label}_node{i}.pprof", "wb") as f:
                f.write(res.content)
            # CPU Profile
            res = requests.get(f"{url}/debug/pprof/profile?seconds=5", timeout=15)
            with open(f"{PROFILES_DIR}/cpu_{label}_node{i}.pprof", "wb") as f:
                f.write(res.content)
            print(f"    Node {i}: ✓")
        except Exception as e:
            print(f"    Node {i}: Failed - {e}")

def wait_for_readiness(clients, timeout=300):
    """Wait for all nodes to be ready (indexing complete)"""
    print("  Waiting for cluster readiness...")
    start = time.time()
    last_print = 0
    
    while time.time() - start < timeout:
        all_ready = True
        pending_counts = []
        
        for i, client in enumerate(clients):
            try:
                action = flight.Action("check_readiness", json.dumps({"dataset": DATASET}).encode("utf-8"))
                results = list(client.do_action(action))
                for res in results:
                    body = res.body.to_pybytes().decode('utf-8')
                    status = json.loads(body)
                    if status.get("status") != "READY":
                        all_ready = False
                        if "reason" in status:
                             pending_counts.append(status["reason"])
            except Exception as e:
                # If node is transiently unavailable, keep waiting
                all_ready = False
        
        if all_ready:
            print(f"    Cluster READY in {time.time() - start:.1f}s")
            return
            
        if time.time() - last_print > 5:
            print(f"    Waiting... ({', '.join(pending_counts)})")
            last_print = time.time()
            
        time.sleep(1)
    print("    WARNING: Timeout waiting for readiness")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uris", default="grpc://localhost:3000,grpc://localhost:3010,grpc://localhost:3020")
    parser.add_argument("--pprof", default="http://localhost:9090,http://localhost:9091,http://localhost:9092")
    parser.add_argument("--dim", type=int, default=384, help="Vector dimension")
    parser.add_argument("--type", type=str, default="float32", choices=TYPE_MAP.keys(), help="Vector data type")
    args = parser.parse_args()
    
    global DIM, DTYPE
    DIM = args.dim
    DTYPE = args.type
    
    clients = [get_client(u) for u in args.uris.split(",")]
    pprof_urls = args.pprof.split(",")
    
    current_count = 0
    
    print("=" * 80)
    print("COMPREHENSIVE 3-NODE PERFORMANCE TEST")
    print("=" * 80)
    print(f"Nodes: {len(clients)}")
    print(f"Vector sizes: {SIZES}")
    print(f"Dimensions: {DIM}")
    print("=" * 80)
    
    for target in SIZES:
        needed = target - current_count
        phase = f"{target // 1000}k_vectors"
        print(f"\n{'=' * 80}")
        print(f"PHASE: {target:,} vectors (adding {needed:,})")
        print(f"{'=' * 80}")
        
        # 1. DoPut
        if needed > 0:
            throughput, bandwidth, bandwidth_mbits, duration = benchmark_do_put(clients, current_count, needed)
            results.add(phase, "DoPut Throughput (vectors/s)", f"{throughput:.0f}")
            results.add(phase, "DoPut Bandwidth (MB/s)", f"{bandwidth:.2f}")
            results.add(phase, "DoPut Bandwidth (Mb/s)", f"{bandwidth_mbits:.2f}")
            results.add(phase, "DoPut Duration (s)", f"{duration:.2f}")
            current_count = target
        
        time.sleep(2)  # Settling time
        
        # 2. DoGet
        get_throughput, get_bandwidth, get_mbits, get_duration, get_errors = benchmark_do_get(clients)
        results.add(phase, "DoGet Throughput (records/s)", f"{get_throughput:.0f}")
        results.add(phase, "DoGet Bandwidth (GB/s)", f"{get_bandwidth:.2f}")
        results.add(phase, "DoGet Bandwidth (Mb/s)", f"{get_mbits:.2f}")
        results.add(phase, "DoGet Errors", str(get_errors))
        
        # 3. DoExchange
        ex_p50, ex_p95, ex_p99, ex_errors = benchmark_do_exchange(clients)
        results.add(phase, "DoExchange P50 (ms)", f"{ex_p50:.2f}")
        results.add(phase, "DoExchange P95 (ms)", f"{ex_p95:.2f}")
        results.add(phase, "DoExchange P99 (ms)", f"{ex_p99:.2f}")
        
        # 4. Dense Search
        dense_p50, dense_p95, dense_p99, dense_errors = benchmark_dense_search(clients)
        results.add(phase, "Dense Search P50 (ms)", f"{dense_p50:.2f}")
        results.add(phase, "Dense Search P95 (ms)", f"{dense_p95:.2f}")
        results.add(phase, "Dense Search P99 (ms)", f"{dense_p99:.2f}")
        results.add(phase, "Dense Search Errors", str(dense_errors))
        
        # 5. Sparse Search
        sparse_p50, sparse_p95, sparse_p99, sparse_errors = benchmark_sparse_search(clients)
        results.add(phase, "Sparse Search P50 (ms)", f"{sparse_p50:.2f}")
        results.add(phase, "Sparse Search P95 (ms)", f"{sparse_p95:.2f}")
        results.add(phase, "Sparse Search P99 (ms)", f"{sparse_p99:.2f}")
        
        # 6. Filtered Search
        filt_p50, filt_p95, filt_p99, filt_errors = benchmark_filtered_search(clients)
        results.add(phase, "Filtered Search P50 (ms)", f"{filt_p50:.2f}")
        results.add(phase, "Filtered Search P95 (ms)", f"{filt_p95:.2f}")
        results.add(phase, "Filtered Search P99 (ms)", f"{filt_p99:.2f}")
        
        # 7. Hybrid Search
        hybrid_p50, hybrid_p95, hybrid_p99, hybrid_errors = benchmark_hybrid_search(clients)
        results.add(phase, "Hybrid Search P50 (ms)", f"{hybrid_p50:.2f}")
        results.add(phase, "Hybrid Search P95 (ms)", f"{hybrid_p95:.2f}")
        results.add(phase, "Hybrid Search P99 (ms)", f"{hybrid_p99:.2f}")
        
        # 8. Collect pprof
        collect_pprof(pprof_urls, label=phase)
    
    # 9. Tombstone Deletion Test
    print(f"\n{'=' * 80}")
    print("PHASE: Tombstone Deletion")
    print(f"{'=' * 80}")
    
    # Wait for ingestion to complete
    wait_for_readiness(clients)
    
    ids_to_del = list(range(current_count - 1000, current_count))
    del_throughput, del_duration, del_errors, post_p50, post_p95, post_p99, post_errors = \
        benchmark_tombstone_deletion(clients, ids_to_del)
    
    results.add("tombstone_deletion", "Delete Throughput (deletes/s)", f"{del_throughput:.0f}")
    results.add("tombstone_deletion", "Delete Duration (s)", f"{del_duration:.2f}")
    results.add("tombstone_deletion", "Delete Errors", str(del_errors))
    results.add("tombstone_deletion", "Post-Delete Search P50 (ms)", f"{post_p50:.2f}")
    results.add("tombstone_deletion", "Post-Delete Search P95 (ms)", f"{post_p95:.2f}")
    results.add("tombstone_deletion", "Post-Delete Search P99 (ms)", f"{post_p99:.2f}")
    results.add("tombstone_deletion", "Post-Delete Search Errors", str(post_errors))
    
    # 10. Final pprof
    collect_pprof(pprof_urls, label="final")
    
    print(f"\n{'=' * 80}")
    print("BENCHMARK COMPLETE")
    print(f"{'=' * 80}")
    print("\nResults Summary:")
    print(results.get_markdown())
    
    # Save results to file
    with open("benchmark_results.json", "w") as f:
        json.dump(dict(results.results), f, indent=2)
    print(f"\nResults saved to benchmark_results.json")
    print(f"Profiles saved to {PROFILES_DIR}/")

if __name__ == "__main__":
    main()
