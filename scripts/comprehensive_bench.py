#!/usr/bin/env python3
"""
Comprehensive Performance Benchmark Suite
Tests DoPut, DoGet, and VectorSearch throughput across multiple scales and dimensions.
"""

import pyarrow.flight as flight
import pyarrow as pa
import numpy as np
import time
import json
from typing import Dict, List, Tuple
import sys

# Test configurations
SCALES = [10_000, 25_000, 50_000, 75_000, 100_000, 150_000, 250_000, 500_000, 750_000, 1_000_000]
DIMENSIONS = [128, 384, 768]
DATA_URI = "grpc://0.0.0.0:3000"
META_URI = "grpc://0.0.0.0:3001"
SEARCH_K = 10
SEARCH_ITERATIONS = 100  # Number of search queries to average

def create_dataset(name: str, rows: int, dims: int) -> pa.RecordBatch:
    """Generate random vector dataset."""
    vectors = np.random.rand(rows, dims).astype(np.float32)
    vector_list = pa.array([v.tolist() for v in vectors], type=pa.list_(pa.float32(), dims))
    
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("vector", pa.list_(pa.float32(), dims)),
        pa.field("timestamp", pa.timestamp('us'))
    ])
    
    ids = pa.array(range(rows), type=pa.int64())
    timestamps = pa.array([pa.scalar(int(time.time() * 1e6), type=pa.int64()).cast(pa.timestamp('us'))] * rows)
    
    return pa.RecordBatch.from_arrays([ids, vector_list, timestamps], schema=schema)

def measure_doput(client: flight.FlightClient, dataset_name: str, batch: pa.RecordBatch) -> Dict:
    """Measure DoPut throughput."""
    descriptor = flight.FlightDescriptor.for_path(dataset_name)
    
    start = time.time()
    writer, _ = client.do_put(descriptor, batch.schema)
    writer.write_batch(batch)
    writer.close()
    duration = time.time() - start
    
    rows = batch.num_rows
    bytes_written = batch.nbytes
    throughput_mb = (bytes_written / (1024 * 1024)) / duration
    throughput_rows = rows / duration
    
    return {
        "duration_s": round(duration, 3),
        "rows": rows,
        "bytes": bytes_written,
        "throughput_mb_s": round(throughput_mb, 2),
        "throughput_rows_s": round(throughput_rows, 2)
    }

def measure_doget(client: flight.FlightClient, dataset_name: str, expected_rows: int) -> Dict:
    """Measure DoGet throughput."""
    ticket = flight.Ticket(json.dumps({"dataset": dataset_name}).encode())
    
    start = time.time()
    reader = client.do_get(ticket)
    total_rows = 0
    total_bytes = 0
    
    for chunk in reader:
        total_rows += chunk.data.num_rows
        total_bytes += chunk.data.nbytes
    
    duration = time.time() - start
    
    throughput_mb = (total_bytes / (1024 * 1024)) / duration
    throughput_rows = total_rows / duration
    
    return {
        "duration_s": round(duration, 3),
        "rows": total_rows,
        "bytes": total_bytes,
        "throughput_mb_s": round(throughput_mb, 2),
        "throughput_rows_s": round(throughput_rows, 2),
        "verified": total_rows == expected_rows
    }

def measure_vector_search(meta_client: flight.FlightClient, dataset_name: str, dims: int, iterations: int) -> Dict:
    """Measure VectorSearch throughput."""
    query_vector = np.random.rand(dims).astype(np.float32).tolist()
    
    action_body = json.dumps({
        "dataset": dataset_name,
        "vector": query_vector,
        "k": SEARCH_K
    }).encode()
    
    action = flight.Action("VectorSearch", action_body)
    
    # Warmup
    list(meta_client.do_action(action))
    
    # Measure
    latencies = []
    for _ in range(iterations):
        start = time.time()
        list(meta_client.do_action(action))
        latencies.append(time.time() - start)
    
    latencies_ms = [l * 1000 for l in latencies]
    
    return {
        "iterations": iterations,
        "avg_latency_ms": round(np.mean(latencies_ms), 2),
        "p50_latency_ms": round(np.percentile(latencies_ms, 50), 2),
        "p95_latency_ms": round(np.percentile(latencies_ms, 95), 2),
        "p99_latency_ms": round(np.percentile(latencies_ms, 99), 2),
        "throughput_qps": round(1000 / np.mean(latencies_ms), 2)
    }

def run_benchmark(scale: int, dims: int) -> Dict:
    """Run complete benchmark for a given scale and dimension."""
    dataset_name = f"bench_{scale}_{dims}"
    
    print(f"\n{'='*80}")
    print(f"Benchmarking: {scale:,} vectors x {dims} dims")
    print(f"{'='*80}")
    
    # Connect
    data_client = flight.FlightClient(DATA_URI)
    meta_client = flight.FlightClient(META_URI)
    
    # Generate data
    print(f"Generating {scale:,} vectors...")
    batch = create_dataset(dataset_name, scale, dims)
    
    # DoPut
    print("Testing DoPut...")
    doput_results = measure_doput(data_client, dataset_name, batch)
    print(f"  Throughput: {doput_results['throughput_mb_s']} MB/s, {doput_results['throughput_rows_s']:,.0f} rows/s")
    
    # Wait for indexing
    print("Waiting for indexing (5s)...")
    time.sleep(5)
    
    # DoGet
    print("Testing DoGet...")
    doget_results = measure_doget(data_client, dataset_name, scale)
    print(f"  Throughput: {doget_results['throughput_mb_s']} MB/s, {doget_results['throughput_rows_s']:,.0f} rows/s")
    
    # VectorSearch
    print(f"Testing VectorSearch ({SEARCH_ITERATIONS} iterations)...")
    search_results = measure_vector_search(meta_client, dataset_name, dims, SEARCH_ITERATIONS)
    print(f"  Throughput: {search_results['throughput_qps']} QPS, P95: {search_results['p95_latency_ms']} ms")
    
    return {
        "scale": scale,
        "dims": dims,
        "dataset": dataset_name,
        "doput": doput_results,
        "doget": doget_results,
        "search": search_results
    }

def main():
    results = []
    
    print("="*80)
    print("COMPREHENSIVE PERFORMANCE BENCHMARK SUITE")
    print("="*80)
    print(f"Scales: {[f'{s:,}' for s in SCALES]}")
    print(f"Dimensions: {DIMENSIONS}")
    print(f"Search K: {SEARCH_K}")
    print(f"Search Iterations: {SEARCH_ITERATIONS}")
    
    for dims in DIMENSIONS:
        for scale in SCALES:
            try:
                result = run_benchmark(scale, dims)
                results.append(result)
            except Exception as e:
                print(f"ERROR: {scale:,} x {dims} dims - {e}")
                continue
    
    # Save results
    output_file = "benchmark_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n{'='*80}")
    print(f"Results saved to: {output_file}")
    print(f"{'='*80}")
    
    # Print summary
    print_summary(results)

def print_summary(results: List[Dict]):
    """Print formatted summary of results."""
    print("\n" + "="*80)
    print("BENCHMARK SUMMARY")
    print("="*80)
    
    for dims in DIMENSIONS:
        print(f"\n{dims} Dimensions:")
        print("-" * 80)
        print(f"{'Scale':<12} {'DoPut MB/s':<15} {'DoGet MB/s':<15} {'Search QPS':<15} {'P95 Latency':<15}")
        print("-" * 80)
        
        for r in results:
            if r['dims'] == dims:
                print(f"{r['scale']:>10,}  "
                      f"{r['doput']['throughput_mb_s']:>12.2f}  "
                      f"{r['doget']['throughput_mb_s']:>12.2f}  "
                      f"{r['search']['throughput_qps']:>12.2f}  "
                      f"{r['search']['p95_latency_ms']:>12.2f} ms")

if __name__ == "__main__":
    main()
