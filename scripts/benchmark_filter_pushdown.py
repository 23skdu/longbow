#!/usr/bin/env python3
"""
Longbow Filter & Projection Pushdown Benchmark

Tests the performance impact of predicate/projection pushdown features.
Compares filtered search with pushdown vs post-filter.

Usage:
    python3 scripts/benchmark_filter_pushdown.py --vectors 10000 --dims 768
    python3 scripts/benchmark_filter_pushdown.py --test-projection
"""

import argparse
import random
import time
import numpy as np
from typing import List, Dict, Any

try:
    from longbow import LongbowClient

    HAS_LONGBOW_SDK = True
except ImportError:
    HAS_LONGBOW_SDK = False
    print("Warning: Longbow SDK not available")


def generate_test_vectors(num_vectors: int, dims: int) -> List[List[float]]:
    """Generate random test vectors."""
    return np.random.randn(num_vectors, dims).astype(np.float32).tolist()


def generate_metadata(num_vectors: int, num_categories: int = 10) -> List[Dict]:
    """Generate metadata with filterable fields."""
    metadata = []
    for i in range(num_vectors):
        metadata.append(
            {
                "id": f"vec_{i}",
                "category": f"cat_{i % num_categories}",
                "priority": i % 5,
                "score": random.random(),
                "active": i % 2 == 0,
            }
        )
    return metadata


def benchmark_filter_pushdown(
    client: LongbowClient,
    dataset: str,
    vectors: List[List[float]],
    metadata: List[Dict],
    filter_field: str = "category",
    filter_value: str = "cat_0",
    k: int = 10,
    num_runs: int = 10,
) -> Dict[str, float]:
    """Benchmark filter pushdown performance."""

    # Insert data
    print(f"Inserting {len(vectors)} vectors...")
    data = [{"vector": v, **m} for v, m in zip(vectors, metadata)]
    client.insert(dataset, data)

    # Warm up
    _ = client.search(dataset, vectors[0], k=k)

    # Benchmark without filter (baseline)
    print("Running baseline search (no filter)...")
    baseline_times = []
    for _ in range(num_runs):
        start = time.perf_counter()
        results = client.search(dataset, vectors[0], k=k)
        baseline_times.append(time.perf_counter() - start)

    baseline_avg = np.mean(baseline_times)
    print(f"  Baseline: {baseline_avg * 1000:.2f}ms")

    # Benchmark with filter
    print(f"Running filtered search ({filter_field}={filter_value})...")
    filtered_times = []
    for _ in range(num_runs):
        start = time.perf_counter()
        results = client.search(
            dataset,
            vectors[0],
            k=k,
            filters=[{"field": filter_field, "op": "eq", "value": filter_value}],
        )
        filtered_times.append(time.perf_counter() - start)

    filtered_avg = np.mean(filtered_times)
    print(f"  With filter: {filtered_avg * 1000:.2f}ms")

    # Calculate improvement
    speedup = baseline_avg / filtered_avg if filtered_avg > 0 else 1.0

    return {
        "baseline_ms": baseline_avg * 1000,
        "filtered_ms": filtered_avg * 1000,
        "speedup": speedup,
        "num_results_filtered": len(results),
        "num_results_baseline": k,
    }


def benchmark_projection(
    client: LongbowClient,
    dataset: str,
    vectors: List[List[float]],
    metadata: List[Dict],
    k: int = 10,
    num_runs: int = 10,
) -> Dict[str, float]:
    """Benchmark projection pushdown performance."""

    # Insert data (includes metadata)
    print(f"Inserting {len(vectors)} vectors...")
    data = [{"vector": v, **m} for v, m in zip(vectors, metadata)]
    client.insert(dataset, data)

    # Warm up
    _ = client.search(dataset, vectors[0], k=k)

    # Benchmark without projection
    print("Running search without projection...")
    no_proj_times = []
    for _ in range(num_runs):
        start = time.perf_counter()
        results = client.search(dataset, vectors[0], k=k)
        no_proj_times.append(time.perf_counter() - start)

    no_proj_avg = np.mean(no_proj_times)
    print(f"  No projection: {no_proj_avg * 1000:.2f}ms")

    # Benchmark with projection (only id and score)
    print("Running search with projection...")
    proj_times = []
    for _ in range(num_runs):
        start = time.perf_counter()
        results = client.search(dataset, vectors[0], k=k, projection=["id", "score"])
        proj_times.append(time.perf_counter() - start)

    proj_avg = np.mean(proj_times)
    print(f"  With projection: {proj_avg * 1000:.2f}ms")

    # Calculate improvement
    speedup = no_proj_avg / proj_avg if proj_avg > 0 else 1.0
    bandwidth_reduction = 1.0 - (proj_avg / no_proj_avg) if no_proj_avg > 0 else 0.0

    return {
        "no_projection_ms": no_proj_avg * 1000,
        "projection_ms": proj_avg * 1000,
        "speedup": speedup,
        "bandwidth_reduction": bandwidth_reduction,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Longbow Filter & Projection Pushdown Benchmark"
    )
    parser.add_argument("--host", default="localhost:3000", help="Longbow server host")
    parser.add_argument("--dataset", default="pushdown_bench", help="Dataset name")
    parser.add_argument("--vectors", type=int, default=10000, help="Number of vectors")
    parser.add_argument("--dims", type=int, default=768, help="Vector dimensions")
    parser.add_argument("--k", type=int, default=10, help="Number of results to return")
    parser.add_argument("--runs", type=int, default=10, help="Number of benchmark runs")
    parser.add_argument(
        "--test-projection", action="store_true", help="Test projection pushdown"
    )
    parser.add_argument(
        "--test-filter", action="store_true", help="Test filter pushdown"
    )
    parser.add_argument("--test-all", action="store_true", help="Test both features")

    args = parser.parse_args()

    if not HAS_LONGBOW_SDK:
        print("Error: Longbow SDK not available. Install with:")
        print("  pip install longbow")
        return 1

    # Generate test data
    print(f"Generating {args.vectors} vectors with {args.dims} dimensions...")
    vectors = generate_test_vectors(args.vectors, args.dims)
    metadata = generate_metadata(args.vectors, num_categories=10)

    # Connect to server
    client = LongbowClient(f"grpc://{args.host}")
    client.connect()

    results = {}

    try:
        if args.test_all or args.test_filter:
            print("\n=== Filter Pushdown Benchmark ===")
            results["filter"] = benchmark_filter_pushdown(
                client, args.dataset, vectors, metadata, k=args.k, num_runs=args.runs
            )

        if args.test_all or args.test_projection:
            print("\n=== Projection Pushdown Benchmark ===")
            # Add more metadata for projection test
            rich_metadata = []
            for i, m in enumerate(metadata):
                m["extra_field_1"] = f"data_{i}"
                m["extra_field_2"] = f"value_{i}"
                m["extra_field_3"] = random.random()
                rich_metadata.append(m)

            results["projection"] = benchmark_projection(
                client,
                args.dataset + "_proj",
                vectors,
                rich_metadata,
                k=args.k,
                num_runs=args.runs,
            )

        # Print summary
        print("\n=== Summary ===")
        for name, result in results.items():
            print(f"\n{name.upper()}:")
            for k, v in result.items():
                print(f"  {k}: {v}")

    finally:
        client.close()

    return 0


if __name__ == "__main__":
    exit(main())
