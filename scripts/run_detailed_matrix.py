#!/usr/bin/env python3
"""
Detailed Performance Matrix Runner
Run this to generate the matrix for docs/performance.md
"""
import argparse
import json
import os
import time
import numpy as np
import pandas as pd
from typing import List, Dict, Any

# Ensure we can import perf_test
import sys
sys.path.append(os.path.dirname(__file__))
import perf_test
from longbow import LongbowClient

# Configuration matching user request
COUNTS = [3000, 5000, 10000, 15000, 25000, 50000]
DIMS = [384]
TYPES_FULL = [
    "float32", "float16", "int8", "uint8", "int32"
]

class MatrixRunner:
    def __init__(self, data_uri, meta_uri):
        self.client = LongbowClient(uri=data_uri, meta_uri=meta_uri)
        self.results = []

    def run_configuration(self, count, dim, dtype):
        ds_name = f"perf_matrix_{dtype}_{dim}_{count}"
        print(f"\n>> CONFIG: {count} vecs, {dim} dim, {dtype}")
        
        res_entry = {
            "count": count,
            "dim": dim,
            "type": dtype,
            "ingest_mb_s": 0,
            "ingest_vec_s": 0,
            "doget_mb_s": 0,
            "doget_vec_s": 0,
            "dense_p50": 0, "dense_p95": 0, "dense_p99": 0,
            "filtered_p50": 0, "filtered_p95": 0, "filtered_p99": 0,
            "sparse_p50": 0, "sparse_p95": 0, "sparse_p99": 0,
            "hybrid_p50": 0, "hybrid_p95": 0, "hybrid_p99": 0,
            "error": ""
        }

        try:
            # 1. Ingest
            print("  Generating data...")
            table = perf_test.generate_vectors(count, dim, with_text=True, dtype_str=dtype)
            
            print("  Ingesting (DoPut)...")
            put_res = perf_test.benchmark_put(self.client, table, ds_name)
            res_entry["ingest_mb_s"] = put_res.throughput 
            res_entry["ingest_vec_s"] = put_res.rows / put_res.duration_seconds

            time.sleep(2) # Wait for indexing

            # 1.5 Retrieval (DoGet)
            print("  Retrieving (DoGet)...")
            get_res = perf_test.benchmark_get(self.client, ds_name)
            res_entry["doget_mb_s"] = get_res.throughput
            res_entry["doget_vec_s"] = get_res.rows / get_res.duration_seconds

            # 2. Dense Search
            q_vecs = perf_test.generate_query_vectors(100, dim, dtype_str=dtype)
            dense_res = perf_test.benchmark_vector_search(self.client, ds_name, q_vecs, k=10, vector_format=dtype)
            res_entry["dense_p50"] = dense_res.p50_ms
            res_entry["dense_p95"] = dense_res.p95_ms
            res_entry["dense_p99"] = dense_res.p99_ms

            # 3. Filtered Search (Metadata filter)
            filters = [{"field": "category", "op": "==", "value": "A"}]
            filt_res = perf_test.benchmark_vector_search(self.client, ds_name, q_vecs, k=10, filters=filters, vector_format=dtype)
            res_entry["filtered_p50"] = filt_res.p50_ms
            res_entry["filtered_p95"] = filt_res.p95_ms
            res_entry["filtered_p99"] = filt_res.p99_ms

            # 4. Sparse (Simulated via high-selectivity filter)
            sparse_filters = [{"field": "id", "op": "==", "value": 1}]
            sparse_res = perf_test.benchmark_vector_search(self.client, ds_name, q_vecs[:10], k=1, filters=sparse_filters, vector_format=dtype)
            res_entry["sparse_p50"] = sparse_res.p50_ms
            res_entry["sparse_p95"] = sparse_res.p95_ms
            res_entry["sparse_p99"] = sparse_res.p99_ms

            # 5. Hybrid Search
            txt_queries = ["machine learning" for _ in range(100)]
            hyb_res = perf_test.benchmark_hybrid_search(self.client, ds_name, q_vecs, k=10, text_queries=txt_queries, alpha=0.5)
            res_entry["hybrid_p50"] = hyb_res.p50_ms
            res_entry["hybrid_p95"] = hyb_res.p95_ms
            res_entry["hybrid_p99"] = hyb_res.p99_ms

            self.results.append(res_entry)
            
            # Cleanup
            try:
                import pyarrow.flight as flight
                action = flight.Action("delete-dataset", json.dumps({"dataset": ds_name}).encode("utf-8"))
                list(self.client._meta_client.do_action(action))
                print(f"  Cleaned up {ds_name}")
            except Exception as e:
                print(f"  Cleanup warning: {e}")
            
        except Exception as e:
            print(f"  FAILED: {e}")
            res_entry["error"] = str(e)
            self.results.append(res_entry)

    def run(self):
        for dtype in TYPES_FULL:
            for count in COUNTS:
                self.run_configuration(count, DIMS[0], dtype)
        
        self.save_results()

    def save_results(self):
        with open("detailed_matrix_results.json", "w") as f:
            json.dump(self.results, f, indent=2)
        print("\nSaved results to detailed_matrix_results.json")
        self.print_markdown()

    def print_markdown(self):
        # Format as Markdown Table
        df = pd.DataFrame(self.results)
        print("\n# Performance Matrix\n")
        print(df.to_markdown(index=False))

if __name__ == "__main__":
    runner = MatrixRunner("grpc://localhost:3000", "grpc://localhost:3001")
    runner.run()
