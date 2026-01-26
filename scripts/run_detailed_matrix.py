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
COUNTS = [5000, 10000, 25000]
DIMS = [128, 384, 1536, 3072]
# "All supported data types" - based on perf_test.py excluding some exotic ones?
# User said "all support data types".
TYPES = [
    "float32", "float16", 
    # "float64", # Often not hw accelerated, but supported
    # "int8", "int16", "int32", "int64", 
    # "uint8", 
    # "complex64", "complex128"
]
# We'll start with a core set to ensure it finishes reasonable fast for the demo, 
# but the script structure will support all. 
TYPES_FULL = [
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64", 
    "float16", "float32", "float64", 
    "complex64", "complex128"
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
            "latency_dense": 0,
            "latency_sparse": 0,
            "latency_filtered": 0,
            "latency_hybrid": 0
        }

        try:
            # 1. Ingest
            print("  Generating data...")
            table = perf_test.generate_vectors(count, dim, with_text=True, dtype_str=dtype)
            
            print("  Ingesting (DoPut)...")
            put_res = perf_test.benchmark_put(self.client, table, ds_name)
            res_entry["ingest_mb_s"] = put_res.throughput 
            res_entry["ingest_vec_s"] = put_res.rows / put_res.duration_seconds

            time.sleep(1) # settle

            # 1.5 Retrieval (DoGet)
            print("  Retrieving (DoGet)...")
            get_res = perf_test.benchmark_get(self.client, ds_name)
            res_entry["doget_mb_s"] = get_res.throughput
            res_entry["doget_vec_s"] = get_res.rows / get_res.duration_seconds

            # 2. Dense Search
            q_vecs = perf_test.generate_query_vectors(100, dim, dtype_str=dtype)
            dense_res = perf_test.benchmark_vector_search(self.client, ds_name, q_vecs, k=10, vector_format=dtype)
            res_entry["latency_dense"] = dense_res.p95_ms

            # 3. Filtered Search (Metadata filter)
            # perf_test generates "category" field: "A" or "B"
            filters = [{"field": "category", "op": "==", "value": "A"}]
            dt_filt_res = perf_test.benchmark_vector_search(self.client, ds_name, q_vecs, k=10, filters=filters, vector_format=dtype)
            res_entry["latency_filtered"] = dt_filt_res.p95_ms

            # 4. Sparse/Id Search (Simulating "Sparse" as lookup or specific high-selectivity)
            # User asked for "4 search types". In comprehensive it was Dense, Sparse, Filtered, Hybrid.
            # Sparse there meant "filter to specific single item" effectively?
            # Let's use ID search as proxy for "Sparse/Point" lookup latency? 
            # Or just highly selective filter?
            # Let's match benchmark_comprehensive "Sparse" which was filter to 1/10th.
            # Here "category" is 1/2.
            # We'll stick to Filtered (category) and Hybrid. 
            # Fourth type? "Search By ID"?
            id_res = perf_test.benchmark_search_by_id(self.client, ds_name, ids=["1", "10", "100"], k=10)
            res_entry["latency_sparse"] = id_res.p95_ms # Using ID lookup as "Sparse/Exact" proxy

            # 5. Hybrid Search
            # generate text queries
            txt_queries = ["machine learning" for _ in range(100)]
            hyb_res = perf_test.benchmark_hybrid_search(self.client, ds_name, q_vecs, k=10, text_queries=txt_queries, alpha=0.5)
            res_entry["latency_hybrid"] = hyb_res.p95_ms

            self.results.append(res_entry)
            
            # Cleanup to free memory
            try:
                # Use internal meta client to delete dataset if SDK doesn't expose it directly yet
                # Or assume client.delete_dataset exists (it might not)
                # flight action "delete-dataset"
                import json
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
            for dim in DIMS:
                for count in COUNTS:
                    self.run_configuration(count, dim, dtype)
        
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
