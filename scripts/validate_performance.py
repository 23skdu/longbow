#!/usr/bin/env python3
"""
Performance Validation Script
Asserts that Longbow meets specific throughput targets for 10k-50k vectors.
Target: Ingest >= 800 MB/s, DoGet >= 1.7 GB/s
"""
import argparse
import sys
import time
import numpy as np
import pyarrow as pa
from longbow import LongbowClient

# Targets
TARGET_INGEST_MB_S = 800.0
TARGET_GET_MB_S = 1700.0  # 1.7 GB/s

def generate_data(rows, dim):
    print(f"Generating {rows} vectors (dim={dim})...")
    data = np.random.rand(rows, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    vectors = pa.FixedSizeListArray.from_arrays(data.flatten(), type=tensor_type)
    ids = pa.array(np.arange(rows), type=pa.int64())
    ts = pa.array([time.time_ns()] * rows, type=pa.timestamp("ns"))
    
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns"))
    ])
    return pa.Table.from_arrays([ids, vectors, ts], schema=schema)

def validate(args):
    client = LongbowClient(uri=args.data_uri, meta_uri=args.meta_uri)
    dataset = f"validate_{args.rows}_{args.dim}"
    
    # 1. Ingestion Validation
    table = generate_data(args.rows, args.dim)
    print(f"\n[VALIDATE] Ingestion for {args.rows} rows...")
    start = time.time()
    
    # Chunked insert as per perf_test recommendations
    chunk_size = 5000
    for i in range(0, table.num_rows, chunk_size):
        chunk = table.slice(i, min(chunk_size, table.num_rows - i))
        client.insert(dataset, chunk)
        
    duration = time.time() - start
    mb = table.nbytes / 1024 / 1024
    throughput = mb / duration
    print(f"-> Ingest: {throughput:.2f} MB/s (Target: {TARGET_INGEST_MB_S} MB/s)")
    
    if throughput < TARGET_INGEST_MB_S:
        print(f"❌ FAIL: Ingestion throughput too low ({throughput:.2f} < {TARGET_INGEST_MB_S})")
    else:
        print("✅ PASS: Ingestion target met")

    print("Waiting for indexing...")
    time.sleep(5)

    # 2. DoGet Validation
    print(f"\n[VALIDATE] DoGet for {args.rows} rows...")
    start = time.time()
    table_out = client.download_arrow(dataset)
    duration = time.time() - start
    mb_out = table_out.nbytes / 1024 / 1024
    throughput_get = mb_out / duration
    print(f"-> DoGet: {throughput_get:.2f} MB/s (Target: {TARGET_GET_MB_S} MB/s)")
    
    if throughput_get < TARGET_GET_MB_S:
        print(f"❌ FAIL: DoGet throughput too low ({throughput_get:.2f} < {TARGET_GET_MB_S})")
    else:
        print("✅ PASS: DoGet target met")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-uri", default="grpc://0.0.0.0:3000")
    parser.add_argument("--meta-uri", default="grpc://0.0.0.0:3001")
    parser.add_argument("--rows", type=int, default=25000)
    parser.add_argument("--dim", type=int, default=128)
    args = parser.parse_args()
    
    validate(args)
