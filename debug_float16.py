#!/usr/bin/env python3
"""Minimal float16 test to debug client hang"""
import sys
import os
import numpy as np
import pyarrow as pa
import time

# Add SDK to path
current_dir = os.getcwd()
sdk_src = os.path.join(current_dir, "longbowclientsdk", "src")
if sdk_src not in sys.path:
    sys.path.insert(0, sdk_src)

from longbow.client import LongbowClient

def main():
    print("Connecting to cluster...")
    client = LongbowClient(uri="grpc://127.0.0.1:3000")
    
    print("Generating 1000 float16 vectors...")
    dim = 384
    rows = 1000
    
    # Generate float16 data
    vectors = np.random.randn(rows, dim).astype(np.float16)
    ids = np.arange(rows, dtype=np.uint32)
    
    print(f"Vector shape: {vectors.shape}, dtype: {vectors.dtype}")
    print(f"IDs shape: {ids.shape}, dtype: {ids.dtype}")
    
    # Create Arrow table
    schema = pa.schema([
        ('id', pa.uint32()),
        ('vector', pa.list_(pa.float16(), dim))
    ])
    
    print("Creating Arrow table...")
    table = pa.Table.from_arrays([
        pa.array(ids),
        pa.FixedSizeListArray.from_arrays(vectors.flatten(), dim)
    ], schema=schema)
    
    print(f"Table created: {table.num_rows} rows, {table.nbytes} bytes")
    
    dataset_name = "debug_float16_minimal"
    print(f"\nUploading to dataset '{dataset_name}'...")
    print("Starting PUT operation...")
    
    start = time.time()
    try:
        client.insert(dataset_name, table)
        elapsed = time.time() - start
        print(f"✅ PUT completed in {elapsed:.2f}s")
    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ PUT failed after {elapsed:.2f}s: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print("\n✅ Test completed successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
