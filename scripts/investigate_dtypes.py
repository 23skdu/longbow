import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import numpy as np
import json
import time
import sys
from longbow.client import LongbowClient

def test_type(client, dtype, dim, num_vectors=100):
    print(f"\n[TEST] Type: {dtype}, Dim: {dim}")
    dataset = f"test_{dtype}_{dim}_{int(time.time())}"
    
    # Generate data
    if dtype.startswith('complex'):
        real = np.random.rand(num_vectors, dim).astype(np.float32)
        imag = np.random.rand(num_vectors, dim).astype(np.float32)
        data = real + 1j * imag
        if dtype == 'complex128':
            data = data.astype(np.complex128)
        else:
            data = data.astype(np.complex64)
    else:
        data = np.random.rand(num_vectors, dim).astype(dtype)
        
    df = pd.DataFrame({
        'id': [str(i) for i in range(num_vectors)],
        'vector': [v for v in data],
        'category': ['A' for _ in range(num_vectors)]
    })
    
    # Insert
    print(f"  Inserting {num_vectors} rows...")
    try:
        client.insert(dataset, df)
    except Exception as e:
        print(f"  INSERT FAILED: {e}")
        return
        
    time.sleep(1) # Wait for ingestion
    
    # DoGet
    print(f"  Downloading (DoGet)...")
    try:
        table = client.download_arrow(dataset)
        print(f"  SUCCESS: Downloaded {table.num_rows} rows.")
    except Exception as e:
        print(f"  DOWNLOAD FAILED: {e}")

def main():
    client = LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")
    
    # combinations that failed in matrix
    failures = [
        ('float16', 128),
        ('float64', 128),
        ('complex64', 128),
        ('complex64', 384),
        ('complex128', 384),
    ]
    
    for dtype, dim in failures:
        test_type(client, dtype, dim)

if __name__ == "__main__":
    main()
