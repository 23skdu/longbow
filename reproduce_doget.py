import pyarrow as pa
from longbow import LongbowClient
import pandas as pd
import time
import numpy as np
import json

def generate_vectors(num_rows, dim):
    return np.random.rand(num_rows, dim).astype(np.float32)

def main():
    client = LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")
    name = "repro_dataset_adv"
    dim = 128
    rows = 500

    print(f"Selecting dataset '{name}'...")
    try:
        client.create_namespace(name, force=True)
    except Exception as e:
        print(f"Create error (ignored): {e}")

    # Generate Data
    print("Generating data...")
    vectors = generate_vectors(rows, dim)
    ids = [str(i) for i in range(rows)]
    # Add category column for filtering
    cats = ["A" if i % 2 == 0 else "B" for i in range(rows)]
    
    df = pd.DataFrame({
        "id": ids,
        "vector": vectors.tolist(),
        "category": cats
    })
    
    print("Inserting data...")
    client.insert(name, df)
    
    print("Waiting for ingestion...")
    time.sleep(2)
    
    # DELETE roughly 10%
    del_ids = list(range(0, rows, 10))
    print(f"Deleting {len(del_ids)} IDs...")
    for vid in del_ids:
        try:
            client.delete(name, ids=[vid])
        except Exception:
            pass
            
    # TEST 1: Filtered Download (Category 'A')
    print("\n[TEST 1] Filtered Download (Category='A')...")
    filters = [{"field": "category", "op": "Eq", "value": "A"}]
    try:
        table = client.download_arrow(name, filter=filters)
        print(f"Success! Downloaded {table.num_rows} rows.")
    except Exception as e:
        print(f"ERROR CAUGHT (Test 1): {e}")

    # TEST 2: Empty Result Filter
    print("\n[TEST 2] Empty Result Filter (Category='Z')...")
    filters_z = [{"field": "category", "op": "Eq", "value": "Z"}]
    try:
        table = client.download_arrow(name, filter=filters_z)
        print(f"Success! Downloaded {table.num_rows} rows.")
    except Exception as e:
        print(f"ERROR CAUGHT (Test 2): {e}")

    # TEST 3: Streaming (download_stream)
    print("\n[TEST 3] Streaming Download...")
    try:
        count = 0
        for batch in client.download_stream(name):
            count += batch.num_rows
        print(f"Success! Streamed {count} rows.")
    except Exception as e:
        print(f"ERROR CAUGHT (Test 3): {e}")

if __name__ == "__main__":
    main()
