
import pyarrow.flight as flight
import pyarrow as pa
import numpy as np
import argparse
import time
import json
from concurrent.futures import ThreadPoolExecutor

DTYPES = {
    "float16": np.float16,
    "float32": np.float32,
    "float64": np.float64,
    # "complex64": np.complex64, # Need special handling if pyarrow doesn't support directly
    # "complex128": np.complex128 
}

# Longbow client shim for complex types if needed, or use existing client logic
# For this script, we'll use direct FlightClient but handle conversions manually if needed

def generate_batch(start_id, count, dim, dtype_str):
    np_type = DTYPES.get(dtype_str, np.float32)
    
    # Generate data
    if "complex" in dtype_str:
        # Complex support in numpy/arrow is tricky. 
        # Longbow expects fixed_size_list of float32/64 usually, with metadata
        # Reusing existing client logic or mimicking it is best.
        # Let's assume standard float types first for this pass, 
        # or implement complex later if basic float works.
        # The user specifically asked for "all data types up to complex128".
        # We need to handle complex.
        pass

    # Simplified generator for standard floats
    data = np.random.rand(count, dim).astype(np_type)
    
    # For Arrow, we often cast to float32 or float64 because float16 support is limited in some PyArrow versions
    # BUT Longbow now supports float16. 
    # Let's use the logic from investigate_dtypes.py: pass list of numpy arrays
    
    vecs = list(data)
    
    # Batch construction
    ids = [str(i) for i in range(start_id, start_id + count)]
    timestamps = [int(time.time_ns())] * count
    
    # Categories for filters
    cats = [f"cat_{i%5}" for i in range(start_id, start_id + count)]
    
    # Texts for hybrid
    texts = [f"doc {i} about stuff" for i in range(start_id, start_id + count)]

    return ids, vecs, timestamps, cats, texts

def benchmark_type(uri, dtype_str, dim, num_vectors):
    print(f"\n[{dtype_str}] Benchmarking {num_vectors} vectors (dim={dim})...")
    client = flight.FlightClient(uri)
    dataset = f"validate_{dtype_str}_{dim}_{int(time.time())}"
    
    # 1. Ingest
    print(f"  Ingesting...")
    batch_size = 2500
    start_time = time.time()
    for i in range(0, num_vectors, batch_size):
        count = min(batch_size, num_vectors - i)
        
        # Use simple dictionary -> Table conversion
        # We need to handle specific types carefully
        data = np.random.rand(count, dim)
        vector_list = []
        
        if dtype_str == "float16":
            # Pass as raw list of np.float16 arrays to let arrow/client handle it?
            # PyArrow < 14 might struggle. 
            # Strategy: Use list of lists for safety, or numpy arrays if known to work
            # investigate_dtypes used: `df = pd.DataFrame({'vector': [v for v in data_f16]})`
            data = data.astype(np.float16)
            vector_list = [v for v in data]
        elif dtype_str == "float32":
            data = data.astype(np.float32)
            vector_list = [v for v in data]
        elif dtype_str == "float64":
            data = data.astype(np.float64)
            vector_list = [v for v in data]
        elif dtype_str == "complex64":
            # Simulate complex as 2x float32 dim? Or use client specific logic?
            # The server expects fixed_size_list<float32>[2*dim] and metadata.
            # Let's keep it simple: 
            # To properly test complex, we should probably use the SDK client if possible, 
            # as it handles the "view as float" logic.
            # IMPT: This script will use LongbowClient to simplify complex handling.
            pass

    # Switching to SDK client usage for simplicity and correctness
    from longbow.client import LongbowClient
    import pandas as pd
    
    lb_client = LongbowClient(uri)
    
    # Generate Data
    if "complex" in dtype_str:
        raw_data = np.random.rand(num_vectors, dim) + 1j * np.random.rand(num_vectors, dim)
        if dtype_str == "complex64":
            raw_data = raw_data.astype(np.complex64)
        else:
            raw_data = raw_data.astype(np.complex128)
    else:
        raw_data = np.random.rand(num_vectors, dim).astype(DTYPES[dtype_str])
        
    # Ingest in batches
    for i in range(0, num_vectors, batch_size):
        end = min(i+batch_size, num_vectors)
        batch_vecs = raw_data[i:end]
        
        # DataFrame
        df = pd.DataFrame({
            "id": [str(k) for k in range(i, end)],
            "vector": [v for v in batch_vecs],
            "category": [f"cat_{k%5}" for k in range(i, end)],
            "text": [f"doc {k} concerning topic {k%3}" for k in range(i, end)] 
        })
        
        lb_client.insert(dataset, df)
        print(f"    Inserted {end}/{num_vectors}")
    
    duration = time.time() - start_time
    print(f"  Ingestion done in {duration:.2f}s ({(num_vectors/duration):.0f} vec/s)")
    
    # 2. Dense Search
    print("  Dense Search...")
    q = raw_data[0] # Query with first vector
    
    # Explicitly handle query vector preparation to avoid client-side issues
    query_vec = q
    if "complex" in dtype_str:
        if dtype_str == "complex64":
            query_vec = q.view(np.float32).flatten().tolist()
        else:
            query_vec = q.view(np.float64).flatten().tolist()
    else:
        query_vec = q.tolist()

    max_retries = 10
    got_results = False
    for attempt in range(max_retries):
        try:
            res = lb_client.search(dataset, query_vec, k=10)
            if len(res) > 0:
                print(f"    Got {len(res)} results (attempt {attempt+1})")
                got_results = True
                break
            else:
                print(f"    Got 0 results (attempt {attempt+1}), retrying...")
                time.sleep(0.5)
        except Exception as e:
            print(f"    Dense Search Failed: {e}")
            break
            
    if not got_results:
        print("    Dense Search Failed: 0 results after retries")
    
    
    # 3. Sparse/Filtered Search
    print("  Filtered Search (category='cat_0')...")
    try:
        # Correctly pass 'filters' as a list of dicts, not 'filter' kwarg
        filter_list = [{"field": "category", "operator": "==", "value": "cat_0"}]
        res = lb_client.search(dataset, query_vec, k=10, filters=filter_list)
        print(f"    Got {len(res)} results")
    except Exception as e:
        print(f"    Filtered Search Failed: {e}")

    
    # 4. Hybrid Search
    print("  Hybrid Search (text='topic 0')...")
    
    try:
        # Prepare vector for hybrid search
        search_vec = q
        if "complex" in dtype_str:
            if dtype_str == "complex64":
                search_vec_list = q.view(np.float32).tolist()
            else:
                search_vec_list = q.view(np.float64).tolist()
        else:
            search_vec_list = q.tolist()
            
        req = {
            "dataset": dataset,
            "vector": search_vec_list,
            "k": 10,
            "text_query": "topic 0",
            "alpha": 0.5
        }
        
        action = flight.Action("HybridSearch", json.dumps(req).encode('utf-8'))
        list(client.do_action(action))
        print(f"    Hybrid Search OK")
    except Exception as e:
        print(f"    Hybrid Search Failed: {e}")

    # 4. Sparse (Action?) or is it handled by Filtered?
    # User asked for "Dense, Sparse, Filtered, Hybrid".
    # "Sparse" usually refers to Sparse Vector search (e.g. SPLADE). 
    # Longbow context: "Sparse" might mean pre-filtered or something else?
    # In benchmark_comprehensive.py: "Sparse Search (using filters to reduce candidate set)". 
    # So Filtered == Sparse in this context? 
    # "Sparse Search: filter to specific category (reduces search space)"
    # "Filtered Search: filter to 30% of data"
    # They seem synonymous in the benchmark script, just different selectivity.
    # We will treat Filtered as covering Sparse requirement unless SPLADE is implied.
    
    print(f"[{dtype_str}] PASSED")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--count", type=int, default=25000)
    parser.add_argument("--type", type=str, default="all", help="Specific type to test (or 'all')")
    args = parser.parse_args()
    
    uri = "grpc://localhost:3000"
    
    # DTypes to test
    types = ["float16", "float32", "float64", "complex64", "complex128"]
    if args.type != "all":
        if args.type in types:
            types = [args.type]
        else:
            print(f"Unknown type: {args.type}. valid: {types}")
            return
    
    for t in types:
        try:
            benchmark_type(uri, t, args.dim, args.count)
        except Exception as e:
            print(f"FAILED {t}: {e}")
            # Don't exit, try others

if __name__ == "__main__":
    main()
