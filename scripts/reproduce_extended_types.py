import pyarrow as pa
import pyarrow.flight as flight
import numpy as np
import json
import time
import sys

# Configuration
URL = "grpc://localhost:3000"
NUM_VECTORS = 5000

def get_client():
    return flight.FlightClient(URL)

def create_table(dim, num_vectors, dtype_pa, dtype_str):
    dataset_name = f"repro_{dtype_str}_{dim}"
    
    metadata = {
        "longbow.vector_type": dtype_str
    }
    
    if "complex" in dtype_str:
        metadata["longbow.complex"] = "true"
        # For complex, we generate 2x floats and treat them as interleaved
        if dtype_str == "complex64":
            data = np.random.rand(num_vectors, dim * 2).astype(np.float32)
            elem_type = pa.float32()
        else: # complex128
            data = np.random.rand(num_vectors, dim * 2).astype(np.float64)
            elem_type = pa.float64()
    else:
        if dtype_pa == pa.float16():
            data = np.random.rand(num_vectors, dim).astype(np.float16)
        elif dtype_pa == pa.float64():
            data = np.random.rand(num_vectors, dim).astype(np.float64)
        else:
            data = np.random.rand(num_vectors, dim).astype(np.float32)
        elem_type = dtype_pa

    # FixedSizeList of elements
    vectors = [list(v) for v in data]
    vector_field = pa.field("vector", pa.list_(elem_type, len(data[0])), metadata=metadata)
    
    schema = pa.schema([
        pa.field("id", pa.string()),
        vector_field,
        pa.field("category", pa.string())
    ], metadata=metadata)
    
    table = pa.Table.from_pydict({
        "id": [str(i) for i in range(num_vectors)],
        "vector": vectors,
        "category": [f"cat_{i % 5}" for i in range(num_vectors)]
    }, schema=schema)
    
    return dataset_name, table

def run_repro(dtype_str, dim):
    print(f"\n--- Testing Type: {dtype_str} (dim={dim}) ---")
    client = get_client()
    
    pa_types = {
        "float16": pa.float16(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "complex64": pa.float32(), # Interleaved
        "complex128": pa.float64(), # Interleaved
    }
    
    if dtype_str not in pa_types:
         print(f"Skipping {dtype_str} - PyArrow mapping unknown")
         return

    dataset_name, table = create_table(dim, NUM_VECTORS, pa_types[dtype_str], dtype_str)
    
    try:
        # 1. Put
        options = flight.FlightCallOptions()
        descriptor = flight.FlightDescriptor.for_path(dataset_name)
        writer, _ = client.do_put(descriptor, table.schema, options)
        writer.write_table(table)
        writer.close()
        print(f"  DoPut: Success ({dataset_name})")
        
        # 2. Get
        ticket = flight.Ticket(json.dumps({"name": dataset_name}).encode('utf-8'))
        reader = client.do_get(ticket)
        res_table = reader.read_all()
        print(f"  DoGet: Success ({len(res_table)} rows)")
        
        # 2.5 Wait for Indexing
        # Indexing is async, so we wait a bit for the index to be ready
        print("  Waiting for indexing...")
        time.sleep(10)
        # We use a float32 vector for query
        if "complex" in dtype_str:
            query_vec = np.random.rand(dim * 2).astype(np.float32).tolist()
        else:
            query_vec = np.random.rand(dim).astype(np.float32).tolist()
            
        action_req = json.dumps({
            "dataset": dataset_name,
            "vector": query_vec,
            "k": 5
        }).encode('utf-8')
        
        action = flight.Action("VectorSearch", action_req)
        results = list(client.do_action(action))
        
        # Parse the last result body (Arrow record batch)
        if len(results) > 0:
            import io
            reader = pa.ipc.open_stream(io.BytesIO(results[-1].body))
            result_table = reader.read_all()
            print(f"  Search: Success ({len(result_table)} rows, k=5 expected)")
            if len(result_table) != 5:
                print(f"    WARNING: Expected 5 results, got {len(result_table)}")
        else:
            print("  Search: FAILED (No results returned)")
        
    except Exception as e:
        print(f"  FAILED: {e}")

if __name__ == "__main__":
    types = ["float16", "float64", "complex64", "complex128"]
    dims = [128, 384]
    for dt in types:
        for d in dims:
            run_repro(dt, d)
