import json
import time
import pyarrow as pa
import pyarrow.flight as flight
import numpy as np

def get_client(uri):
    return flight.FlightClient(uri)

def insert_vector(client, dataset, vector_id, vector):
    # simple DoPut with 1 row
    schema = pa.schema([
        ("id", pa.uint32()),
        ("vector", pa.list_(pa.float32(), 128)),
        ("text", pa.string()) # Optional
    ])
    
    # Create single-row table
    ids = pa.array([vector_id], type=pa.uint32())
    vecs = pa.array([vector], type=pa.list_(pa.float32(), 128))
    texts = pa.array(["test"], type=pa.string())
    table = pa.Table.from_arrays([ids, vecs, texts], schema=schema)
    
    descriptor = flight.FlightDescriptor.for_path(dataset)
    writer, _ = client.do_put(descriptor, schema)
    writer.write_table(table)
    writer.close()

def search(client, dataset, query_vec, local_only=False):
    req = {
        "dataset": dataset,
        "vector": query_vec,
        "k": 10,
        "local_only": local_only
    }
    action = flight.Action("VectorSearch", json.dumps(req).encode('utf-8'))
    results = list(client.do_action(action))
    if not results:
        return []
    result_json = json.loads(results[0].body.to_pybytes())
    return result_json.get("ids", [])

def main():
    print("Waiting for cluster to settle...")
    time.sleep(5) 
    
    # Data Ports: 3000, 3010, 3020
    # Meta Ports: 3001, 3011, 3021
    
    d1 = get_client("grpc://localhost:3000")
    m1 = get_client("grpc://localhost:3001")
    
    d2 = get_client("grpc://localhost:3010")
    # m2 = get_client("grpc://localhost:3011") # Unused
    
    d3 = get_client("grpc://localhost:3020")
    # m3 = get_client("grpc://localhost:3021") # Unused
    
    ds = "global_test"
    vec = [0.1] * 128
    
    print("Inserting data (Data Plane)...")
    # Node 1 gets ID 1
    insert_vector(d1, ds, 1, vec)
    # Node 2 gets ID 2
    insert_vector(d2, ds, 2, vec)
    # Node 3 gets ID 3
    insert_vector(d3, ds, 3, vec)
    
    print("Data inserted. Waiting for indexing...")
    time.sleep(2)
    
    print("Testing GLOBAL Search on Node 1 (Meta Plane)...")
    ids = search(m1, ds, vec, local_only=False)
    print(f"Global IDs: {ids}")
    
    # HNSW Index assigns internal IDs (0, 1, 2...), ignoring user IDs.
    # We verify that we got 3 unique results, proving we reached all 3 nodes.
    if len(set(ids)) == 3:
        print("PASS: Global search found 3 unique IDs (Scatter-Gather working)")
    else:
        print(f"FAIL: Expected 3 unique IDs, got {len(set(ids))}: {ids}")
        exit(1)
        
    print("Testing LOCAL Search on Node 1 (Meta Plane)...")
    ids = search(m1, ds, vec, local_only=True)
    print(f"Local IDs: {ids}")
    
    if len(ids) == 1:
        print("PASS: Local search found 1 result")
    else:
        print(f"FAIL: Local search expected 1 result, got {len(ids)}")
        exit(1)

if __name__ == "__main__":
    main()
