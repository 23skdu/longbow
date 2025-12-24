import json
import pyarrow.flight as flight
import pyarrow as pa
import numpy as np

def get_client(uri):
    return flight.FlightClient(uri)

def insert_data(client, dataset, num_vectors=10, dims=128):
    schema = pa.schema([
        ("id", pa.uint32()),
        ("vector", pa.list_(pa.float32(), dims))
    ])
    
    ids = pa.array(range(num_vectors), type=pa.uint32())
    # Random vectors
    data = np.random.rand(num_vectors, dims).astype(np.float32).tolist()
    vecs = pa.array(data, type=pa.list_(pa.float32(), dims))
    
    table = pa.Table.from_arrays([ids, vecs], schema=schema)
    descriptor = flight.FlightDescriptor.for_path(dataset)
    writer, _ = client.do_put(descriptor, schema)
    writer.write_table(table)
    writer.close()

def search_batched(client, dataset, query_vectors):
    req = {
        "dataset": dataset,
        "vectors": query_vectors,
        "k": 5,
        "local_only": True
    }
    action = flight.Action("VectorSearch", json.dumps(req).encode('utf-8'))
    results = list(client.do_action(action))
    
    all_results = []
    for res in results:
        result_json = json.loads(res.body.to_pybytes())
        all_results.append(result_json)
    return all_results

def main():
    data_client = get_client("grpc://localhost:3000")
    meta_client = get_client("grpc://localhost:3001")
    ds = "test_pipelining"
    dims = 128
    
    print("Inserting data...")
    insert_data(data_client, ds, num_vectors=100, dims=dims)
    import time
    time.sleep(2)
    
    query_vectors = [
        [0.1] * dims,
        [0.2] * dims,
        [0.3] * dims,
        [0.4] * dims,
        [0.5] * dims
    ]
    
    print(f"Sending batch search for {len(query_vectors)} vectors...")
    try:
        results = search_batched(meta_client, ds, query_vectors)
        print(f"Received {len(results)} results")
        for i, res in enumerate(results):
            ids = res.get('ids', [])
            print(f"Query {i}: found {len(ids)} IDs")
            
        if len(results) == len(query_vectors):
            print("PASS: Received one result per query vector")
        else:
            print(f"FAIL: Expected {len(query_vectors)} results, got {len(results)}")
    except Exception as e:
        print(f"FAIL: Error during batched search: {e}")

if __name__ == "__main__":
    main()
