import pyarrow.flight as flight
import json
import numpy as np
import pyarrow as pa
import sys

def main():
    uri = "grpc://localhost:3000"
    meta_uri = "grpc://localhost:3001"
    client = flight.FlightClient(uri)
    m_client = flight.FlightClient(meta_uri)
    
    print(f"Testing PUT to {uri}...")
    dim = 384
    rows = 10
    data = np.random.rand(rows, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)
    ids = pa.array(range(rows), type=pa.int64())
    ts = pa.array([0] * rows, type=pa.timestamp("ns"))
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
    ])
    table = pa.Table.from_arrays([ids, vectors, ts], schema=schema)
    
    descriptor = flight.FlightDescriptor.for_path("test_dataset")
    writer, _ = client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.close()
    print("PUT successful.")

    print(f"Testing SEARCH to {meta_uri}...")
    vec = np.random.rand(dim).astype(np.float32).tolist()
    req = json.dumps({
        "dataset": "test_dataset",
        "vector": vec,
        "k": 5
    }).encode("utf-8")
    action = flight.Action("VectorSearch", req)
    results = list(m_client.do_action(action))
    print(f"SEARCH successful. Results: {len(results)}")

if __name__ == "__main__":
    main()
