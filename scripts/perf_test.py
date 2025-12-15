#!/usr/bin/env python3
import argparse
import sys
import time
import json
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import dask.dataframe as dd

def generate_batch(num_rows, dim):
    print(f"Generating {num_rows} vectors of dimension {dim}...")
    data = np.random.rand(num_rows, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)
    ids = pa.array(np.arange(num_rows), type=pa.int64())
    
    # Add timestamp column for testing temporal filters
    # Generate timestamps from now backwards
    now = pd.Timestamp.now()
    timestamps = [now - pd.Timedelta(minutes=i) for i in range(num_rows)]
    ts_array = pa.array(timestamps, type=pa.timestamp('ns'))

    schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('vector', tensor_type),
        pa.field('timestamp', pa.timestamp('ns'))
    ])

    return pa.Table.from_arrays([ids, vectors, ts_array], schema=schema)

def run_put(client, table, name):
    print(f"\n[PUT] Uploading dataset '{name}'...")
    descriptor = flight.FlightDescriptor.for_path(name)
    start_time = time.time()
    writer, _ = client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.close()
    duration = time.time() - start_time
    
    mb = table.nbytes / 1024 / 1024
    print(f"[PUT] Completed in {duration:.4f}s ({mb:.2f} MB)")
    print(f"[PUT] Throughput: {mb / duration:.2f} MB/s")
    return duration

def run_get(client, name, filters=None):
    print(f"\n[GET] Downloading dataset '{name}' with filters: {filters}...")
    
    query = {"name": name}
    if filters:
        query["filters"] = filters
        
    ticket = flight.Ticket(json.dumps(query).encode('utf-8'))
    
    start_time = time.time()
    try:
        reader = client.do_get(ticket)
        table = reader.read_all()
    except flight.FlightError as e:
        print(f"[GET] Error: {e}")
        return
        
    duration = time.time() - start_time
    
    # Dask Integration
    print("[DASK] Converting to Dask DataFrame...")
    pdf = table.to_pandas()
    ddf = dd.from_pandas(pdf, npartitions=4)
    
    print(f"[GET] Retrieved {len(ddf)} rows")
    print(f"[GET] Completed in {duration:.4f}s")
    print("\n--- Head of Data ---")
    print(ddf.head())
    print("--------------------
")

def list_available_flights(client):
    print("\n[META] Listing available flights...")
    try:
        for flight_info in client.list_flights():
            print(f" - Path: {flight_info.descriptor.path}")
            print(f"   Total Records: {flight_info.total_records}")
            print(f"   Total Bytes: {flight_info.total_bytes}")
    except Exception as e:
        print(f"[META] Error listing flights: {e}")

def main():
    parser = argparse.ArgumentParser(description='Longbow Perf Test with Dask & Filters')
    parser.add_argument('--host', default='localhost', help='Server host')
    parser.add_argument('--data-port', default=3000, type=int, help='Port for DoPut/DoGet')
    parser.add_argument('--meta-port', default=3001, type=int, help='Port for Metadata ops')
    parser.add_argument('--rows', default=10000, type=int, help='Number of rows')
    parser.add_argument('--dim', default=128, type=int, help='Vector dimension')
    parser.add_argument('--name', default='test_dataset', help='Dataset name')
    parser.add_argument('--filter', action='append', help='Add filter in format field:op:value (e.g. timestamp:>:2023-01-01)')

    args = parser.parse_args()

    data_loc = f"grpc://{args.host}:{args.data_port}"
    meta_loc = f"grpc://{args.host}:{args.meta_port}"

    print(f"Data Client: {data_loc}")
    print(f"Meta Client: {meta_loc}")

    data_client = flight.FlightClient(data_loc)
    meta_client = flight.FlightClient(meta_loc)

    # 1. Generate and Put Data
    table = generate_batch(args.rows, args.dim)
    run_put(data_client, table, args.name)

    # 2. Metadata Lookup
    list_available_flights(meta_client)

    # 3. Get Data (Full)
    run_get(data_client, args.name)

    # 4. Get Data (Filtered)
    # Example filter if none provided: last 5000 rows based on timestamp logic or ID
    # We'll use the user provided filters if any
    parsed_filters = []
    if args.filter:
        for f in args.filter:
            parts = f.split(':', 2)
            if len(parts) == 3:
                parsed_filters.append({"field": parts[0], "operator": parts[1], "value": parts[2]})
    
    if parsed_filters:
        run_get(data_client, args.name, parsed_filters)
    else:
        # Demo filter: ID < 10
        print("Running demo filter: id < 10")
        run_get(data_client, args.name, [{"field": "id", "operator": "<", "value": "10"}])

if __name__ == "__main__":
    main()
