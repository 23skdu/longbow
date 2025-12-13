#!/usr/bin/env python3
import argparse,sys,time
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight

def generate_batch(num_rows, dim):
    print(f"Generating {num_rows} vectors of dimension {dim}...")
    # Generate random float32 vectors
    data = np.random.rand(num_rows, dim).astype(np.float32)

    # Create Arrow arrays
    # We treat the vector as a FixedSizeList
    tensor_type = pa.list_(pa.float32(), dim)

    # Flatten for Arrow storage
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)

    # Create IDs
    ids = pa.array(np.arange(num_rows), type=pa.int64())

    # Create Table
    schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('vector', tensor_type)
    ])

    return pa.Table.from_arrays([ids, vectors], schema=schema)

def run_put(client, table, name):
    print(f"\n[PUT] Uploading dataset '{name}'...")

    # Create descriptor
    descriptor = flight.FlightDescriptor.for_path(name)

    start_time = time.time()
    writer, _ = client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.close()
    end_time = time.time()

    duration = end_time - start_time
    total_bytes = table.nbytes
    throughput_mb = (total_bytes / 1024 / 1024) / duration

    print(f"[PUT] Completed in {duration:.4f}s")
    print(f"[PUT] Throughput: {throughput_mb:.2f} MB/s")
    print(f"[PUT] Records/sec: {table.num_rows / duration:.2f}")
    return duration

def run_get(client, name, expected_rows):
    print(f"\n[GET] Downloading dataset '{name}'...")

    ticket = flight.Ticket(name.encode('utf-8'))

    start_time = time.time()
    reader = client.do_get(ticket)
    read_table = reader.read_all()
    end_time = time.time()

    duration = end_time - start_time
    total_bytes = read_table.nbytes
    throughput_mb = (total_bytes / 1024 / 1024) / duration

    print(f"[GET] Completed in {duration:.4f}s")
    print(f"[GET] Throughput: {throughput_mb:.2f} MB/s")
    print(f"[GET] Records/sec: {read_table.num_rows / duration:.2f}")

    if read_table.num_rows != expected_rows:
        print(f"[WARN] Mismatch! Expected {expected_rows} rows, got {read_table.num_rows}")
    else:
        print(f"[OK] Row count verified: {read_table.num_rows}")

    return duration

def main():
    parser = argparse.ArgumentParser(description='Longbow Arrow Flight Performance Test')
    parser.add_argument('--host', default='localhost', help='Server host')
    parser.add_argument('--port', default=3000, type=int, help='Server port')
    parser.add_argument('--rows', default=100000, type=int, help='Number of rows')
    parser.add_argument('--dim', default=128, type=int, help='Vector dimension')
    parser.add_argument('--name', default='test_dataset', help='Dataset name')

    args = parser.parse_args()

    location = f"grpc://{args.host}:{args.port}"
    print(f"Connecting to {location}...")

    try:
        client = flight.FlightClient(location)

        # Generate Data
        table = generate_batch(args.rows, args.dim)
        print(f"Data Size: {table.nbytes / 1024 / 1024:.2f} MB")

        # Run Tests
        run_put(client, table, args.name)
        run_get(client, args.name, args.rows)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
