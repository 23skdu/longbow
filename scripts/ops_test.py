import argparse
import json
import sys
import time
import pyarrow as pa
import pyarrow.flight as flight
import numpy as np
import polars as pl

def get_client(host, port):
    location = f"grpc://{host}:{port}"
    return flight.FlightClient(location)

def do_action_snapshot(client):
    print("Triggering snapshot action...")
    try:
        # The action type for snapshot is likely defined in the server, assuming 'snapshot'
        action = flight.Action("snapshot", b"")
        results = client.do_action(action)
        for result in results:
            print(f"Snapshot Result: {result.body.to_pybytes().decode('utf-8')}")
    except Exception as e:
        print(f"Error triggering snapshot: {e}")

def list_flights(client):
    print("Listing flights...")
    try:
        flights = client.list_flights()
        for info in flights:
            descriptor = info.descriptor
            path = descriptor.path[0].decode('utf-8') if descriptor.path else "Unknown"
            print(f"Flight: {path}")
            print(f"  Schema: {info.schema}")
            print(f"  Total Records: {info.total_records}")
            print(f"  Total Bytes: {info.total_bytes}")
    except Exception as e:
        print(f"Error listing flights: {e}")

def get_flight_info(client, dataset_name):
    print(f"Getting flight info for dataset: {dataset_name}...")
    try:
        descriptor = flight.FlightDescriptor.for_path(dataset_name)
        info = client.get_flight_info(descriptor)
        print(f"Flight Info for {dataset_name}:")
        print(f"  Schema: {info.schema}")
        print(f"  Total Records: {info.total_records}")
        print(f"  Total Bytes: {info.total_bytes}")
        for endpoint in info.endpoints:
            print(f"  Endpoint: {endpoint.locations}")
    except Exception as e:
        print(f"Error getting flight info: {e}")

def perform_search(client, dataset_name, vector_size=128, k=10):
    print(f"Performing vector search on {dataset_name}...")
    try:
        # Generate a random query vector
        query_vector = np.random.rand(vector_size).astype(np.float32)
        
        # Construct the search ticket
        # Assuming the server expects a JSON ticket with 'query' and 'k'
        ticket_data = {
            "dataset": dataset_name,
            "k": k,
            "query": query_vector.tolist()
        }
        ticket_bytes = json.dumps(ticket_data).encode('utf-8')
        ticket = flight.Ticket(ticket_bytes)
        
        reader = client.do_get(ticket)
        table = reader.read_all()
        print(f"Search Results (Top {k}):")
        
        # Convert Arrow Table to Polars DataFrame
        df = pl.from_arrow(table)
        print(df)
    except Exception as e:
        print(f"Error performing search: {e}")

def main():
    parser = argparse.ArgumentParser(description="Longbow Ops Test Script")
    parser.add_argument("--host", default="localhost", help="Flight server host")
    parser.add_argument("--data-port", type=int, default=3000, help="Data plane port")
    parser.add_argument("--meta-port", type=int, default=3001, help="Control plane port")
    parser.add_argument("--action", choices=["snapshot", "list", "info", "search", "all"], default="all", help="Action to perform")
    parser.add_argument("--dataset", default="test_dataset", help="Dataset name for info/search")
    parser.add_argument("--vector-size", type=int, default=128, help="Vector size for search")
    
    args = parser.parse_args()
    
    data_client = get_client(args.host, args.data_port)
    meta_client = get_client(args.host, args.meta_port)
    
    if args.action in ["snapshot", "all"]:
        # Snapshots are typically administrative, might be on meta or data port depending on implementation
        # Assuming meta port for admin actions if separated, or data port if not.
        # Let's try meta first as it's a control operation.
        do_action_snapshot(meta_client)
        
    if args.action in ["list", "all"]:
        list_flights(meta_client)
        
    if args.action in ["info", "all"]:
        get_flight_info(meta_client, args.dataset)
        
    if args.action in ["search", "all"]:
        perform_search(data_client, args.dataset, args.vector_size)

if __name__ == "__main__":
    main()
