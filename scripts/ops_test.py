#!/usr/bin/env python3
"""Longbow Operational Test Script

CLI tool for testing Longbow operations:
- Data Plane: DoPut (Upload), DoGet (Download)
- Meta Plane: Search (Vector/Hybrid), ListFlights, GetFlightInfo, Snapshot
- Dual-port support (Data: 3000, Meta: 3001)
"""
import argparse
import json
import sys
import time
import uuid
import numpy as np
import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd

# Optional: Polars for nice table printing
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


def get_client(uri):
    print(f"Connecting to {uri}...")
    return flight.FlightClient(uri)


# =============================================================================
# Data Plane Operations (Data Port)
# =============================================================================

def command_put(args, data_client, meta_client):
    """Upload data to a dataset via DoPut."""
    name = args.dataset
    rows = args.rows
    dim = args.dim
    
    print(f"Generating {rows} vectors of dimension {dim} for dataset '{name}'...")
    
    # Generate synthetic data
    data = np.random.rand(rows, dim).astype(np.float32)
    tensor_type = pa.list_(pa.float32(), dim)
    flat_data = data.flatten()
    vectors = pa.FixedSizeListArray.from_arrays(flat_data, type=tensor_type)
    ids = pa.array(np.arange(rows), type=pa.int64())
    ts = pa.array([pd.Timestamp.now()] * rows, type=pa.timestamp("ns"))
    
    fields = [
        pa.field("id", pa.int64()),
        pa.field("vector", tensor_type),
        pa.field("timestamp", pa.timestamp("ns")),
    ]
    
    # Add text field for hybrid search testing
    if args.with_text:
        texts = [f"doc_{i} keyword_{i%10}" for i in range(rows)]
        fields.append(pa.field("text", pa.string()))
        table = pa.Table.from_arrays([ids, vectors, ts, pa.array(texts)], schema=pa.schema(fields))
    else:
        table = pa.Table.from_arrays([ids, vectors, ts], schema=pa.schema(fields))

    print(f"Uploading {rows} rows to '{name}'...")
    descriptor = flight.FlightDescriptor.for_path(name)
    writer, _ = data_client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.close()
    print("Upload complete.")


def command_get(args, data_client, meta_client):
    """Download data from a dataset via DoGet."""
    name = args.dataset
    print(f"Downloading dataset '{name}'...")
    
    query = {"name": name}
    ticket = flight.Ticket(json.dumps(query).encode("utf-8"))
    
    try:
        reader = data_client.do_get(ticket)
        table = reader.read_all()
        print(f"Retrieved {table.num_rows} rows.")
        if HAS_POLARS:
            print(pl.from_arrow(table))
        else:
            print(table.to_pandas().head())
    except flight.FlightError as e:
        print(f"Error: {e}")


# =============================================================================
# Meta Plane Operations (Meta Port)
# =============================================================================

def command_list(args, data_client, meta_client):
    """List available flights (datasets)."""
    print("Listing flights...")
    try:
        flights = meta_client.list_flights()
        found = False
        for info in flights:
            found = True
            path = info.descriptor.path[0].decode('utf-8') if info.descriptor.path else "Unknown"
            print(f"- {path} (Records: {info.total_records}, Bytes: {info.total_bytes})")
        if not found:
            print("No datasets found.")
    except Exception as e:
        print(f"Error listing flights: {e}")


def command_info(args, data_client, meta_client):
    """Get info for a specific flight."""
    name = args.dataset
    print(f"Getting info for '{name}'...")
    try:
        descriptor = flight.FlightDescriptor.for_path(name)
        info = meta_client.get_flight_info(descriptor)
        print(f"Schema: {info.schema}")
        print(f"Total Records: {info.total_records}")
        print(f"Total Bytes: {info.total_bytes}")
    except Exception as e:
        print(f"Error: {e}")


def command_search(args, data_client, meta_client):
    """Perform vector search via Meta Server DoAction."""
    name = args.dataset
    k = args.k
    query_vector = np.random.rand(args.dim).astype(np.float32).tolist()
    
    print(f"Searching '{name}' (k={k})...")
    
    request = {
        "dataset": name,
        "vector": query_vector,
        "k": k,
    }
    
    # Add text query for hybrid search
    if args.text_query:
        request["text_query"] = args.text_query
        request["alpha"] = args.alpha
        print(f"Hybrid search with text='{args.text_query}' alpha={args.alpha}")

    payload = json.dumps(request).encode("utf-8")
    
    try:
        # Search is a DoAction on Meta Server with type "VectorSearch"
        action = flight.Action("VectorSearch", payload)
        results = meta_client.do_action(action)
        
        for res in results:
            body = json.loads(res.body.to_pybytes())
            print(json.dumps(body, indent=2))
            
    except flight.FlightError as e:
        print(f"Search failed: {e}")


def command_snapshot(args, data_client, meta_client):
    """Trigger a snapshot via Meta Server DoAction."""
    print("Triggering snapshot...")
    request = {"action": "snapshot"} # payload structure dep on server, often empty or config
    
    try:
        # Server uses "force_snapshot" for manual trigger
        action = flight.Action("force_snapshot", b"")
        results = meta_client.do_action(action)
        for res in results:
            print(json.dumps(json.loads(res.body.to_pybytes()), indent=2))
    except Exception as e:
        print(f"Snapshot failed: {e}")


# =============================================================================
# Main Dispatch
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Longbow Ops Test CLI")
    
    # Global Connections
    parser.add_argument("--data-uri", default="grpc://0.0.0.0:3000", help="Data Server URI")
    parser.add_argument("--meta-uri", default="grpc://0.0.0.0:3001", help="Meta Server URI")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # PUT
    put_parser = subparsers.add_parser("put", help="Upload random data")
    put_parser.add_argument("--dataset", required=True, help="Dataset name")
    put_parser.add_argument("--rows", type=int, default=100, help="Number of rows")
    put_parser.add_argument("--dim", type=int, default=128, help="Vector dimension")
    put_parser.add_argument("--with-text", action="store_true", help="Include text column")
    
    # GET
    get_parser = subparsers.add_parser("get", help="Download dataset")
    get_parser.add_argument("--dataset", required=True, help="Dataset name")
    
    # LIST
    subparsers.add_parser("list", help="List all datasets")
    
    # INFO
    info_parser = subparsers.add_parser("info", help="Get dataset info")
    info_parser.add_argument("--dataset", required=True, help="Dataset name")

    # SEARCH
    search_parser = subparsers.add_parser("search", help="Vector/Hybrid search")
    search_parser.add_argument("--dataset", required=True, help="Dataset name")
    search_parser.add_argument("--dim", type=int, default=128, help="Vector dimension")
    search_parser.add_argument("--k", type=int, default=5, help="Top K results")
    search_parser.add_argument("--text-query", help="Text query for hybrid search")
    search_parser.add_argument("--alpha", type=float, default=0.5, help="Hybrid alpha (0=sparse, 1=dense)")

    # SNAPSHOT
    subparsers.add_parser("snapshot", help="Force database snapshot")

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        # Initialize appropriate clients based on command needs
        # For simplicity, init both (connection is lazy/lightweight)
        data_client = get_client(args.data_uri)
        meta_client = get_client(args.meta_uri)
        
        commands = {
            "put": command_put,
            "get": command_get,
            "list": command_list,
            "info": command_info,
            "search": command_search,
            "snapshot": command_snapshot,
        }
        
        func = commands.get(args.command)
        if func:
            func(args, data_client, meta_client)
            
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
