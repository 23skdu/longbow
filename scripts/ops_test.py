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


def get_client(uri, routing_key=None):
    print(f"Connecting to {uri}...")
    # Add generic middleware/metadata if routing_key provided
    # However, pyarrow.flight.FlightClient doesn't take session-wide metadata easily in all versions.
    # We will pass it per-call.
    return flight.FlightClient(uri)

def get_options(args):
    """Generate call options including routing metadata."""
    if hasattr(args, 'routing_key') and args.routing_key:
        return flight.FlightCallOptions(headers=[
            (b"x-longbow-key", args.routing_key.encode("utf-8"))
        ])
    return flight.FlightCallOptions()


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
        # Use "meta" to align with perf_test.py and internal convention
        fields.append(pa.field("meta", pa.string()))
        table = pa.Table.from_arrays([ids, vectors, ts, pa.array(texts)], schema=pa.schema(fields))
    else:
        table = pa.Table.from_arrays([ids, vectors, ts], schema=pa.schema(fields))

    # NOTE: Schema: id (int64), vector (FixedSizeList<float32>[dim]), timestamp (Timestamp[ns]), meta (string)
    # The server strictly validates that NumColumns matches NumFields in schema.

    print(f"Uploading {rows} rows to '{name}'...")
    descriptor = flight.FlightDescriptor.for_path(name)
    
    options = get_options(args)
    writer, _ = data_client.do_put(descriptor, table.schema, options=options)
    writer.write_table(table)
    writer.close()
    print("Upload complete.")


def command_get(args, data_client, meta_client):
    """Download data from a dataset via DoGet."""
    name = args.dataset
    print(f"Downloading dataset '{name}'...")
    
    query = {"name": name}
    if hasattr(args, 'filter') and args.filter:
        filters = []
        for f in args.filter:
            parts = f.split(':')
            if len(parts) == 3:
                filters.append({"field": parts[0], "operator": parts[1], "value": parts[2]})
        query["filters"] = filters

    ticket = flight.Ticket(json.dumps(query).encode("utf-8"))
    
    try:
        options = get_options(args)
        reader = data_client.do_get(ticket, options=options)
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
        options = get_options(args)
        info = meta_client.get_flight_info(descriptor, options=options)
        print(f"Schema: {info.schema}")
        print(f"Total Records: {info.total_records}")
        print(f"Total Bytes: {info.total_bytes}")
    except Exception as e:
        print(f"Error: {e}")



def command_status(args, data_client, meta_client):
    """Get cluster status via Meta Server DoAction."""
    print("Getting cluster status...")
    try:
        action = flight.Action("cluster-status", b"")
        options = get_options(args)
        results = meta_client.do_action(action, options=options)
        for res in results:
            status = json.loads(res.body.to_pybytes())
            print(json.dumps(status, indent=2))
            
            # Print table for members if polars available
            if HAS_POLARS and "members" in status:
                df = pl.DataFrame(status["members"])
                print("\nMembership Table:")
                print(df)
                
    except Exception as e:
        print(f"Error getting status: {e}")


def command_delete(args, data_client, meta_client):
    """Delete vectors from a dataset via Meta Server DoAction."""
    name = args.dataset
    print(f"Deleting vectors from '{name}'...")

    # Parse IDs
    ids = []
    if args.ids:
        for p in args.ids.split(','):
            if '-' in p:
                start, end = map(int, p.split('-'))
                ids.extend(range(start, end + 1))
            else:
                ids.append(int(p))
    
    # DoAction "delete-vector" currently supports single ID per request on server
    # We will loop for now. Efficient batch deletion requires server update.
    count = 0
    errors = 0
    
    for vid in ids:
        payload = {
            "dataset": name,
            "vector_id": float(vid) # Server expects float64 for generic interface, cast to uint32 internal
        }
        try:
            action = flight.Action("delete-vector", json.dumps(payload).encode("utf-8"))
            results = list(meta_client.do_action(action))
            # Just consuming
            count += 1
            if count % 100 == 0:
                print(f"Deleted {count} vectors...")
        except Exception as e:
            print(f"Failed to delete ID {vid}: {e}")
            errors += 1
            
    print(f"Deletion complete. Success: {count}, Errors: {errors}")


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
    
    if hasattr(args, 'filter') and args.filter:
        filters = []
        for f in args.filter:
            parts = f.split(':')
            if len(parts) == 3:
                filters.append({"field": parts[0], "operator": parts[1], "value": parts[2]})
        request["filters"] = filters
    
    # Add text query for hybrid search
    if args.text_query:
        request["text_query"] = args.text_query
        request["alpha"] = args.alpha
        print(f"Hybrid search with text='{args.text_query}' alpha={args.alpha}")

    payload = json.dumps(request).encode("utf-8")
    
    # Global Search Metadata
    options = get_options(args)
    if hasattr(args, 'global_search') and args.global_search:
        print("Performing GLOBAL Distributed Search (x-longbow-global=true)")
        # Append to existing headers if any (FlightCallOptions are immutable-ish, construct new?)
        # pyarrow options is list of tuples.
        # Check if get_options returned valid object or if we need to extend it.
        # But get_options creates new FlightCallOptions. 
        # Easier to just modify get_options or construct here.
        # Let's inspect get_options logic: it uses args.routing_key.
        
        headers = []
        if hasattr(args, 'routing_key') and args.routing_key:
            headers.append((b"x-longbow-key", args.routing_key.encode("utf-8")))
        
        headers.append((b"x-longbow-global", b"true"))
        options = flight.FlightCallOptions(headers=headers)
    elif args.local:
        # Local metadata logic if needed (or just request body)
        pass # Request body handles it ("local_only": true)

    
    try:
        # Search is a DoAction on Meta Server with type "VectorSearch"
        action = flight.Action("VectorSearch", payload)
        options = get_options(args)
        results = meta_client.do_action(action, options=options)
        
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


def command_exchange(args, data_client, meta_client):
    """Test DoExchange for DataPort connectivity."""
    print("Testing DoExchange on DataPort...")
    descriptor = flight.FlightDescriptor.for_command(b"fetch") # Trigger sync logic

    try:
        # DoExchange is a bidirectional stream
        writer, reader = data_client.do_exchange(descriptor)
        
        # Send a dummy batch
        schema = pa.schema([("data", pa.string())])
        table = pa.Table.from_arrays([pa.array(["ping"])], schema=schema)
        writer.begin(schema)
        writer.write_table(table)
        writer.done_writing()
        
        # Read response (Ack)
        for chunk in reader:
            if chunk.data:
                print(f"Received: {chunk.data.to_pybytes()}")
            if chunk.app_metadata:
                print(f"Metadata: {chunk.app_metadata}")
            
    except flight.FlightError as e:
        print(f"DoExchange failed: {e}")
    except Exception as e:
        print(f"Error: {e}")


def command_validate(args, data_client, meta_client):
    """Run full validation suite."""
    print("Running full validation...")
    
    # 0. Cleanup existing
    try:
        action = flight.Action("delete-dataset", json.dumps({"dataset": "validate_test"}).encode("utf-8"))
        list(meta_client.do_action(action))
    except:
        pass

    # 1. Put
    unique_id = str(uuid.uuid4())[:8]
    args.dataset = f"validate_test_{unique_id}"
    args.rows = 100
    args.dim = 4
    args.with_text = True
    command_put(args, data_client, meta_client)
    time.sleep(1) # Allow for indexing

    # 2. Get with Filter
    print("\n[Validation] Testing DoGet with Filter...")
    # Filter: id > 50
    # Note: Filter format supported by parser: "field=value" or json?
    # Based on store implementation: TicketQuery json
    filters = [{"field": "id", "operator": ">", "value": "50"}]
    query = {"name": args.dataset, "filters": filters}
    ticket = flight.Ticket(json.dumps(query).encode("utf-8"))
    reader = data_client.do_get(ticket)
    table = reader.read_all()
    print(f"DoGet Filtered rows: {table.num_rows}")
    if table.num_rows == 0:
        print("FAIL: No rows returned for filter id > 50")
    elif table.num_rows == 49: # IDs 51..99 = 49 rows
        print("PASS: Rows returned correctly (49)")
    else:
        print(f"WARN: Unexpected row count {table.num_rows}")

    # 3. Vector Search with Filter
    print("\n[Validation] Testing VectorSearch with Filter...")
    # Using 'operator' instead of 'op' for filter struct in zero_alloc_parser?
    # Checked zero_alloc_parser.go: struct Filter { Field, Operator, Value } json:"operator"
    # But DoGet logic might map "op" to "operator" manually? 
    # DoGet uses TicketQuery which has Filters []Filter.
    # So both should use "operator".
    filters = [{"field": "id", "operator": "<", "value": "10"}]
    # Random query vector of dim 4
    qvec = [0.1, 0.2, 0.3, 0.4]
    
    req = {
        "dataset": args.dataset,
        "vector": qvec,
        "k": 5,
        "filters": filters
    }
    action = flight.Action("VectorSearch", json.dumps(req).encode("utf-8"))
    results = meta_client.do_action(action)
    found_count = 0
    for res in results:
        body = json.loads(res.body.to_pybytes())
        ids = body.get("ids", [])
        print(f"Search IDs: {ids}")
        found_count += len(ids)
        # Verify IDs are < 10
        for id_val in ids:
            if id_val >= 10:
                print(f"FAIL: Found ID {id_val} which is >= 10")
        
    if found_count > 0:
        print("PASS: Search returned filtered results")
    else:
        print("WARN: Search returned no results (might be valid if random vectors far apart)")

    # 4. DoExchange
    print("\n[Validation] Testing DoExchange...")
    command_exchange(args, data_client, meta_client)
    print("\nValidation Complete.")

def command_namespaces(args, data_client, meta_client):
    """Test Namespace operations."""
    print("Testing Namespace operations...")
    ns_name = "ops-test-ns"
    
    # 1. Create
    print(f"Creating namespace '{ns_name}'...")
    try:
        req = {"name": ns_name}
        action = flight.Action("CreateNamespace", json.dumps(req).encode("utf-8"))
        list(meta_client.do_action(action))
        print("Created.")
    except Exception as e:
        print(f"Create failed: {e}")

    # 2. List
    print("Listing namespaces...")
    try:
        action = flight.Action("ListNamespaces", b"")
        for res in meta_client.do_action(action):
            body = json.loads(res.body.to_pybytes())
            print(f"Namespaces: {body}")
            if ns_name not in body.get("namespaces", []):
                print(f"FAIL: Namespace '{ns_name}' not found in list")
    except Exception as e:
        print(f"List failed: {e}")

    # 3. Count
    print("Checking counts...")
    try:
        action = flight.Action("GetTotalNamespaceCount", b"")
        for res in meta_client.do_action(action):
            print(f"Total Count: {json.loads(res.body.to_pybytes())}")
    except Exception as e:
        print(f"Count failed: {e}")

    # 4. Delete
    print(f"Deleting namespace '{ns_name}'...")
    try:
        req = {"name": ns_name}
        action = flight.Action("DeleteNamespace", json.dumps(req).encode("utf-8"))
        list(meta_client.do_action(action))
        print("Deleted.")
    except Exception as e:
        print(f"Delete failed: {e}")

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
    # Add filter argument support
    get_parser.add_argument("--filter", action="append", help="Filter: field:op:value")

    # DELETE
    del_parser = subparsers.add_parser("delete", help="Delete vectors")
    del_parser.add_argument("--dataset", required=True, help="Dataset name")
    del_parser.add_argument("--ids", required=True, help="Comma separated IDs or ranges (1,2,5-10)")
    
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
    search_parser.add_argument("--local", action="store_true", help="Force local-only search")
    search_parser.add_argument("--global", dest="global_search", action="store_true", help="Force GLOBAL distributed search")
    search_parser.add_argument("--alpha", type=float, default=0.5, help="Hybrid alpha (0=sparse, 1=dense)")
    search_parser.add_argument("--filter", action="append", help="Filter: field:op:value")

    # SNAPSHOT
    subparsers.add_parser("snapshot", help="Force database snapshot")

    # STATUS
    subparsers.add_parser("status", help="Get cluster status")

    # EXCHANGE
    subparsers.add_parser("exchange", help="Test DoExchange")

    # VALIDATE
    subparsers.add_parser("validate", help="Run full validation")

    # NAMESPACES
    subparsers.add_parser("namespaces", help="Test Namespace Operations")
    
    # GLOBAL options
    parser.add_argument("--routing-key", help="Explicit routing key (x-longbow-key metadata)")

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
            "delete": command_delete,
            "list": command_list,
            "info": command_info,
            "search": command_search,
            "snapshot": command_snapshot,
            "status": command_status,
            "exchange": command_exchange,
            "validate": command_validate,
            "namespaces": command_namespaces,
        }
        
        func = commands.get(args.command)
        if func:
            func(args, data_client, meta_client)
            
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
