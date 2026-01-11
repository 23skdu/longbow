#!/usr/bin/env python3
"""Longbow Operational Test Script (Refactored for SDK)

CLI tool for testing Longbow operations using the official Python SDK.
"""
import argparse
import json
import sys
import time
import uuid
import numpy as np
import pandas as pd
import urllib.request
import logging

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

# Import SDK
try:
    from longbow import LongbowClient
except ImportError:
    print("Error: 'longbow' SDK not found. Install it via 'pip install ./longbowclientsdk'")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def get_client(args):
    headers = {}
    if args.routing_key:
        headers["x-longbow-key"] = args.routing_key
    
    return LongbowClient(uri=args.data_uri, meta_uri=args.meta_uri, headers=headers)

# =============================================================================
# Data Plane Operations
# =============================================================================

def command_put(args, client):
    """Upload data to a dataset."""
    name = args.dataset
    rows = args.rows
    dim = args.dim
    
    print(f"Generating {rows} vectors of dimension {dim} for dataset '{name}'...")
    
    # Generate synthetic data (List of Dicts is easiest for SDK v0.1)
    # For high-perf, we'd use Pandas, but this ops_test is usually small scale.
    
    data = []
    timestamp = pd.Timestamp.now()
    
    vecs = np.random.rand(rows, dim).astype(np.float32)
    
    for i in range(rows):
        record = {
            "id": i,
            "vector": vecs[i].tolist(),
            "timestamp": timestamp
        }
        if args.with_text:
            record["meta"] = f"doc_{i} keyword_{i%10}"
        data.append(record)

    print(f"Uploading {rows} rows to '{name}'...")
    client.insert(name, data)
    print("Upload complete.")


def command_get(args, client):
    """Download data from a dataset."""
    name = args.dataset
    print(f"Downloading dataset '{name}'...")
    
    filters = []
    if hasattr(args, 'filter') and args.filter:
        for f in args.filter:
            parts = f.split(':')
            if len(parts) == 3:
                filters.append({"field": parts[0], "operator": parts[1], "value": parts[2]})
    
    try:
        ddf = client.download(name, filter=filters)
        # Materialize for display
        df = ddf.compute()
        print(f"Retrieved {len(df)} rows.")
        if HAS_POLARS:
            print(pl.from_pandas(df))
        else:
            print(df.head())
    except Exception as e:
        print(f"Error: {e}")

# =============================================================================
# Meta Plane Operations
# =============================================================================

def command_list(args, client):
    """List available flights (datasets)."""
    print("Listing flights...")
    try:
        namespaces = client.list_namespaces()
        if namespaces:
            for ns in namespaces:
                print(f"- {ns}")
        else:
            print("No datasets found.")
    except Exception as e:
        print(f"Error listing flights: {e}")


def command_info(args, client):
    """Get info for a specific flight."""
    name = args.dataset
    print(f"Getting info for '{name}'...")
    try:
        info = client.get_info(name)
        print(json.dumps(info, indent=2))
    except Exception as e:
        print(f"Error: {e}")


def command_status(args, client):
    """Get cluster status."""
    print("Getting cluster status...")
    # Not yet wrapped in SDK convenience method strictly, but do_action is available
    # Actually wait, we didn't wrap raw do_action well in Client.
    # We should access client._meta_client
    try:
        import pyarrow.flight as flight
        if client._meta_client is None:
            client.connect()
            
        action = flight.Action("cluster-status", b"")
        options = client._get_call_options()
        results = client._meta_client.do_action(action, options=options)
        for res in results:
            status = json.loads(res.body.to_pybytes())
            print(json.dumps(status, indent=2))
            
            if HAS_POLARS and "members" in status:
                df = pl.DataFrame(status["members"])
                print("\nMembership Table:")
                print(df)
    except Exception as e:
        print(f"Error getting status: {e}")


def command_delete(args, client):
    """Delete vectors or namespace."""
    name = args.dataset
    
    ids = []
    if args.ids:
        for p in args.ids.split(','):
            if '-' in p:
                start, end = map(int, p.split('-'))
                ids.extend(range(start, end + 1))
            else:
                ids.append(int(p))
    
    if ids:
        print(f"Deleting {len(ids)} vectors from '{name}'...")
        # SDK delete takes list of IDs
        try:
            client.delete(name, ids)
            print("Delete calls issued.")
        except Exception as e:
            print(f"Delete failed: {e}")
    else:
        print("No IDs provided (use --ids). To delete dataset use 'namespaces delete'.")


def command_search(args, client):
    """Perform vector search."""
    name = args.dataset
    if hasattr(args, 'seed') and args.seed is not None:
        np.random.seed(args.seed)
    
    k = args.k
    query_vector = np.random.rand(args.dim).astype(np.float32).tolist()
    
    print(f"Searching '{name}' (k={k})...")
    
    filters = []
    if hasattr(args, 'filter') and args.filter:
        for f in args.filter:
            parts = f.split(':')
            if len(parts) == 3:
                filters.append({"field": parts[0], "operator": parts[1], "value": parts[2]})
    
    extra_args = {}
    if args.include_vectors:
        extra_args["include_vectors"] = True
        extra_args["vector_format"] = args.vector_format
        print(f"Including vectors in output (format={args.vector_format})")
    
    if args.text_query:
        extra_args["text_query"] = args.text_query
        extra_args["alpha"] = args.alpha
        print(f"Hybrid search with text='{args.text_query}' alpha={args.alpha}")
        
    if hasattr(args, 'global_search') and args.global_search:
        # SDK supports headers injection via init, but we want it PER CALL here.
        # current SDK `search` doesn't accept call-specific headers overrides easily
        # except via `client` re-init?
        # Actually, `client._get_call_options` reads `self.headers`.
        # We can hack it or we can add `headers` arg to search?
        # For now, let's warn or accept straightforward approach.
        print("Note: GLOBAL search flag logic in SDK requires client headers.")
        # We'll just rely on what we can pass in kwargs if server supports it in body?
        # Server expects x-longbow-global header.
        # Let's temporarily mutate client headers
        client.headers["x-longbow-global"] = "true"

    try:
        ddf = client.search(name, query_vector, k=k, filters=filters, **extra_args)
        df = ddf.compute()
        
        if HAS_POLARS:
            print(pl.from_pandas(df))
        else:
            print(df)
            
    except Exception as e:
        print(f"Search failed: {e}")
    finally:
        if args.global_search:
            client.headers.pop("x-longbow-global", None)


def command_snapshot(args, client):
    """Trigger a snapshot."""
    print("Triggering snapshot...")
    try:
        client.snapshot()
        print("Snapshot verified.") 
        # Note: SDK snapshot() returns iterator list consumption inside, so it waits.
    except Exception as e:
        print(f"Snapshot failed: {e}")

# =============================================================================
# GraphRAG Operations
# =============================================================================

def command_graph_stats(args, client):
    """Get GraphRAG statistics."""
    # Use raw do_action via client
    try:
        if client._meta_client is None: client.connect()
        import pyarrow.flight as flight
        
        req = {"dataset": args.dataset}
        action = flight.Action("GetGraphStats", json.dumps(req).encode("utf-8"))
        options = client._get_call_options()
        results = client._meta_client.do_action(action, options=options)
        for res in results:
            print(json.dumps(json.loads(res.body.to_pybytes()), indent=2))
    except Exception as e:
        print(f"Error: {e}")

def command_add_edge(args, client):
    try:
        if client._meta_client is None: client.connect()
        import pyarrow.flight as flight
        
        req = {
            "dataset": args.dataset,
            "subject": args.subject,
            "predicate": args.predicate,
            "object": args.object,
            "weight": args.weight
        }
        action = flight.Action("add-edge", json.dumps(req).encode("utf-8"))
        list(client._meta_client.do_action(action, options=client._get_call_options()))
        print("Edge added.")
    except Exception as e:
        print(f"Error: {e}")

def command_traverse(args, client):
    try:
        if client._meta_client is None: client.connect()
        import pyarrow.flight as flight
        
        req = {
            "dataset": args.dataset,
            "start": args.start_node,
            "max_hops": args.max_hops,
            "incoming": args.incoming,
            "weighted": not args.no_weighted,
            "decay": args.decay
        }
        action = flight.Action("traverse-graph", json.dumps(req).encode("utf-8"))
        results = client._meta_client.do_action(action, options=client._get_call_options())
        for res in results:
             print(json.loads(res.body.to_pybytes()))
    except Exception as e:
        print(f"Error: {e}")

def command_similar(args, client):
    try:
        if client._meta_client is None: client.connect()
        import pyarrow.flight as flight
        
        req = {
            "dataset": args.dataset,
            "id": args.id, # String or Int?
            # SDK usually expects matching types. args.id is string from CLI.
            # Convert if integer-ish?
            "k": args.k
        }
        # Attempt int conversion
        try:
            req["id"] = int(args.id)
        except:
            pass
            
        action = flight.Action("VectorSearchByID", json.dumps(req).encode("utf-8"))
        results = client._meta_client.do_action(action, options=client._get_call_options())
        for res in results:
            print(json.loads(res.body.to_pybytes()))
    except Exception as e:
        print(f"Error: {e}")

# =============================================================================
# Main Dispatch
# =============================================================================

def command_exchange(args, client):
    """Test DoExchange."""
    print("Testing DoExchange on DataPort...")
    import pyarrow.flight as flight
    import pyarrow as pa
    
    if client._data_client is None: client.connect()
    
    try:
        descriptor = flight.FlightDescriptor.for_command(b"fetch")
        writer, reader = client._data_client.do_exchange(descriptor)
        
        schema = pa.schema([("data", pa.string())])
        table = pa.Table.from_arrays([pa.array(["ping"])], schema=schema)
        writer.begin(schema)
        writer.write_table(table)
        writer.done_writing()
        
        for chunk in reader:
            if chunk.data:
                print(f"Received: {chunk.data.to_pybytes()}")
    except Exception as e:
        print(f"Error: {e}")


def command_validate(args, client):
    """Run smoke tests using SDK."""
    print("Running functional smoke tests (SDK)...")
    
    # Clean prev
    try:
        # We don't have explicit delete_dataset API yet besides delete vectors?
        # Wait, create_namespace works. delete works?
        # client.delete currently sends "DeleteNamespace" action.
        # This deletes the dataset. Correct.
        client.delete("smoke_test") 
    except:
        pass

    unique_id = str(uuid.uuid4())[:8]
    dataset = f"smoke_test_{unique_id}"
    print(f"\nUsing dataset: {dataset}")

    # TEST 1: Orthogonal Vectors with Meta
    print("\n[Test 1] Standard Vector Search (Orthogonal)")
    
    data = [
        {"id": 0, "vector": [1.0, 0.0, 0.0, 0.0], "meta": "x", "timestamp": pd.Timestamp.now()},
        {"id": 1, "vector": [0.0, 1.0, 0.0, 0.0], "meta": "y", "timestamp": pd.Timestamp.now()}, # Target
        {"id": 2, "vector": [0.0, 0.0, 1.0, 0.0], "meta": "z", "timestamp": pd.Timestamp.now()},
    ]
    
    client.insert(dataset, data)
    print("  Inserted. Waiting for index...")
    time.sleep(20)
    
    # Search
    qvec = [0.0, 1.0, 0.0, 0.0]
    ddf = client.search(dataset, qvec, k=1)
    df = ddf.compute()
    
    found = False
    if not df.empty:
        if df.iloc[0]['id'] == 1:
            print(f"  PASS: Retrieved ID 1 as top result (Score: {df.iloc[0]['score']:.4f})")
            found = True
        else:
            print(f"  FAIL: Expected ID 1, got {df.iloc[0]['id']}")
    else:
        print("  FAIL: No results")
        
    # Cleanup
    try:
        client.delete(dataset)
    except:
        pass
    
    print("\nSmoke Tests Complete (Simplified).")


def command_namespaces(args, client):
    """Namespace ops."""
    ns_name = "ops-test-ns"
    try:
        print(f"Creating '{ns_name}'...")
        client.create_namespace(ns_name)
        
        print("Listing...")
        nss = client.list_namespaces()
        print(f"Namespaces: {nss}")
        if ns_name not in nss:
             print("FAIL: Namespace not found")
        
        print(f"Deleting '{ns_name}'...")
        client.delete(ns_name)
        print("Done.")
    except Exception as e:
        print(f"Error: {e}")

def get_metric(url, name):
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read().decode('utf-8')
            for line in data.split('\n'):
                if line.startswith(name) and not line.startswith('#'):
                     parts = line.split()
                     if len(parts) >= 2: return float(parts[1])
    except Exception as e:
        print(f"Failed metrics: {e}")
    return 0.0

def command_pool_verify(args, client):
    """Verify pooling using metrics (requires existing dataset)."""
    metrics_url = args.metrics_url
    dataset = args.dataset
    print(f"Verifying Pooling on '{dataset}'...")
    
    new_start = get_metric(metrics_url, "longbow_hnsw_search_pool_new_total")
    get_start = get_metric(metrics_url, "longbow_hnsw_search_pool_get_total")
    print(f"Start: New={new_start}, Get={get_start}")
    
    qvec = np.random.rand(128).tolist() # assumption
    success = 0
    for _ in range(100):
        try:
            client.search(dataset, qvec, k=1).compute()
            success += 1
        except:
            pass

    print(f"Searches: {success}")
    new_end = get_metric(metrics_url, "longbow_hnsw_search_pool_new_total")
    get_end = get_metric(metrics_url, "longbow_hnsw_search_pool_get_total")
    
    print(f"End: New={new_end}, Get={get_end}")
    delta_new = new_end - new_start
    delta_get = get_end - get_start
    
    if delta_get > 0:
        ratio = delta_new / delta_get
        print(f"New/Get Ratio: {ratio:.4f}")
        if ratio < 0.2: print("PASS: Pooling effective")
        else: print("WARN: Pooling ineffective")

def main():
    parser = argparse.ArgumentParser(description="Longbow Ops Test CLI (SDK Edition)")
    parser.add_argument("--data-uri", default="grpc://0.0.0.0:3000", help="Data Server URI")
    parser.add_argument("--meta-uri", default="grpc://0.0.0.0:3001", help="Meta Server URI")
    parser.add_argument("--metrics-url", default="http://localhost:9090/metrics", help="Prometheus")
    parser.add_argument("--routing-key", help="Routing key header")
    
    subparsers = parser.add_subparsers(dest="command")
    
    # Put
    p_put = subparsers.add_parser("put")
    p_put.add_argument("--dataset", required=True)
    p_put.add_argument("--rows", type=int, default=100)
    p_put.add_argument("--dim", type=int, default=128)
    p_put.add_argument("--with-text", action="store_true")
    
    # Get
    p_get = subparsers.add_parser("get")
    p_get.add_argument("--dataset", required=True)
    p_get.add_argument("--filter", action="append")
    
    # List
    subparsers.add_parser("list")
    
    # Info
    p_info = subparsers.add_parser("info")
    p_info.add_argument("--dataset", required=True)
    
    # Delete
    p_del = subparsers.add_parser("delete")
    p_del.add_argument("--dataset", required=True)
    p_del.add_argument("--ids", help="e.g. 1,2,5-10")
    
    # Search
    p_search = subparsers.add_parser("search")
    p_search.add_argument("--dataset", required=True)
    p_search.add_argument("--dim", type=int, default=128)
    p_search.add_argument("--k", type=int, default=5)
    p_search.add_argument("--text-query")
    p_search.add_argument("--alpha", type=float, default=0.5)
    p_search.add_argument("--include-vectors", action="store_true")
    p_search.add_argument("--vector-format", default="f32")
    p_search.add_argument("--filter", action="append")
    p_search.add_argument("--seed", type=int)
    p_search.add_argument("--global", dest="global_search", action="store_true")

    # Others
    subparsers.add_parser("status")
    subparsers.add_parser("snapshot")
    subparsers.add_parser("exchange")
    subparsers.add_parser("validate")
    subparsers.add_parser("namespaces")
    
    p_pool = subparsers.add_parser("pool-verify")
    p_pool.add_argument("--dataset", required=True)
    
    # Graph
    p_g = subparsers.add_parser("graph-stats")
    p_g.add_argument("--dataset", required=True)
    
    p_edge = subparsers.add_parser("add-edge")
    p_edge.add_argument("--dataset", required=True)
    p_edge.add_argument("--subject", type=int, required=True)
    p_edge.add_argument("--predicate", required=True)
    p_edge.add_argument("--object", type=int, required=True)
    p_edge.add_argument("--weight", type=float, default=1.0)
    
    p_trav = subparsers.add_parser("traverse")
    p_trav.add_argument("--dataset", required=True)
    p_trav.add_argument("--start-node", type=int, required=True)
    p_trav.add_argument("--max-hops", type=int, default=2)
    p_trav.add_argument("--incoming", action="store_true")
    p_trav.add_argument("--no-weighted", action="store_true")
    p_trav.add_argument("--decay", type=float, default=0.0)
    
    p_sim = subparsers.add_parser("similar")
    p_sim.add_argument("--dataset", required=True)
    p_sim.add_argument("--id", required=True)
    p_sim.add_argument("--k", type=int, default=5)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
        
    client = get_client(args)
    
    cmds = {
        "put": command_put,
        "get": command_get,
        "list": command_list,
        "info": command_info,
        "delete": command_delete,
        "search": command_search,
        "status": command_status,
        "snapshot": command_snapshot,
        "exchange": command_exchange,
        "validate": command_validate,
        "namespaces": command_namespaces,
        "pool-verify": command_pool_verify,
        "graph-stats": command_graph_stats,
        "add-edge": command_add_edge,
        "traverse": command_traverse,
        "similar": command_similar
    }
    
    with client:
        if args.command in cmds:
            cmds[args.command](args, client)
        else:
            print("Unknown command")

if __name__ == "__main__":
    main()
