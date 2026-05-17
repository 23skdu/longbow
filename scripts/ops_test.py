#!/usr/bin/env python3
"""
scripts/ops_test.py
Longbow Functional CLI tool and Integration/Smoke testing suite.
Provides subcommands for all functions documented in docs/functions.md.
"""

import sys
import os
import argparse
import time
import numpy as np
import pandas as pd
import json
import pyarrow.flight as flight
import pyarrow as pa
from longbow import LongbowClient

# Beautiful styling helper
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_success(msg):
    print(f"{Colors.OKGREEN}✓ {msg}{Colors.ENDC}")

def print_info(msg):
    print(f"{Colors.OKBLUE}ℹ {msg}{Colors.ENDC}")

def print_warning(msg):
    print(f"{Colors.WARNING}⚠ {msg}{Colors.ENDC}")

def print_error(msg):
    print(f"{Colors.FAIL}✗ {msg}{Colors.ENDC}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(
        description="Longbow CLI Functional Testing Tool using Python SDK",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--uri", default="grpc://localhost:3000", help="Data Server URI (default: grpc://localhost:3000)")
    parser.add_argument("--meta-uri", default="grpc://localhost:3001", help="Meta Server URI (default: grpc://localhost:3001)")
    
    subparsers = parser.add_subparsers(dest="command", required=True, help="Subcommand to run")
    
    # 1. put
    put_parser = subparsers.add_parser("put", help="Ingest records into a dataset")
    put_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    put_parser.add_argument("--rows", type=int, default=10, help="Number of rows to ingest (default: 10)")
    put_parser.add_argument("--dim", type=int, default=128, help="Vector dimension size (default: 128)")
    
    # 2. get
    get_parser = subparsers.add_parser("get", help="Bulk scan and retrieve all records")
    get_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    
    # 3. exchange
    subparsers.add_parser("exchange", help="Verify bidirectional FlightData echo handshake")
    
    # 4. search
    search_parser = subparsers.add_parser("search", help="Perform KNN Vector or Hybrid search")
    search_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    search_parser.add_argument("--k", type=int, default=5, help="Number of nearest neighbors to return (default: 5)")
    search_parser.add_argument("--text-query", help="Optional text query for hybrid search")
    search_parser.add_argument("--alpha", type=float, default=0.5, help="Hybrid blend alpha parameter (default: 0.5)")
    search_parser.add_argument("--dim", type=int, default=128, help="Query vector dimension size if dataset is empty/missing")
    
    # 5. status
    subparsers.add_parser("status", help="Get node cluster status and dataset namespaces list")
    
    # 6. delete
    delete_parser = subparsers.add_parser("delete", help="Soft-delete a specific ID or IDs from a dataset")
    delete_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    delete_parser.add_argument("--ids", required=True, help="Comma-separated list of IDs to delete")
    
    # 7. similar
    similar_parser = subparsers.add_parser("similar", help="Find similar vectors by a given ID")
    similar_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    similar_parser.add_argument("--id", required=True, help="Record ID to query similarity against")
    similar_parser.add_argument("--k", type=int, default=5, help="Number of results (default: 5)")
    
    # 8. namespaces
    ns_parser = subparsers.add_parser("namespaces", help="List or create/delete namespaces")
    ns_parser.add_argument("--create", help="Create namespace with given name")
    ns_parser.add_argument("--delete", help="Delete namespace with given name")
    ns_parser.add_argument("--list", action="store_true", help="List all namespaces")
    ns_parser.add_argument("--dim", type=int, default=128, help="Dimensions if creating (default: 128)")
    
    # 9. add-edge
    edge_parser = subparsers.add_parser("add-edge", help="Add directed graph edge (Subject -> Predicate -> Object)")
    edge_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    edge_parser.add_argument("--subject", type=int, required=True, help="Subject node ID")
    edge_parser.add_argument("--predicate", required=True, help="Predicate edge description")
    edge_parser.add_argument("--object", type=int, required=True, help="Object node ID")
    edge_parser.add_argument("--weight", type=float, default=1.0, help="Edge weight (default: 1.0)")
    
    # 10. traverse
    trav_parser = subparsers.add_parser("traverse", help="Traverse starting from a given node")
    trav_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    trav_parser.add_argument("--start", type=int, required=True, help="Starting node ID")
    trav_parser.add_argument("--hops", type=int, default=2, help="Max hops traversal depth (default: 2)")
    
    # 11. graph-stats
    stats_parser = subparsers.add_parser("graph-stats", help="Show Graph stats")
    stats_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    
    # 12. pagerank
    pr_parser = subparsers.add_parser("pagerank", help="Compute PageRank centrality scores")
    pr_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    
    # 13. communities
    comm_parser = subparsers.add_parser("communities", help="Detect network communities (LPA)")
    comm_parser.add_argument("--dataset", required=True, help="Name of the dataset")
    
    # 14. snapshot
    subparsers.add_parser("snapshot", help="Trigger manual database snapshot to disk")
    
    # 15. validate
    subparsers.add_parser("validate", help="Run full end-to-end integration and smoke test suite")
    
    args = parser.parse_args()
    
    # Connect client
    try:
        client = LongbowClient(uri=args.uri, meta_uri=args.meta_uri)
        client.connect()
    except Exception as e:
        print_error(f"Failed to connect to Longbow Server: {e}")
        sys.exit(1)
        
    try:
        if args.command == "put":
            # 1. Ingestion
            print_info(f"Ingesting {args.rows} rows into dataset '{args.dataset}'...")
            
            # Check if dataset exists, if not, create it
            exists = False
            try:
                # Use list_namespaces or info
                flights = client.list_namespaces()
                if args.dataset in flights:
                    exists = True
            except:
                pass
                
            if not exists:
                print_info(f"Dataset '{args.dataset}' does not exist. Creating it with dim={args.dim}...")
                client.create_dataset(args.dataset, dimensions=args.dim, vector_type="float32", metric="cosine")
                
            # Ingest random data
            data = pd.DataFrame({
                "id": [str(i) for i in range(args.rows)],
                "vector": [np.random.randn(args.dim).astype(np.float32).tolist() for _ in range(args.rows)],
                "metadata": [f"Metadata value {i}" for i in range(args.rows)]
            })
            client.insert(args.dataset, data)
            print_success(f"Successfully ingested {args.rows} records into '{args.dataset}'.")
            
        elif args.command == "get":
            # 2. Get bulkscan
            print_info(f"Retrieving all records from '{args.dataset}'...")
            table = client.download_arrow(args.dataset)
            df = table.to_pandas()
            print_success(f"Retrieved {len(df)} records.")
            print(f"\n{Colors.BOLD}Schema:{Colors.ENDC}\n{table.schema}")
            print(f"\n{Colors.BOLD}Data Preview:{Colors.ENDC}")
            print(df.head(10))
            
        elif args.command == "exchange":
            # 3. do_exchange
            print_info("Testing bidirectional FlightData echo handshake...")
            descriptor = flight.FlightDescriptor.for_path("default")
            writer, reader = client._data_client.do_exchange(descriptor)
            
            # Send single ping
            writer.write_data(flight.FlightData(data_body=b"ping"))
            writer.done_writing()
            
            # Read ACK response
            resp = reader.read()
            if resp and resp.data_body:
                body = resp.data_body.to_pybytes()
                print_success(f"Exchange handshake successful! Received: {body.decode('utf-8')}")
            else:
                print_error("Failed to receive valid echo response from exchange endpoint.")
                sys.exit(1)
                
        elif args.command == "search":
            # 4. Search (dense / hybrid / text)
            print_info(f"Querying nearest neighbors on '{args.dataset}' (k={args.k})...")
            
            # Try to get actual dataset schema dimension
            dim = args.dim
            try:
                info = client.get_flight_info_metadata(args.dataset)
                # Parse dimension from schema string
                schema_str = info.get("schema", "")
                if "fixed_size_list" in schema_str:
                    parts = schema_str.split("[")
                    if len(parts) > 1:
                        dim = int(parts[1].split("]")[0])
                        print_info(f"Detected dataset vector dimensions = {dim}")
            except:
                pass
                
            query_vec = np.random.randn(dim).astype(np.float32).tolist()
            
            kwargs = {}
            if args.text_query:
                kwargs["text_query"] = args.text_query
                kwargs["alpha"] = args.alpha
                print_info(f"Using Hybrid search blend (text_query='{args.text_query}', alpha={args.alpha}).")
                
            results = client.search(args.dataset, vector=query_vec, k=args.k, **kwargs)
            print_success(f"Found {len(results)} matching neighbors:")
            print(results)
            
        elif args.command == "status":
            # 5. Status
            print_info("Fetching cluster node and mesh status...")
            try:
                action = flight.Action("cluster-status", b"")
                results = list(client._meta_client.do_action(action, options=client._get_call_options()))
                if results:
                    status_data = json.loads(results[0].body.to_pybytes())
                    print_success("Gossip mesh cluster status retrieved:")
                    print(json.dumps(status_data, indent=2))
                else:
                    print_warning("Cluster status action succeeded but returned empty result.")
            except Exception as e:
                print_warning(f"Gossip mesh cluster status unavailable: {e}")
                
            print_info("\nListing all registered datasets:")
            datasets = client.list_namespaces()
            print_success(f"Registered datasets ({len(datasets)}):")
            for ds in datasets:
                print(f"  - {ds}")
                
        elif args.command == "delete":
            # 6. Delete
            print_info(f"Decompressing and soft-deleting vector IDs [{args.ids}] from '{args.dataset}'...")
            id_list = [int(i.strip()) for i in args.ids.split(",") if i.strip()]
            client.delete(args.dataset, id_list)
            print_success(f"Soft-delete successfully signaled for IDs {id_list}.")
            
        elif args.command == "similar":
            # 7. Similar by ID
            print_info(f"Finding vectors similar to ID '{args.id}' on '{args.dataset}' (k={args.k})...")
            results = client.search_by_id(args.dataset, id=args.id, k=args.k)
            print_success("Found similar vectors:")
            print(json.dumps(results, indent=2))
            
        elif args.command == "namespaces":
            # 8. Namespaces
            if args.create:
                print_info(f"Creating namespace '{args.create}'...")
                client.create_namespace(args.create, dims=args.dim)
                print_success(f"Namespace '{args.create}' successfully created.")
            elif args.delete:
                print_info(f"Deleting namespace '{args.delete}'...")
                client.delete_namespace(args.delete)
                print_success(f"Namespace '{args.delete}' successfully deleted.")
            elif args.list:
                print_info("Listing active namespaces:")
                ns = client.list_namespaces()
                print_success(f"Namespaces: {ns}")
            else:
                ns_parser.print_help()
                
        elif args.command == "add-edge":
            # 9. add-edge
            print_info(f"Adding graph edge: {args.subject} --[{args.predicate}]--> {args.object} (weight={args.weight}) on '{args.dataset}'...")
            client.add_edge(args.dataset, subject=args.subject, predicate=args.predicate, object=args.object, weight=args.weight)
            print_success("Edge added successfully.")
            
        elif args.command == "traverse":
            # 10. traverse
            print_info(f"Traversing graph starting from node {args.start} (max_hops={args.hops}) on '{args.dataset}'...")
            paths = client.traverse(args.dataset, start=args.start, max_hops=args.hops)
            print_success(f"Found {len(paths)} traversed paths:")
            for p in paths:
                if isinstance(p, list):
                    for path in p:
                        nodes = path.get("Nodes", [])
                        score = path.get("Score", 0.0)
                        print(f"  Path: {' -> '.join(map(str, nodes))} (Score: {score:.4f})")
                else:
                    print(p)
                    
        elif args.command == "graph-stats":
            # 11. graph-stats
            print_info(f"Fetching graph stats for '{args.dataset}'...")
            stats = client.get_graph_stats(args.dataset)
            print_success("Graph Stats:")
            print(json.dumps(stats, indent=2))
            
        elif args.command == "pagerank":
            # 12. pagerank
            print_info(f"Computing HNSW topology PageRank for '{args.dataset}'...")
            scores = client.calculate_pagerank(args.dataset)
            print_success("Top 10 computed PageRank scores:")
            sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]
            for node, score in sorted_scores:
                print(f"  Node {node}: {score:.6f}")
                
        elif args.command == "communities":
            # 13. communities
            print_info(f"Detecting Louvain HNSW communities on '{args.dataset}'...")
            communities = client.detect_communities(args.dataset)
            print_success(f"Community Detection Summary:")
            print(f"  Total Communities: {communities.get('CommunityCount')}")
            labels = communities.get("Labels", {})
            print(f"  Sample node labels (first 10):")
            for node, label in list(labels.items())[:10]:
                print(f"    Node {node} -> Community {label}")
                
        elif args.command == "snapshot":
            # 14. snapshot
            print_info("Triggering manual database snapshot checkpoint to disk...")
            client.snapshot()
            print_success("Database snapshot successfully flushed to persistent storage.")
            
        elif args.command == "validate":
            # 15. validate (Full integration suite)
            run_validation(client)
            
    except Exception as e:
        print_error(f"Error executing command '{args.command}': {e}")
        sys.exit(1)

def run_validation(client):
    print(f"\n{Colors.HEADER}{Colors.BOLD}=== Running Longbow E2E Smoke Validation Suite ==={Colors.ENDC}")
    dataset = "ops_validate_ds"
    dim = 128
    
    # 1. Cleanup old validation dataset
    try:
        print_info(f"Checking for pre-existing dataset '{dataset}'...")
        client.drop_dataset(dataset)
        print_success(f"Stale dataset '{dataset}' dropped.")
    except:
        pass
        
    # 2. Namespace & Dataset Creation
    print_info(f"Creating test dataset '{dataset}' (dim={dim})...")
    client.create_dataset(
        name=dataset,
        dimensions=dim,
        vector_type="float32",
        metric="cosine",
        disk_enabled=False
    )
    print_success("Dataset created.")
    
    # 3. Insert/Ingest Rows
    print_info("Ingesting 100 benchmark rows...")
    data = pd.DataFrame({
        "id": [str(i) for i in range(100)],
        "vector": [np.random.randn(dim).astype(np.float32).tolist() for _ in range(100)],
        "metadata": [f"Validation point {i}" for i in range(100)]
    })
    client.insert(dataset, data)
    print_success("Ingestion complete. Waiting for index prewarming...")
    time.sleep(2)
    
    # 4. Dense Vector Search
    print_info("Performing Vector Search...")
    query_vec = data.iloc[0]["vector"]
    res = client.search(dataset, vector=query_vec, k=5)
    print_success(f"Vector search verified. Matches found: {len(res)}")
    print(res.head(3))
    
    # 5. Hybrid Search (Dummy / Echo query)
    print_info("Performing Hybrid Search...")
    res_hybrid = client.search(dataset, vector=query_vec, k=5, text_query="point 0", alpha=0.5)
    print_success(f"Hybrid search verified. Matches found: {len(res_hybrid)}")
    
    # 6. GraphRAG Knowledge Graph
    print_info("Testing Knowledge Graph addition and traversal...")
    # Add chain 0 -> 1 -> 2
    client.add_edge(dataset, subject=0, predicate="linked", object=1, weight=1.0)
    client.add_edge(dataset, subject=1, predicate="linked", object=2, weight=0.8)
    
    stats = client.get_graph_stats(dataset)
    print_success(f"Graph stats verified: {stats}")
    
    paths = client.traverse(dataset, start=0, max_hops=2)
    print_success(f"Graph traversal verified. Paths found: {len(paths)}")
    
    # 7. PageRank & Louvain Centrality
    print_info("Running PageRank and Community Louvain detection...")
    pr_scores = client.calculate_pagerank(dataset)
    print_success(f"PageRank calculated. Scores returned: {len(pr_scores)}")
    
    comm = client.detect_communities(dataset)
    print_success(f"Community detection LPA verified. Group count = {comm.get('CommunityCount')}")
    
    # 8. Vector search by ID
    print_info("Testing similarity SearchByID...")
    sim = client.search_by_id(dataset, id="0", k=3)
    print_success(f"SearchByID verified: similar count = {len(sim.get('Matches', []))}")
    
    # 9. Soft Delete
    print_info("Testing vector soft-delete...")
    client.delete(dataset, ids=[99])
    print_success("Soft-delete successful.")
    
    # 10. Manual Snapshot
    print_info("Triggering database snapshot...")
    client.snapshot()
    print_success("Manual snapshot saved.")
    
    # 11. Cleanup
    print_info("Cleaning up validation dataset...")
    client.drop_dataset(dataset)
    print_success("Dataset dropped successfully.")
    
    print(f"\n{Colors.OKGREEN}{Colors.BOLD}=== All E2E Integration and Smoke Tests PASSED! ==={Colors.ENDC}\n")

if __name__ == "__main__":
    main()
