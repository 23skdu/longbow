#!/usr/bin/env python3
"""
Functional test for Longbow GraphRAG features.
Tests Triple Ingestion, Traversal, Spreading Activation, PageRank, and Community Detection.
"""

import sys
import os
import time
import numpy as np
import pandas as pd
from longbow import LongbowClient

def test_graphrag_features():
    print("=== Longbow GraphRAG Functional Test ===")
    
    # Connect to local instance (adjust ports if needed)
    client = LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")
    dataset = "graph_test_ds"
    dim = 128
    
    # 1. Cleanup and Setup
    try:
        client.search(dataset, vector=[0.0]*dim, k=1)
        print(f"Dataset {dataset} exists.")
    except Exception:
        print(f"Initializing dataset {dataset}...")
        
    # 2. Ingest Vectors (as nodes)
    print("Ingesting 100 vectors as graph nodes...")
    data = pd.DataFrame({
        "id": [str(i) for i in range(100)],
        "vector": [np.random.randn(dim).astype(np.float32).tolist() for _ in range(100)],
        "metadata": [f"Node {i}" for i in range(100)]
    })
    client.insert(dataset, data)
    
    # Wait for indexing
    print("Waiting for indexing...")
    time.sleep(2)
    
    # 3. Test Knowledge Graph (Triples)
    print("\nTesting Knowledge Graph Triples...")
    # Add a chain: 0 -> 1 -> 2
    client.add_edge(dataset, subject=0, predicate="linked_to", object=1, weight=1.0)
    client.add_edge(dataset, subject=1, predicate="linked_to", object=2, weight=0.8)
    # Add a star: 0 -> 10, 0 -> 11, 0 -> 12
    for i in [10, 11, 12]:
        client.add_edge(dataset, subject=0, predicate="mentions", object=i, weight=0.5)
        
    stats = client.get_graph_stats(dataset)
    print(f"Graph Stats: {stats}")
    assert stats["edge_count"] >= 5
    
    # 4. Test Traversal
    print("\nTesting Graph Traversal...")
    paths = client.traverse(dataset, start=0, max_hops=2)
    print(f"Found {len(paths)} paths from Node 0")
    for p in paths:
        if isinstance(p, list): # Traversal returns list of path objects
            for path in p:
                nodes = path.get("Nodes", [])
                score = path.get("Score", 0.0)
                print(f"  Path: {' -> '.join(map(str, nodes))} (Score: {score:.4f})")
    
    # 5. Test GraphRAG Spreading Activation (Re-ranking)
    print("\nTesting Vector Search Re-ranking (Spreading Activation)...")
    query_vec = data.iloc[0]["vector"] # Query for Node 0
    
    # ANN search (baseline)
    res_ann = client.search(dataset, vector=query_vec, k=5)
    print("ANN Top 5 IDs:", res_ann["id"].tolist())
    
    # GraphRAG search (alpha=0.8, depth=2)
    res_graph = client.search(dataset, vector=query_vec, k=10, graph_alpha=0.8, graph_depth=2)
    print("GraphRAG Top 10 IDs:", res_graph["id"].tolist())
    
    # Check if neighbors (1, 2, 10, 11, 12) are boosted
    boosted = set(["1", "2", "10", "11", "12"])
    found_boosted = [id for id in res_graph["id"].tolist() if id in boosted]
    print(f"Boosted neighbors found in top 10: {found_boosted}")

    # 6. Test Advanced Analytics
    print("\nTesting Advanced Analytics...")
    
    print("Calculating PageRank...")
    pr_scores = client.calculate_pagerank(dataset, max_iterations=20)
    if pr_scores:
        # Sort by score descending
        top_pr = sorted(pr_scores.items(), key=lambda x: x[1], reverse=True)[:5]
        print(f"Top 5 PageRank nodes: {top_pr}")
    else:
        print("PageRank returned no scores (is the graph connected?)")
        
    print("\nDetecting Communities...")
    communities = client.detect_communities(dataset)
    print(f"Community detection results: Count={communities.get('CommunityCount')}")
    labels = communities.get("Labels", {})
    if labels:
        # Check first 5 node labels
        sample_labels = {k: labels[k] for k in list(labels.keys())[:5]}
        print(f"Sample node labels: {sample_labels}")

    print("\n=== GraphRAG Functional Test Complete ===")

if __name__ == "__main__":
    try:
        test_graphrag_features()
    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        sys.exit(1)
