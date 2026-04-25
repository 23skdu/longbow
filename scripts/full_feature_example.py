#!/usr/bin/env python3
"""
Comprehensive example demonstrating Longbow 0.1.9 features:
- 2-bit TurboQuant
- PID-based Autonomous efSearch Tuning
- Direct Arrow-backed Persistent MMap
- Distributed GraphRAG Expansion
- MetaServer Operations (GetFlightInfo)
"""

import sys
import os
import time
import numpy as np
import pandas as pd
from longbow import LongbowClient

def run_example():
    print("=== Longbow 0.1.9 Feature Showcase ===")
    
    # 1. Initialization
    client = LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")
    dataset = "showcase_ds"
    dim = 384
    
    # 2. Create Dataset with 2-bit TurboQuant and MMap Persistence
    print(f"\n[1] Creating dataset {dataset} with 2-bit TQ and MMap...")
    try:
        client.drop_dataset(dataset)
    except:
        pass
        
    client.create_dataset(
        name=dataset,
        dimensions=dim,
        vector_type="turboquant",
        turboquant_bits=2,  # Extreme 64x compression
        disk_enabled=True,   # Enable MMap-backed persistence
        metric="cosine"
    )
    
    # 3. Ingest Data
    print("\n[2] Ingesting vectors...")
    count = 1000
    data = pd.DataFrame({
        "id": [str(i) for i in range(count)],
        "vector": [np.random.randn(dim).astype(np.float32).tolist() for _ in range(count)],
        "metadata": [f"Record {i}" for i in range(count)]
    })
    client.insert(dataset, data)
    
    print("Waiting for background indexing...")
    time.sleep(3)
    
    # 4. Search with Autonomous PID Tuning
    print("\n[3] Searching with PID efSearch tuning...")
    query_vec = np.random.randn(dim).astype(np.float32).tolist()
    
    # ef_search_pid=True triggers the autonomous controller
    results = client.search(
        dataset, 
        vector=query_vec, 
        k=10, 
        ef_search_pid=True, 
        recall_target=0.98
    )
    print(f"Search results (Top 5):\n{results.head(5)}")
    
    # 5. Distributed GraphRAG Expansion
    print("\n[4] Distributed GraphRAG Expansion...")
    # Expand node 0, 1, 2
    expansion = client.graph_rag_expand(dataset, node_ids=[0, 1, 2])
    print(f"Neighbors for nodes [0, 1, 2]: {expansion}")
    
    # 6. MetaServer Operations
    print("\n[5] Retrieving MetaServer FlightInfo...")
    info = client.get_flight_info_metadata(dataset)
    print(f"Dataset Stats: {info}")
    
    # 7. Persistence Check (MMap)
    print("\n[6] MMap Persistence Check...")
    # Snapshots are handled automatically, but we can force one
    client.snapshot()
    print("Snapshot triggered. Adjacency lists are now mmap-ready on disk.")
    
    print("\n=== Showcase Complete ===")

if __name__ == "__main__":
    run_example()
