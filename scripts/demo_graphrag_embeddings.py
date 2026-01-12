
import logging
import sys
import time
import numpy as np
import pandas as pd
from typing import List

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("demo_graphrag")

try:
    from longbow import LongbowClient, LongbowQueryError
except ImportError:
    print("Error: 'longbow' SDK not found. Install it via 'pip install -e longbowclientsdk'")
    sys.exit(1)

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("Error: 'sentence-transformers' not found. Install it via 'pip install sentence-transformers'")
    sys.exit(1)


# --- Configuration ---
DATASET_NAME = "graphrag_demo_v1"
MODEL_NAME = 'all-MiniLM-L6-v2' # 384 dimensions
URI = "grpc://localhost:3000"

# --- Content Generation ---
# Some random content about AI and Databases to be relevant
DOCUMENTS = [
    {"id": 1, "text": "Vector databases are specialized databases for storing embedding vectors.", "category": "tech"},
    {"id": 2, "text": "GraphRAG combines knowledge graphs with retrieval augmented generation.", "category": "tech"},
    {"id": 3, "text": "Embeddings capture semantic meaning of text in a high-dimensional space.", "category": "ai"},
    {"id": 4, "text": "PostgreSQL is a powerful open-source relational database system.", "category": "tech"},
    {"id": 5, "text": "Transformers revolutionized natural language processing tasks.", "category": "ai"},
    {"id": 6, "text": "HNSW is an efficient algorithm for approximate nearest neighbor search.", "category": "algo"},
    {"id": 7, "text": "Semantic search allows finding documents based on meaning rather than keywords.", "category": "ai"},
    {"id": 8, "text": "Knowledge graphs represent entities and relationships as nodes and edges.", "category": "tech"},
    {"id": 9, "text": "Longbow is a high-performance vector database written in Go.", "category": "product"},
    {"id": 10, "text": "Python is a popular programming language for data science and AI.", "category": "tech"},
]

def generate_embeddings(model, texts: List[str]) -> List[List[float]]:
    logger.info(f"Generating embeddings for {len(texts)} documents using {MODEL_NAME}...")
    # Encode returns numpy array
    embeddings = model.encode(texts)
    return embeddings.tolist()

def main():
    logger.info("Initializing GraphRAG Demo...")
    
    # 1. Load Model
    logger.info("Loading Sentence Transformer model...")
    model = SentenceTransformer(MODEL_NAME)
    
    # 2. Prepare Data
    texts = [doc["text"] for doc in DOCUMENTS]
    vectors = generate_embeddings(model, texts)
    
    data_rows = []
    for i, doc in enumerate(DOCUMENTS):
        data_rows.append({
            "id": doc["id"],
            "vector": vectors[i],
            "text": doc["text"],       # Top level for potential text search
            "category": doc["category"], # Top level for filtering
            "metadata": {"source": "demo"} # Keep extra meta
        })
        
    df = pd.DataFrame(data_rows)
    logger.info(f"Prepared DataFrame with {len(df)} rows.")

    # 3. Initialize Client
    client = LongbowClient(uri=URI, meta_uri=URI.replace("3000", "3001"))
    
    # 4. Cleanup & Create Namespace
    try:
        current_ns = client.list_namespaces()
        if DATASET_NAME in current_ns:
            logger.info(f"Dataset {DATASET_NAME} exists. Deleting...")
            client.delete(DATASET_NAME)
            time.sleep(2)
        
        # Let insert auto-create the dataset with the correct schema
        # client.create_namespace(DATASET_NAME) 
    except Exception as e:
        logger.warning(f"Namespace setup issue: {e}")

    # 5. Ingest
    logger.info("Ingesting data...")
    client.insert(DATASET_NAME, df)
    
    logger.info("Waiting for indexing (sleep 10s)...")
    time.sleep(10)
    
    # 6. Verify Search Operations
    logger.info("--- 6.1 Vector Search ---")
    query_text = "How do vector databases work?"
    query_vec = model.encode([query_text])[0].tolist()
    
    results = client.search(DATASET_NAME, query_vec, k=3)
    res_df = results.compute()
    print(f"Query: '{query_text}'")
    for _, row in res_df.iterrows():
        print(f"  [{row['score']:.4f}] ID {row['id']}: {row.get('text', 'N/A')}")

    logger.info("\n--- 6.2 Filtered Search (category='ai') ---")
    filters = [{"field": "category", "op": "Eq", "value": "ai"}]
    results = client.search(DATASET_NAME, query_vec, k=3, filters=filters)
    res_df = results.compute()
    print(f"Query: '{query_text}' (Filter: category=ai)")
    for _, row in res_df.iterrows():
        print(f"  [{row['score']:.4f}] ID {row['id']}: {row.get('text', 'N/A')}")

    logger.info("\n--- 6.3 Search By ID ---")
    target_id = 9 # Longbow
    res = client.search_by_id(DATASET_NAME, target_id, k=3)
    print(f"Neighbors for ID {target_id} (Longbow):")
    # Response is a dict with 'results': [...]
    if 'results' in res:
        for item in res['results']:
             print(f"  [{item['score']:.4f}] ID {item['id']}")
    else:
        print("  No results found.")

    logger.info("\n--- 6.4 Hybrid Search ---")
    # Hybrid search uses 'alpha' parameter (0.0=dense, 1.0=sparse/text). 
    # Current Longbow might simplify this or just pass params.
    # Let's pass alpha=0.5 and text_query
    results = client.search(DATASET_NAME, query_vec, k=3, alpha=0.5, text_query="vector database")
    res_df = results.compute()
    print(f"Hybrid Query: '{query_text}' + keyword 'vector database'")
    for _, row in res_df.iterrows():
        print(f"  [{row['score']:.4f}] ID {row['id']}: {row.get('text', 'N/A')}")

    # 7. Verify GraphRAG Support
    logger.info("\n--- 7. GraphRAG Verification ---")
    
    # Add Manual Edges (Semantic Relationships)
    # Longbow (9) -> Vector DB (1)
    # Vector DB (1) -> Embeddings (3)
    # GraphRAG (2) -> Knowledge Graph (8)
    
    logger.info("Adding Graph Edges...")
    client.add_edge(DATASET_NAME, 9, "related_to", 1, weight=0.9)
    client.add_edge(DATASET_NAME, 1, "uses", 3, weight=0.8)
    client.add_edge(DATASET_NAME, 2, "enhances", 8, weight=0.85)

    # Helper to traverse
    def print_traversal(start_node):
        logger.info(f"Traversing from Node {start_node}...")
        path_results = client.traverse(DATASET_NAME, start_node, max_hops=2)
        # Result format: List[Dict(id, distance/score, ...)]
        # Actually server returns FlightInfo or Stream? 
        # client.traverse implementation returns list of dicts.
        print(f"Traversal Results (Start: {start_node}):")
        for p in path_results:
             # Structure might be opaque, just print what we get
             print(f"  {p}")

    print_traversal(9) # Should find 1, and then 3?
    
    logger.info("Getting Graph Stats...")
    stats = client.get_graph_stats(DATASET_NAME)
    print(f"Graph Stats: {stats}")

    logger.info("Demo Completed Successfully.")

if __name__ == "__main__":
    main()
