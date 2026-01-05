#!/usr/bin/env python3
"""
Example demonstrating Longbow's pluggable distance metrics.

This script shows how to:
1. Create datasets with different distance metrics
2. Insert the same vectors into each dataset
3. Search and compare results across metrics
"""

import pyarrow as pa
import pyarrow.flight as flight
import numpy as np


def create_test_vectors(n=100, dims=128):
    """Generate random test vectors."""
    np.random.seed(42)
    vectors = np.random.randn(n, dims).astype(np.float32)
    
    # Normalize half of them (for cosine/dot product comparison)
    for i in range(n // 2, n):
        norm = np.linalg.norm(vectors[i])
        if norm > 0:
            vectors[i] = vectors[i] / norm
    
    return vectors


def create_schema_with_metric(metric_name, dims=128):
    """Create Arrow schema with specified distance metric."""
    return pa.schema([
        pa.field('id', pa.string()),
        pa.field('vector', pa.list_(pa.float32(), dims))
    ], metadata={
        'longbow.metric': metric_name
    })


def insert_vectors(client, collection_name, vectors, metric):
    """Insert vectors into a collection."""
    print(f"\n📝 Inserting {len(vectors)} vectors into '{collection_name}' (metric: {metric})")
    
    schema = create_schema_with_metric(metric, dims=vectors.shape[1])
    
    # Create table
    ids = [f"vec_{i}" for i in range(len(vectors))]
    table = pa.table({
        'id': ids,
        'vector': vectors.tolist()
    }, schema=schema)
    
    # Insert via DoPut
    descriptor = flight.FlightDescriptor.for_path(collection_name)
    writer, _ = client.do_put(descriptor, schema)
    writer.write_table(table)
    writer.close()
    
    print(f"✅ Inserted {len(vectors)} vectors")


def search_collection(client, collection_name, query_vector, k=5):
    """Search a collection and return results."""
    # Create search ticket (simplified JSON format)
    import json
    ticket_data = {
        "collection": collection_name,
        "k": k,
        "query": query_vector.tolist()
    }
    ticket = flight.Ticket(json.dumps(ticket_data).encode())
    
    # Execute search
    reader = client.do_get(ticket)
    results = reader.read_all()
    
    return results


def main():
    print("=" * 70)
    print("Longbow Distance Metrics Demo")
    print("=" * 70)
    
    # Connect to Longbow
    print("\n🔌 Connecting to Longbow at grpc://localhost:3000...")
    client = flight.FlightClient("grpc://localhost:3000")
    
    # Generate test data
    print("\n🎲 Generating test vectors...")
    vectors = create_test_vectors(n=100, dims=128)
    print(f"   Generated {len(vectors)} vectors of dimension {vectors.shape[1]}")
    print(f"   First 50 vectors: random")
    print(f"   Last 50 vectors: normalized (unit length)")
    
    # Test each metric
    metrics = ['euclidean', 'cosine', 'dot_product']
    collections = {}
    
    for metric in metrics:
        collection_name = f"demo_{metric}"
        collections[metric] = collection_name
        insert_vectors(client, collection_name, vectors, metric)
    
    # Create a query vector (normalized)
    print("\n🔍 Creating query vector (normalized)...")
    query = vectors[75]  # Use one of the normalized vectors
    query_norm = np.linalg.norm(query)
    print(f"   Query vector norm: {query_norm:.4f}")
    
    # Search each collection
    print("\n" + "=" * 70)
    print("Search Results Comparison")
    print("=" * 70)
    
    for metric in metrics:
        print(f"\n📊 {metric.upper()} Distance:")
        print("-" * 70)
        
        try:
            results = search_collection(client, collections[metric], query, k=5)
            
            # Display results
            if results.num_rows > 0:
                ids = results.column('id').to_pylist()
                scores = results.column('score').to_pylist()
                
                for i, (id_val, score) in enumerate(zip(ids, scores), 1):
                    # For dot product, show both negative (stored) and positive (actual similarity)
                    if metric == 'dot_product':
                        actual_sim = -score
                        print(f"   {i}. {id_val:10s}  distance={score:8.4f}  similarity={actual_sim:8.4f}")
                    else:
                        print(f"   {i}. {id_val:10s}  distance={score:8.4f}")
            else:
                print("   No results found")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Analysis
    print("\n" + "=" * 70)
    print("Analysis")
    print("=" * 70)
    print("""
Key Observations:

1. **Euclidean Distance**:
   - Measures absolute distance in vector space
   - Sensitive to vector magnitude
   - Best for: spatial data, image embeddings

2. **Cosine Distance**:
   - Measures angle between vectors (ignores magnitude)
   - Returns 1.0 - cosine_similarity
   - Best for: text embeddings, semantic search

3. **Dot Product**:
   - Stored as negative for HNSW minimization
   - Higher similarity = more negative distance
   - Best for: MIPS, recommendation systems

4. **For Normalized Vectors**:
   - Cosine and Dot Product give similar rankings
   - Dot Product is slightly faster (no sqrt)
   - Euclidean still considers magnitude differences
""")
    
    print("\n✨ Demo complete!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
