# Unified Search & Discovery Guide

Longbow provides an integrated suite of search capabilities, from low-latency vector similarity to complex relational filtering and graph-based retrieval. This guide covers everything from basic distance metrics to advanced GraphRAG workflows.

## Search Modes Overview

1. **Dense Search**: HNSW-based vector similarity (Zero-Copy).
2. **Sparse Search**: BM25/Keyword matching via inverted index.
3. **Filtered Search**: Metadata filtering using post-filtering or SQL-based CTEs.
4. **Hybrid Search**: RRF-fused results combining dense and sparse signals.
5. **ByID Search**: Instant retrieval of specific vectors and their neighbors.
6. **Temporal Search**: Versioned discovery via As-Of and Range queries.
7. **Graph Discovery**: Multi-hop pathfinding and spreading activation re-ranking.
8. **Adaptive Indexing**: Automated index selection using Learned Indexes.

---

## 1. Dense Search (Vector Similarity)

Dense search uses hand-optimized HNSW (Hierarchical Navigable Small Worlds) indexes for sub-millisecond similarity retrieval.

### Zero-Copy Data Plane

Longbow implements a **Zero-Copy** data plane using Apache Arrow. Vectors are accessed directly from memory-mapped files during HNSW traversal, eliminating heap allocations and significantly reducing GC pressure.

### Supported Metrics

| Metric | Formula | Best For |
| :--- | :--- | :--- |
| **Euclidean (L2)** | `√(Σ(a[i] - b[i])²)` | Image search, magnitude-sensitive data. |
| **Cosine Distance** | `1.0 - (dot(a,b) / (\|\|a\|\|*\|\|b\|\|))` | Text embeddings, semantic similarity. |
| **Dot Product** | `-(Σ(a[i] * b[i]))` | MIPS, pre-normalized recommendation systems. |

---

## 2. Sparse Search (BM25 & Keywords)

Sparse search provides traditional full-text retrieval using a highly concurrent Inverted Index.

- **Algorithm**: BM25 (Best Matching 25) with configurable $k_1$ and $b$ parameters.
- **Tokenization**: Standard WordPiece tokenization with support for custom stop-word filtering.
- **Use Case**: Exact keyword matching where semantic embeddings might be too "fuzzy" (e.g., searching for specific part numbers or rare medical terms).

---

## 3. Hybrid Search (RRF & Alpha Blending)

Hybrid search combines the strengths of Dense and Sparse retrieval.

### Reciprocal Rank Fusion (RRF)

Longbow uses RRF to merge results from multiple search paths without requiring normalized scores.
$$RRF(doc) = \sum_{p \in \text{paths}} \frac{1}{k + \text{rank}_p(doc)}$$

### Alpha Blending

For advanced users, `alpha` blending allows manual weighting between signals:

- `alpha = 1.0`: Pure Dense search.
- `alpha = 0.0`: Pure Sparse search.
- `alpha = 0.5`: Balanced Hybrid search.

---

## 4. Filtered Search (SQL & CTEs)

Longbow supports complex metadata filtering using both JSON-based filters and SQL-like Common Table Expressions (CTEs).

### Filter Operators

- **Comparison**: `==`, `!=`, `>`, `<`, `>=`, `<=`
- **Set Logic**: `IN`, `NOT IN`
- **Boolean**: `AND`, `OR`, `NOT`

### Common Table Expressions (CTEs)

Define temporary sets to filter another dataset. For example, selecting "top products from active vendors".

```json
{
  "ctes": [{
    "name": "active_vendors",
    "search": { "dataset": "vendors", "filters": [{"field": "status", "operator": "==", "value": "active"}] }
  }],
  "dataset": "products",
  "filters": [{"field": "vendor_id", "operator": "IN", "value": "active_vendors"}]
}
```

---

## 5. ByID Search & Neighborhoods

ByID search is an $O(1)$ operation that retrieves a specific vector by its unique identifier and optionally its nearest neighbors in the graph.

- **Primary Key Lookup**: Instant metadata and vector retrieval.
- **Neighbor Discovery**: Use `K` to find similar items starting from a known entity without providing a new query vector.

---

## 6. Temporal Search (Time-Travel)

Temporal search leverages versioned snapshots of the data plane to allow searching through time.

### Query Types

- **As-Of**: "What were the nearest neighbors of this vector as of Jan 1st?"
- **Range**: Find all changes or states for a vector within a time window.
- **Sliding Window**: Aggregate similarity statistics over a moving temporal window.

---

## 7. Geo-Spatial Search

Location-aware search using a Quadtree spatial index. Every vector can carry a `GeoPoint` (lat/lon).

### Radius & Bounding Box

- **SearchRadius**: Haversine distance-based culling within N kilometers.
- **SearchBox**: Precise rectangular region intersection.

### Hybrid Geo+Vector

Score = $W_{geo} \times GeoScore + W_{vec} \times VectorScore$. This allows finding "Restaurants near me that are semantically similar to 'Sushi'".

---

## 8. GraphRAG & Reasoning

GraphRAG overlays a knowledge graph on top of vector space, enabling relationship-aware discovery and multi-hop reasoning.

### Spreading Activation Re-ranking

1. **Seeds**: Initial search results become active nodes.
2. **Propagation**: Relevance "mass" spreads through edges with hop-based decay.
3. **Final Ranking**: Scores blend Vector Similarity and Graph Centrality (Closeness).

### Recommendation API

The `Recommend` API uses graph-based walk strategies (Random Walk with Restart) to find items that are structurally similar to a set of seed IDs.

---

## 9. Adaptive Index Selection (Learned Index)

Longbow automatically selects the best ANN index type (HNSW, IVF-PQ, DiskANN) for each query at runtime using a **k-NN classifier**.

### Feature-Based Dispatch

The classifier uses an 11-dimensional feature vector, including:

- `DatasetSize` (Most discriminating)
- `QueryComplexity`
- `AvgVectorNorm`
- `IsFiltered` / `IsHybrid`

### Fisher Linear Discriminant (LDA)

The system learns optimal feature weights over time by maximizing between-class variance between different index performances, ensuring the selector adapts to your specific hardware and data distribution.

---

## 10. Resilience & Observability

### Circuit Breaker

To maintain stability, search operations are protected by a Circuit Breaker:

- **Trip Conditions**: 10 consecutive failures.
- **Cooldown**: 30-second automated reset.

### Prometheus Metrics

Exposed on port `9090`:

- `longbow_search_ops_total`: Throughput per mode.
- `longbow_search_duration_seconds`: Latency P50/P95/P99.
- `longbow_learned_index_adaptations_total`: Tracking adaptive switches.
