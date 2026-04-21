# Unified Search & Discovery Guide

Longbow provides an integrated suite of search capabilities, from low-latency vector similarity to complex relational filtering and graph-based retrieval. This guide covers everything from basic distance metrics to advanced GraphRAG workflows.

## Search Modes Overview

1.  **Dense Search**: HNSW-based vector similarity (Zero-Copy).
2.  **Sparse Search**: BM25/Keyword matching via inverted index.
3.  **Filtered Search**: Metadata filtering using post-filtering or SQL-based CTEs.
4.  **Hybrid Search**: RRF-fused results combining dense and sparse signals.
5.  **Graph Discovery**: Multi-hop pathfinding and spreading activation re-ranking.

---

## 1. Vector Similarity & Distance Metrics

Longbow supports multiple distance metrics, each hand-optimized with SIMD kernels (AVX-512, AVX2, NEON) for maximum performance.

### Supported Metrics

| Metric | Formula | Best For |
| :--- | :--- | :--- |
| **Euclidean (L2)** | `√(Σ(a[i] - b[i])²)` | Image search, magnitude-sensitive data. |
| **Cosine Distance**| `1.0 - (dot(a,b) / (||a||*||b||))` | Text embeddings, semantic similarity. |
| **Dot Product** | `-(Σ(a[i] * b[i]))` | MIPS, pre-normalized recommendation systems. |

> [!NOTE]
> For **Dot Product**, Longbow minimizes the *negative* dot product to align with HNSW traversal logic. The most similar vectors will have the most negative distance scores.

### Global SIMD Performance

All metrics leverage platform-specific optimizations, providing significantly higher throughput than generic implementations:
- **x86_64**: AVX-512 (16 floats/cycle) or AVX2 (8 floats/cycle).
- **ARM64**: NEON (4 floats/cycle).

---

## 2. Advanced SQL Filtering (CTEs & Subqueries)

Longbow extends the search protocol with relational capabilities, allowing complex multi-stage filtering within a single query ticket.

### Common Table Expressions (CTEs)
Define temporary sets to filter another dataset. For example, selecting "top products from active vendors".

```json
{
  "with": [{
    "name": "active_vendors",
    "search": { "dataset": "vendors", "filters": [{"field": "status", "operator": "==", "value": "active"}] }
  }],
  "name": "products",
  "filters": [{"field": "vendor_id", "operator": "IN", "value": "active_vendors"}]
}
```

### Subqueries
Inline filters that dynamically execute a secondary search.
```json
{
  "name": "orders",
  "filters": [{
    "field": "user_id", "operator": "IN",
    "subquery": { "name": "active_users", "search": { "dataset": "users", "k": 100 } }
  }]
}
```

---

## 3. The Hybrid Search Pipeline

The `HybridSearchPipeline` integrates retrieval, fusion, and re-ranking into a single execution flow.

1.  **Retrieval**: Parallel HNSW (Dense) and BM25 (Sparse) execution.
2.  **Fusion**: Combines results using **Reciprocal Rank Fusion (RRF)**.
    - $RRF\_score(doc) = \sum \frac{1}{k + rank(doc)}$
3.  **Re-ranking**: Refines results using Cross-Encoders or Heuristics.

### Multi-Stage Reranking
Longbow supports secondary re-ranking for higher precision:
- **Heuristic**: Weighted blend of vector distance (70%) and keyword matches (30%).
- **Transformer Cross-Encoder**: High-fidelity reranking using transformer models.
  - **Native/ONNX**: Optimized execution on CUDA/Metal GPUs.
  - **Wazero (Cross-Platform)**: Sandboxed WASM execution for zero-dependency environments.
  - **WordPiece Tokenization**: Integrated subword encoding ensuring semantic continuity between the model and the search index.

---

## 4. Quantized Search (Memory Optimization)

When using **SQ8** or **BQ** quantization, Longbow optimizes the traversal path to minimize floating-point operations.

- **SQ8 Search**: Uses 8-bit integer SIMD kernels for distance calculations, falling back to a **refined** FP32 distance only for top-k candidates to maintain precision.
- **BQ (Binary) Search**: Uses `Popcount` and `XOR` logic for 32x faster distance scoring on massive datasets.
- **Auto-Refinement**: Configurable `LONGBOW_HNSW_REFINEMENT_FACTOR` automatically re-scores quantized candidates with original vectors during the final ranking phase.

---

## 4. Graph Discovery & GraphRAG

GraphRAG overlays a knowledge graph on top of vector space, enabling relationship-aware discovery.

### Spreading Activation Re-ranking
Adjusts vector similarity scores based on node connectivity:
1.  **Seeds**: Initial search results become active nodes.
2.  **Propagation**: Mass spreads through edges (with hop-based decay).
3.  **Rescoring**: Final scores blend Vector Relevance and Graph Centrality.

```python
# Python SDK Example
results = client.search(
    "my_dataset", 
    vector=[...], 
    k=10, 
    graph_alpha=0.5, # 50/50 Vector vs Graph
    graph_depth=2    # 2-hop expansion
)
```

### Graph Navigation API
Find paths between entities using the `Navigate` API, which supports **BFS**, **A***, and **Parallel BFS** strategies.

```go
query := store.NavigatorQuery{
    StartID: 101, TargetID: 202, MaxHops: 5,
}
path, _ := index.Navigate(ctx, query)
```

---

## 5. Resilience: The Circuit Breaker

To maintain stability under load, search operations are protected by a **Circuit Breaker**:
- **Trip Conditions**: 10 consecutive failures.
- **Cooldown**: 30-second automated reset.
- **Fail-Fast**: Returns `Unavailable` during the trip period to allow the backend to shed load and recover.
