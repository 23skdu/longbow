# Unified Search & Discovery Guide

**Last Updated**: 2026-04-28

Longbow provides an integrated suite of search capabilities, from low-latency vector similarity to complex relational filtering and graph-based retrieval.

## Search Modes Overview

| Mode | Description | Python SDK |
|------|------------|-----------|
| **Dense** | HNSW-based vector similarity | `client.search()` |
| **Sparse** | BM25/Keyword matching | Built-in |
| **Filtered** | Metadata filtering | `filters=` parameter |
| **Hybrid** | RRF-fused dense + sparse | `client.search(alpha=0.5)` |
| **ByID** | Instant specific vector retrieval | `client.search_by_id()` |
| **Temporal** | Versioned time-travel queries | `client.temporal_search()` |
| **Geo-Spatial** | Radius/bounding box search | `client.geo_search()` |
| **GraphRAG** | Knowledge graph spreading | `client.recommend()` |
| **Learned Index** | Automatic index selection | Auto-enabled |
| **TurboQuant** | Compressed vector search | `vector_type="turboquant"` |

---

## 1. Dense Search (Vector Similarity)

HNSW-based vector similarity with sub-millisecond latency.

### Python SDK

```python
from longbow import LongbowClient

client = LongbowClient(uri="grpc://localhost:3000")
client.connect()

# Basic vector search
results = client.search(
    dataset="documents",
    vector=[0.1, 0.2, 0.3, ...],  # Query vector
    k=10  # Return top 10
)
# Returns DataFrame with: id, text, distance columns
```

### Supported Metrics

| Metric | Formula | Best For |
| :--- | :--- | :--- |
| **Euclidean (L2)** | `√(Σ(a[i] - b[i])²)` | Image search |
| **Cosine Distance** | `1.0 - (dot(a,b) / (\|\|a\|\|*\|\|b\|\|))` | Text embeddings |
| **Dot Product** | `-(Σ(a[i] * b[i]))` | MIPS, recommendations |

### IVF-OPQ Composite Index

For billion-scale datasets:

```python
client.create_namespace(
    name="billion_scale",
    dims=768,
    data_type="opq",      # Optimized Product Quantization
    nlist=1024,          # IVF clusters
    nprobe=64            # Clusters to search
)
```

---

## 2. Sparse Search (BM25)

Traditional full-text retrieval using inverted index.

**Algorithm**: BM25 with configurable $k_1$ and $b$ parameters.

**Use Case**: Exact keyword matching where semantic embeddings might be too "fuzzy".

---

## 3. Hybrid Search (RRF & Alpha Blending)

Combines Dense and Sparse retrieval.

### Reciprocal Rank Fusion (RRF)

$$RRF(doc) = \sum_{p \in paths} \frac{1}{k + rank_p(doc)}$$

### Alpha Blending

```python
results = client.search(
    dataset="documents",
    vector=[0.1, 0.2, ...],
    text_query="search terms",  # Combine with BM25
    alpha=0.7,  # 1.0 = dense, 0.0 = sparse
    k=10
)
```

---

## 4. Filtered Search

Metadata filtering using post-filtering.

```python
results = client.search(
    dataset="documents",
    vector=[0.1, 0.2, ...],
    filters=[
        {"field": "category", "op": "eq", "value": "tech"},
        {"field": "priority", "op": "gte", "value": 5},
    ],
    k=10
)
```

**Operators**: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `like`

---

## 5. ByID Search

O(1) instant retrieval of specific vectors.

```python
# Find neighbors of known vector
results = client.search_by_id(
    dataset="documents",
    id=12345,
    k=10  # Get 10 nearest neighbors
)

# Get single vector
vector = client.get_vector(
    dataset="documents", 
    id=12345
)
```

---

## 6. Temporal Search (Time-Travel)

Versioned discovery via snapshots.

```python
# As-Of search (state at specific time)
results = client.temporal_search(
    dataset="documents",
    search_type="as_of",
    timestamp=1700050000000000000,  # Unix nanoseconds
    k=10
)

# Range search (time window)
results = client.temporal_search(
    dataset="documents",
    search_type="range",
    start_time=1700000000000000000,
    end_time=1700100000000000000,
    k=10
)

# Sliding window (last N items)
results = client.temporal_search(
    dataset="documents",
    search_type="sliding_window",
    window_size=100,
    k=10
)

# Sliding window by duration
results = client.temporal_search(
    dataset="documents",
    search_type="sliding_window_time",
    duration="1h",  # "30m", "2h", "1d"
    k=10
)

# Version history
versions = client.temporal_version_history(
    dataset="documents",
    vector_id=12345
)
```

---

## 7. Geo-Spatial Search

Location-aware search using Quadtree index.

```python
# Radius search (Haversine distance)
results = client.geo_search(
    dataset="locations",
    center={"lat": 37.7749, "lon": -122.4194},
    radius_km=10,
    search_type="radius",
    k=10
)

# Bounding box search
results = client.geo_search(
    dataset="locations",
    box={"min_lat": 37.7, "max_lat": 37.8, "min_lon": -122.5, "max_lon": -122.4},
    search_type="box",
    k=10
)

# Hybrid (vector + geo)
results = client.geo_search(
    dataset="locations",
    center={"lat": 37.7749, "lon": -122.4194},
    radius_km=5,
    vector=[0.1, 0.2, ...],  # Combine with semantic search
    search_type="hybrid",
    k=10
)
```

---

## 8. GraphRAG (Knowledge Graph)

Dual-path: Spreading Activation + Knowledge Graph triples.

### Spreading Activation

```python
# Hybrid search with graph re-ranking
results = client.search(
    dataset="documents",
    vector=[0.1, 0.2, ...],
    alpha=0.7,  # Graph weight (1.0 = full graph, 0.0 = pure vector)
    depth=2,    # Graph traversal depth
    k=10
)
```

### Knowledge Graph

```python
# Add edges
client.add_edge(
    dataset="knowledge",
    subject=1,
    predicate="knows",
    object=2,
    weight=1.0
)

# Recommend (hybrid vector-graph)
results = client.recommend(
    dataset="documents",
    seed_ids=["doc_1", "doc_2"],
    alpha=0.5,  # Balance vector vs graph
    max_hops=2,
    k=10
)

# Traverse graph
results = client.traverse(
    dataset="knowledge",
    start=1,
    max_hops=2,
    decay=0.5
)

# PageRank centrality
scores = client.calculate_pagerank(dataset="knowledge")

# Community detection
communities = client.detect_communities(dataset="knowledge")
```

---

## 9. TurboQuant Search (Compressed)

Two-stage vector compression achieving 4-64x storage reduction.

### Usage

```python
# Create TurboQuant dataset
client.create_namespace(
    name="compressed",
    dims=768,
    data_type="turboquant",  # or "tq"
    turboquant_bits=4       # 2, 4, or 8 bits
)

# Search works the same
results = client.search(
    dataset="compressed",
    vector=[0.1, 0.2, ...],
    k=10
)
```

### Compression Ratios

| Bits | Compression | Typical Use |
|------|-------------|-------------|
| 2-bit | 16x | Archival |
| 4-bit | 8x | Standard |
| 8-bit | 4x | High recall |

---

## 10. Learned Index (Adaptive)

Automatic index selection using k-NN classifier.

### Feature Vector

11-dimensional features including:

- `DatasetSize` (Most discriminating)
- `QueryComplexity`
- `AvgVectorNorm`
- `IsFiltered` / `IsHybrid`

### Auto-Dispatch

System learns optimal weights over time via Fisher Linear Discriminant (LDA).

---

## 11. Global Distributed Search (Cluster-Wide)

Scatter-gather search across multiple Longbow nodes.

### Architecture

```
Query → Local HNSW Search → Scatter to Peers → Gather Raw Vectors → Global Sort → Merge (RRF) → Top-K
```

For Hybrid searches, the `GlobalSearchCoordinator` gathers the full `top-K` raw Dense and Sparse lists globally *before* applying Global Reciprocal Rank Fusion (RRF) to ensure mathematical correctness of the rank denominators.

### Python SDK

```python
# Automatic global search when peer nodes available
results = client.search(
    dataset="documents",
    vector=[0.1, 0.2, ...],
    k=10
    # If cluster peers exist, automatically scatter-gathers
)
```

### Global IDs

Each vector has a global ID across the cluster:

- **GlobalID**: Unique across all nodes (node_id << 32 | local_id)
- **LocalID**: Unique within single node

### Request Options

```python
# Force local-only search (skip scatter-gather)
results = client.search(
    dataset="documents",
    vector=[0.1, 0.2, ...],
    k=10,
    local_only=True  # Skip cluster peers
)

# Specify specific nodes
results = client.search(
    dataset="documents",
    vector=[0.1, 0.2, ...],
    k=10,
    nodes=["node-1", "node-2"]  # Specific peers only
)
```

### Cluster Metrics (Prometheus)

| Metric | Description |
|--------|-------------|
| `longbow_global_search_fanout_size` | Peers contacted per search |
| `longbow_global_search_partial_failures` | Failed peer queries |
| `longbow_global_search_duration_seconds` | Scatter-gather latency |
| `longbow_gossip_active_members` | Healthy cluster nodes |
| `longbow_global_rrf_latency_seconds` | Latency of global reciprocal rank fusion |
| `longbow_global_rrf_payload_bytes` | Payload elements passed to global RRF |

### CLI Benchmark

```bash
# Benchmark distributed search
python3 scripts/unified_benchmark.py \
    --mode cluster \
    --dims 768 \
    --counts 10000 \
    --peers 3
```

---

## 12. Resilience & Observability

### Circuit Breaker

- **Trip Conditions**: 10 consecutive failures
- **Cooldown**: 30-second reset

### Prometheus Metrics (Port 9090)

| Metric | Description |
|--------|-------------|
| `longbow_search_ops_total` | Throughput per mode |
| `longbow_search_duration_seconds` | Latency P50/P95/P99 |
| `longbow_vector_search_latency_seconds` | Search latency histogram |
| `longbow_turboquant_search_total` | TurboQuant search count |
| `longbow_learned_index_adaptations_total` | Adaptive index switches |

### Benchmark

```bash
# Run benchmark suite
python3 scripts/unified_benchmark.py \
    --mode dense,hybrid,filtered,byid,temporal,geo,graphrag,turboquant \
    --dims 768 \
    --counts 10000
```

> [!NOTE]
> For complete GraphRAG documentation, see [graphrag.md](graphrag.md).