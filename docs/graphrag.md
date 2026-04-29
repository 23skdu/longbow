# GraphRAG: Dual-Path Graph Architecture

**Last Updated**: 2026-04-28
**Related Docs**: [functions.md](functions.md) · [vectorsearch.md](vectorsearch.md) · [comparison.md](comparison.md)

---

## Overview

Longbow implements a dual-path GraphRAG architecture that combines implicit vector relationships with explicit knowledge graph triples. This hybrid approach allows for both semantic similarity spreading and strict entity-relationship traversal.

### Architecture Paths

| Path | Type | Use Case |
|------|------|----------|
| **Spreading Activation** | Vector-based | Semantic re-ranking using HNSW Layer 0 |
| **Knowledge Graph** | Triple-based (SPOW) | Explicit entity relationships |

---

## 1. Knowledge Graph (Explicit Edges)

The `GraphStore` maintains explicit directed edges between `VectorID`s.

### Edge Model

```go
type Edge struct {
    Subject  VectorID  // Source node
    Predicate string   // Relationship type (e.g., "knows", "owns", "cites")
    Object   VectorID  // Target node  
    Weight   float32   // Edge weight (0.0-1.0)
}
```

### Storage

- **Forward/Backward Maps**: Adjacency lists for efficient traversal
- **Dictionary Encoding**: Predicates are dictionary-encoded for memory efficiency
- **CSR Format**: Automatically flattens to Compressed Sparse Row for GPU acceleration

### Python SDK Usage

```python
from longbow import LongbowClient

client = LongbowClient(uri="grpc://localhost:3000")
client.connect()

# Add edges
client.add_edge(
    dataset="knowledge",
    subject=1,
    predicate="knows",
    object=2,
    weight=1.0
)

client.add_edge(
    dataset="knowledge", 
    subject=2,
    predicate="works_at",
    object=3,
    weight=0.8
)

# Graph traversal
results = client.traverse(
    dataset="knowledge",
    start=1,
    max_hops=2,
    decay=0.5  # decay factor per hop
)

# Get graph statistics
stats = client.get_graph_stats(dataset="knowledge")
# Returns: {"nodes": 1000, "edges": 5000, "predicates": ["knows", "works_at"]}
```

### Arrow Flight Actions

| Action | Description | Parameters |
|--------|-------------|------------|
| `add-edge` | Adds an explicit relationship | `subject`, `predicate`, `object`, `weight` |
| `traverse-graph` | BFS traversal from a start node | `start`, `max_hops`, `incoming`, `weighted` |
| `GraphRAGExpand` | Distributed expansion for multiple nodes | `node_ids`, `max_hops` |
| `GetGraphStats` | Basic graph metadata | `dataset` |

---

## 2. Spreading Activation Algorithm

When `alpha > 0` is passed in search, the following re-ranking occurs:

1. **ANN Seed Query**: Standard HNSW search retrieves top-K seeds
2. **BFS Expansion**: Seeds expand outward up to `graph_depth` hops
3. **Activation Propagation**:
   ```
   activation[neighbor] += activation[parent] * alpha^hop * EdgeWeight
   ```
4. **Re-ranking**: Nodes sorted by accumulated activation

### Python SDK Usage

```python
# Hybrid search with graph spreading
results = client.search(
    dataset="documents",
    vector=[0.1, 0.2, ...],
    k=10,
    alpha=0.7,  # 1.0 = full graph, 0.0 = pure vector
    depth=2      # graph traversal depth
)

# Pure graph-based recommendation
results = client.recommend(
    dataset="documents",
    seed_ids=["doc_1", "doc_2"],
    k=10,
    alpha=0.5,  # 1.0 = vector, 0.0 = graph
    max_hops=2
)
```

---

## 3. Graph Analytics

Advanced algorithms run on the HNSW Layer 0 graph to identify influential nodes or communities.

### PageRank Centrality

Calculates relative importance based on HNSW connectivity.

```python
scores = client.calculate_pagerank(
    dataset="knowledge",
    damping_factor=0.85,
    max_iterations=20
)
# Returns: {node_id: score, ...}
```

### Community Detection (LPA)

Identifies densely connected clusters.

```python
communities = client.detect_communities(
    dataset="knowledge",
    max_iterations=10
)
# Returns: {node_id: community_id, ...}
```

---

## 4. Distributed GraphRAG

For large-scale graphs across multiple nodes:

```python
# Expand multiple seed nodes across cluster
expansion = client.graph_rag_expand(
    dataset="knowledge",
    node_ids=[1, 2, 3, 4, 5]  # Multi-seed expansion
)
# Returns: {1: [neighbors...], 2: [neighbors...], ...}
```

---

## 5. Hardware Acceleration (v0.1.9)

Moving spreading activation and BFS expansion to GPU (CUDA/Metal) for sub-millisecond traversal.

### CSR Optimization

The GraphStore automatically flattens to CSR format before VRAM sync:

| Array | Type | Description |
|-------|------|-------------|
| Offsets | `uint32[]` | Pointers into neighbors array |
| Neighbors | `uint32[]` | Flattened target node IDs |
| Weights | `float32[]` | Flattened edge weights |

### GPU Kernels

1. **BFS Expansion**: Atomic bitset for visited checks, parallel frontier expansion
2. **Activation Propagate**: Atomic FP additions with alpha decay

### Performance Benchmarks (v0.1.9)

| Scale | Operation | CPU Latency | GPU Latency |
|-------|-----------|------------|-----------|
| 1M nodes / 10M edges | GraphExpand (d=2) | 15ms | **0.8ms** |
| 10M nodes / 100M edges | GraphExpand (d=2) | 85ms | **4.2ms** |
| 100M nodes / 1B edges | GraphExpand (d=3) | 650ms | **28ms** |

**CLI Benchmark**:
```bash
python3 scripts/unified_benchmark.py --mode graphrag --dims 768 --counts 10000
```

---

## Performance Characteristics

| Operation | Complexity | Typical Latency |
|-----------|------------|----------------|
| `add-edge` | O(1) | < 1ms |
| `traverse` (depth=2) | O(M^2) | 1–20ms |
| PageRank (N=1M) | O(I × E) | 500ms – 2s |
| LPA (N=1M) | O(I × E) | 300ms – 1.5s |

> [!IMPORTANT]
> Enable GPU acceleration with `gpu_enabled: true` in dataset config.
> Graph analytics (PageRank/LPA) run on local HNSW graph partition in distributed mode.

---

## Arrow Integration

Knowledge graph data exports/imports as Arrow RecordBatches.

**Schema**:
- `subject`: `uint32`
- `predicate`: `dictionary<int32, binary>`
- `object`: `uint32`
- `weight`: `float32`

This enables seamless migration via Arrow Flight streams.