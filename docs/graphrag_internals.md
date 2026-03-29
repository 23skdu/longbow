# GraphRAG Internals: Spreading Activation Algorithm

**Last Updated**: 2026-03-28
**Related Docs**: [graph_rag.md](graph_rag.md) · [graph_navigation.md](graph_navigation.md) · [vectorsearch.md](vectorsearch.md)

---

## Overview

Longbow's GraphRAG path extends standard ANN search with a **spreading activation** traversal
over the HNSW graph. The result is a re-ranked candidate list that reflects both vector
similarity *and* graph connectivity — giving better recall for queries where related entities
cluster together in the embedding space.

The two parameters that control this behaviour are:

| Parameter    | Type    | Default | Range     | Description |
|--------------|---------|---------|-----------|-------------|
| `GraphAlpha` | float64 | `0.85`  | 0.0 – 1.0 | Damping coefficient: fraction of activation retained at each hop |
| `GraphDepth` | int     | `2`     | 0 – N     | Maximum BFS hops from each seed node |

---

## Algorithm: Spreading Activation

### Step 1 — ANN Seed Query

A standard HNSW vector search retrieves the top-K candidates. These are called **seed nodes**
and are assigned an initial activation score derived from their ANN similarity score:

```
activation[seed] = 1.0 / (1.0 + distance)
```

### Step 2 — BFS Graph Expansion

Starting from each seed, the algorithm expands outward layer by layer up to `GraphDepth` hops.
At each hop, the activation of a parent node is propagated to its HNSW layer-0 neighbors,
multiplied by the damping coefficient `GraphAlpha`:

```
activation[neighbor] += activation[parent] * GraphAlpha^hop
```

If a node is reachable via multiple paths, its activation accumulates (PageRank-style).

### Step 3 — Re-ranking

All nodes in the expanded set are then sorted by accumulated activation score (descending) and
the top-K are returned.

### Score Decay Formula

```
score_at_hop_n = initial_score × GraphAlpha^n
```

| Alpha | Score at Hop 1 | Score at Hop 2 | Score at Hop 3 |
|-------|---------------|---------------|---------------|
| 0.0   | 0.000         | 0.000         | 0.000         |
| 0.50  | 0.500         | 0.250         | 0.125         |
| 0.85  | 0.850         | 0.723         | 0.614         |
| 1.0   | 1.000         | 1.000         | 1.000         |

---

## Parameter Guidance

### `GraphAlpha`

- **`alpha = 0.0`**: No spreading. Only the seed ANN results are returned (equivalent to
  disabling GraphRAG).
- **`alpha = 0.85`**: The PageRank default. Recommended for most RAG workloads — good
  balance between graph influence and precision.
- **`alpha = 1.0`**: Full spreading with no decay. Use only with shallow `GraphDepth` (1–2)
  to avoid flooding the result set with distantly related nodes.

### `GraphDepth`

- **`depth = 0`**: Returns only seed nodes. Graph expansion is disabled.
- **`depth = 1`**: Expands direct HNSW neighbors of seeds. Low latency, useful for
  entity-relationship queries.
- **`depth = 2`**: Recommended default. Captures second-order relationships without
  significant latency increase.
- **`depth ≥ 4`**: Use with caution. Exponential candidate explosion at high M values.

### Recommended Defaults

```go
// For most RAG workloads:
GraphAlpha = 0.85
GraphDepth = 2

// For precision-focused retrieval (legal, medical):
GraphAlpha = 0.70
GraphDepth = 1

// For broad knowledge-base exploration:
GraphAlpha = 0.90
GraphDepth = 3
```

---

## Performance Characteristics

| Config            | Latency Overhead vs ANN | Recall@10 Gain | Use Case |
|-------------------|------------------------|----------------|---------- |
| depth=0           | +0%                    | 0%             | Pure ANN  |
| alpha=0.85, depth=1 | +5–15%               | +5–12%         | Most RAG  |
| alpha=0.85, depth=2 | +10–30%              | +8–18%         | GraphRAG default |
| alpha=0.90, depth=3 | +25–60%              | +10–22%        | Deep KG traversal |

> [!TIP]
> Profile your specific workload with the `longbow_graph_rag_rerank_latency_seconds` histogram
> before committing to depth ≥ 3 in production.

---

## Prometheus Metrics

All GraphRAG operations emit the following metrics (defined in
`internal/metrics/graph_navigation_metrics.go`):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `longbow_graph_rag_operations_total` | Counter | `dataset`, `result` | Total GraphRAG calls by outcome |
| `longbow_graph_rag_alpha_value` | Histogram | `dataset` | Distribution of alpha values used |
| `longbow_graph_rag_depth_value` | Histogram | `dataset` | Distribution of depth values used |
| `longbow_graph_rag_rerank_latency_seconds` | Histogram | `dataset` | Re-ranking phase latency |
| `longbow_graph_rag_seed_nodes_total` | Histogram | `dataset` | ANN seeds before expansion |
| `longbow_graph_rag_expanded_nodes_total` | Histogram | `dataset` | Nodes returned after expansion |

### Example Grafana Query

```promql
# P95 re-ranking latency by dataset
histogram_quantile(0.95,
  rate(longbow_graph_rag_rerank_latency_seconds_bucket[5m])
) by (dataset)

# Average expansion ratio (expanded / seed)
rate(longbow_graph_rag_expanded_nodes_total_sum[5m]) /
rate(longbow_graph_rag_seed_nodes_total_sum[5m])
```

---

## API Usage

### Archer Python Client

```python
from longbow_client import VectorSearchRequest

response = client.search(VectorSearchRequest(
    dataset="knowledge_graph",
    vector=embedding,
    k=10,
    graph_alpha=0.85,   # spreading activation damping
    graph_depth=2,      # BFS hops
))
```

### Arrow Flight (Go)

```go
// Set in the Arrow exchange metadata:
metadata := map[string]string{
    "graph_alpha": "0.85",
    "graph_depth": "2",
}
```

---

## Error Conditions

| Condition | Error Message | Action |
|-----------|--------------|--------|
| `alpha < 0.0` or `alpha > 1.0` | `alpha must be in [0.0, 1.0]` | Fix caller |
| `depth < 0` | `depth must be >= 0` | Fix caller |
| Dataset has no HNSW index | `GraphRAG requires an HNSW-backed index` | Use HNSW index type |

---

## References

- [Graph Navigation docs](graph_navigation.md) — `FindPath`, BFS and beam strategies
- [Graph RAG overview](graph_rag.md) — client-side usage and examples
- [HNSW architecture](architecture.md) — graph structure and layer 0 connectivity
- Brin & Page (1998) — PageRank: the mathematical basis for damped activation spreading
