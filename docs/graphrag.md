# GraphRAG: Dual-Path Graph Architecture

**Last Updated**: 2026-04-24
**Related Docs**: [functions.md](functions.md) · [vectorsearch.md](vectorsearch.md)

---

## Overview

Longbow implements a dual-path GraphRAG architecture that combines implicit vector relationships with explicit knowledge graph triples. This hybrid approach allows for both semantic similarity spreading and strict entity-relationship traversal.

### 1. Vector-based GraphRAG (Spreading Activation)
Uses the HNSW graph Layer 0 to expand and re-rank vector search results. This is effective for discovering semantically related clusters without explicit edge definitions.

### 2. Triple-based Knowledge Graph (GraphStore)
Uses an explicit Subject-Predicate-Object-Weight (SPOW) store. This allows for high-precision retrieval based on defined relationships (e.g., `EntityA --[belongs_to]--> EntityB`).

---

## 1. Vector Graph Analytics

Advanced algorithms can be run on the HNSW Layer 0 graph to identify influential nodes or community structures. These are exposed via the Meta Plane (Port 3001).

### PageRank Centrality
Calculates the relative importance of nodes based on their HNSW connectivity. Highly connected "hub" nodes receive higher scores.
- **Algorithm**: Power iteration with damping.
- **API Action**: `calculate-pagerank`
- **Parameters**: `damping_factor` (default 0.85), `max_iterations`, `tolerance`.

### Community Detection (LPA)
Identifies clusters of nodes that are more densely connected to each other than to the rest of the graph.
- **Algorithm**: Label Propagation Algorithm (LPA).
- **API Action**: `detect-communities`
- **Parameters**: `max_iterations`.

---

## 2. Knowledge Graph (Triples)

The `GraphStore` maintains explicit directed edges between `VectorID`s.

### Storage Model
Edges are stored in memory using forward and backward adjacency maps.
- **Triple**: `(Subject: VectorID, Predicate: String, Object: VectorID, Weight: Float32)`
- **Indexing**: Predicates are dictionary-encoded for memory efficiency.

### API Actions
| Action | Description | Parameters |
|--------|-------------|------------|
| `add-edge` | Adds an explicit relationship | `subject`, `predicate`, `object`, `weight` |
| `traverse-graph` | BFS traversal from a start node | `start`, `max_hops`, `incoming`, `weighted` |
| `GetGraphStats` | Basic graph metadata | `dataset` |

---

## 3. Spreading Activation Algorithm

When `graph_alpha > 0` is passed in a `VectorSearchRequest`, the following re-ranking occurs:

1. **ANN Seed Query**: Standard HNSW search retrieves top-K seeds.
2. **BFS Expansion**: Seeds expand outward up to `graph_depth` hops.
3. **Activation Propagation**:
   ```
   activation[neighbor] += activation[parent] * GraphAlpha^hop * EdgeWeight
   ```
4. **Re-ranking**: Nodes are sorted by total accumulated activation.

---

## 4. Arrow Integration

Knowledge graph data can be exported/imported as Arrow RecordBatches.

**Schema**:
- `subject`: `uint32`
- `predicate`: `dictionary<int32, binary>` (Self-contained vocabulary)
- `object`: `uint32`
- `weight`: `float32`

This allows for seamless migration of Knowledge Graphs between Longbow nodes using standard Arrow Flight streams.

---

## 5. Hardware-Accelerated GraphRAG (v0.1.9)

Longbow 0.1.9 introduces **Hardware-accelerated GraphRAG**, moving the spreading activation and BFS expansion logic directly to the GPU (CUDA/Metal). This allows for sub-millisecond traversal of graphs with billions of edges.

### Compressed Sparse Row (CSR) Optimization
To achieve high throughput, the `GraphStore` automatically flattens its adjacency maps into a **CSR format** before synchronizing with VRAM:
- **Offsets**: `uint32[]` (Pointers into the neighbors array)
- **Neighbors**: `uint32[]` (Flattened target node IDs)
- **Weights**: `float32[]` (Flattened edge weights)

### GPU Kernels
1. **BFS Expansion Kernel**: Uses an atomic bitset for visited checks and manages frontier expansion in parallel.
2. **Activation Propagate Kernel**: Accumulates scores across edges using atomic floating-point additions, respecting `alpha` decay and weights.

### Performance (v0.1.9 Benchmarks)
| Scale | Operation | CPU Latency | GPU Latency (CUDA/Metal) |
|-------|-----------|-------------|-------------------------|
| 1M Nodes / 10M Edges | `GraphExpand` (d=2) | 15ms | **0.8ms** |
| 10M Nodes / 100M Edges | `GraphExpand` (d=2) | 85ms | **4.2ms** |
| 100M Nodes / 1B Edges | `GraphExpand` (d=3) | 650ms | **28ms** |

---

## Performance Characteristics

| Operation | Complexity | Typical Latency |
|-----------|------------|-----------------|
| `add-edge` | O(1) | < 1ms |
| `traverse` (depth=2) | O(M^2) | 1–20ms |
| PageRank (N=1M) | O(I * E) | 500ms – 2s |
| LPA (N=1M) | O(I * E) | 300ms – 1.5s |

> [!IMPORTANT]
> To enable hardware acceleration, ensure your dataset is initialized with `gpu_enabled: true` and the `GraphAlpha` parameter is provided in the search query.

> [!NOTE]
> Graph analytics (PageRank/LPA) are currently local-node operations. In distributed mode, they run on the partition's local HNSW graph.
