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

---

## 6. Adaptive Index Selection (Learned Index)

Longbow automatically selects the best ANN index type (HNSW, IVF-PQ, DiskANN) for each
query at runtime using a **k-nearest-neighbour classifier** trained on observed query
performance data.

### How it Works

Every search request arrives carrying a `QueryFeatures` struct. This is projected into an
11-dimensional feature vector:

| Dimension | Source field | Notes |
|---|---|---|
| 0 | `VectorDimension` | Raw int |
| 1 | `NumQueryVectors` | Batch size |
| 2 | `SearchK` | Number of results requested |
| 3 | `DatasetSize` | Most discriminating feature |
| 4 | `NumCollections` | |
| 5 | `QueryComplexity` | `simple=0.0`, `medium=0.5`, `complex=1.0` |
| 6 | `AvgVectorNorm` | |
| 7 | `IsFiltered` | 0 or 1 |
| 8 | `IsHybrid` | 0 or 1 |
| 9 | `TimeOfDay` | |
| 10 | `DayOfWeek` | |

The `FeatureNormalizer` maintains online per-feature min/max statistics (updated on every
`AddTrainingSample` call) and normalises each vector to [0, 1] before distance computation.

**Scoring (once `MinTrainingSamples` is reached):**

```
query_vec = normalise(extractFeatureVector(features))
for each stored TrainingSample s:
    dist = weightedEuclidean(query_vec, normalise(s.Features), featureWeights)

k nearest neighbours vote by inverse-distance weight:
    score[s.Index] += 1 / (dist + ε)

Recommended index = argmax(scores)
```

**Before `MinTrainingSamples`:** falls back to `getDefaultPrediction`, a hand-coded heuristic
based on dataset size thresholds. All predictions emit
`longbow_learned_index_predictions_total{method="default"|"knn"}`.

### Feature Weight Learning (LDA)

After every `UpdateInterval` when ≥ `MinTrainingSamples` samples are buffered, an async
goroutine runs **`updateWeights`** — a Fisher Linear Discriminant between-class variance
computation:

```
For each feature d:
    between_var[d] = Σ_class  count_class × (class_mean[d] − global_mean[d])²
    weight[d] = between_var[d] / Σ between_var   (+ε floor)
```

Features that strongly separate HNSW from IVF-PQ from DiskANN accumulate higher weights,
improving k-NN distance precision over time. The update runs concurrently without blocking search.

### Configuration

| Environment / config field | Default | Effect |
|---|---|---|
| `LearnedIndexConfig.KNN` | `7` | k for the k-NN scorer |
| `LearnedIndexConfig.MinTrainingSamples` | `100` | Minimum samples before k-NN activates |
| `LearnedIndexConfig.UpdateInterval` | `1h` | Minimum interval between weight updates |
| `LearnedIndexConfig.EnableAutoSelection` | `true` | Toggle auto-selection entirely |

### Observability

All learned index activity is exposed via Prometheus on port 9090:

| Metric | Type | Description |
|---|---|---|
| `longbow_learned_index_predictions_total{index_type, method}` | Counter | Predictions per index type and scoring method (`knn`/`default`) |
| `longbow_learned_index_prediction_correct_total` | Counter | Confirmed correct predictions (feedback loop) |
| `longbow_learned_index_training_samples_total` | Gauge | Buffer depth (max 10,000) |
| `longbow_learned_index_knn_duration_seconds` | Histogram | k-NN scoring latency per prediction |
| `longbow_learned_index_weight_update_duration_seconds` | Histogram | LDA weight update latency |
| `longbow_learned_index_adaptations_total{status}` | Counter | Adaptation lifecycle events |
| `longbow_learned_index_adaptation_latency_gain_ms` | Histogram | Observed latency delta after adaptation |
| `longbow_learned_index_sample_overflow_total` | Counter | Buffer eviction events |

### Rollback

The `RuntimeIndexAdapter` supports index-switch rollback via the `IndexSwitcher` interface:

```go
type IndexSwitcher interface {
    SwitchIndex(collection string, to IndexType) error
}

adapter.WithIndexSwitcher(myStore) // myStore implements IndexSwitcher
adapter.Rollback("my_collection")  // now actually reverts the live index
```

Without a wired `IndexSwitcher`, `Rollback` updates internal state and returns a typed error
rather than silently succeeding.

### Benchmark Validation

```bash
python3 scripts/unified_benchmark.py --mode learned_index --metrics-addr 127.0.0.1:9090
```

The benchmark runs 4 stages: heuristic warm-up, training sample accumulation, Prometheus metric
verification (confirms `method="knn"` predictions appear), and latency comparison.
