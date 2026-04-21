# Agent Memory in Longbow

Longbow provides specialized features for high-performance LLM Agent memory, focusing on long-latency retrieval, semantic awareness, and adaptive performance tuning.

## Core Pillars of Agent Memory

### 1. Hybrid Search (Dense + Sparse)

Agents often need to retrieve specific facts (matching a keyword like a project name) alongside semantic concepts. Longbow combines:

- **Vector Search (Dense)**: Semantic similarity via OpenAI, Cohere, ONNX, or WASM embeddings.
- **BM25 Search (Sparse)**: Full-text keyword matching using an integrated inverted index.
- **Rank Fusion**: Automatically merges results to ensure the most relevant context is provided to the LLM agent.

### 2. Adaptive Learned Index (Production Hardened)

As an agent's memory (collection size) grows or its query patterns change (e.g., switching from general chat to deep codebase analysis), the underlying index must adapt. Longbow's **Learned Index** automatically optimizes itself:

- **k-NN Predictor**: A data-driven model that learns the relationship between query features (vector dimension, search_k, dataset size) and index performance.
- **Semantic Awareness**: The predictor understands that different embedding models (e.g., `text-embedding-3-small` vs `ada-002`) exhibit different performance characteristics and adapts its recommendations accordingly.
- **Live Migration**: The system can perform zero-downtime, background migrations between index types (e.g., from HNSW to DiskANN) if it detects a more optimal configuration.
- **Performance Rollback**: If a suggested adaptation degrades performance, the system automatically detects the regression and rolls back to the previous stable state, recording the failure to avoid repeating the mistake.

### 3. Temporal Awareness

Agent memory is often time-sensitive. Longbow supports:

- **Time-based Queries**: Filtering and ranking results by age.
- **Recency Biasing**: Boosting newer memories that are likely more relevant to the current conversation context.

## Configuration for Agents

To enable fully adaptive memory for an agent, initialize the store with the Learned Index enabled:

```go
predictor := store.NewIndexPerformancePredictor(store.DefaultLearnedIndexConfig())
adapter := store.NewRuntimeIndexAdapter(logger, predictor, store.DefaultAdaptationConfig(), store)
adapter.Start()
```

When performing searches, the store will now collect performance feedback to refine its internal models, ensuring that as your agent "lives" longer, its memory access becomes faster and more reliable.
