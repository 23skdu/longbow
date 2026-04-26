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

### 4. Geo-Spatial Awareness
Agents operating in the physical world (e.g., delivery bots, drone swarms, or augmented reality assistants) require memories tied to coordinates. Longbow provides:
- **Proximity Retrieval**: Quickly finding relevant memories within a specific radius of the agent's current location.
- **Geographic Re-ranking**: Weighting semantic relevance against physical distance to provide contextually accurate answers.

### 5. Turboquant (High-Speed Compression)
For agents running on "the edge" or requiring massive memory pools with sub-millisecond latency, **Turboquant V2** provides:
- **4x-8x Memory Reduction**: Using **Learnable Bit-Widths** (adaptive 1/2/4-bit quantization) to store millions of memories in a fraction of the RAM.
- **Hardware Acceleration**: Leveraging **AVX-512**, **AVX2**, and **ARM Neon** kernels for blazing fast dot product calculations directly on the compressed data.

## Configuration for Agents

To enable fully adaptive memory for an agent, initialize the store with the Learned Index enabled:

```go
predictor := store.NewIndexPerformancePredictor(store.DefaultLearnedIndexConfig())
adapter := store.NewRuntimeIndexAdapter(logger, predictor, store.DefaultAdaptationConfig(), store)
adapter.Start()
```

When performing searches, the store will now collect performance feedback to refine its internal models, ensuring that as your agent "lives" longer, its memory access becomes faster and more reliable.

## Memory Hygiene & Multi-Tenancy

For complex AI agents, managing memory is not just about retrieval; it's about isolation, selective forgetting, and resource efficiency.

### 1. Session Isolation via Namespaces
Agents often handle multiple users or sessions simultaneously. Longbow's **Namespaces** allow you to isolate these contexts completely:
- **Tenancy**: Create a separate namespace for each user (`user_123`, `user_456`). This ensures that an agent never accidentally retrieves one user's private data for another.
- **Bulk Cleanup**: When a session ends or a user deletes their profile, a single `delete-namespace` call wipes all associated memories instantly across both RAM and disk.

### 2. Selective Forgetting with Tombstones
Agents frequently need to "forget" or update specific facts without restarting the system:
- **Soft-Deletes**: Deleting a specific memory (via `Delete`) marks it with a **Tombstone**. This is a sub-millisecond operation that ensures the memory is immediately excluded from all future searches.
- **Fact Updates**: When an agent learns new information about an existing topic (e.g., a user's changed preference), re-ingesting the memory with the same ID automatically tombstones the old version and indexes the new one, ensuring the agent's knowledge remains current.

### 3. Background Hygiene (Compaction)
As an agent matures and its memory accumulates tombstones (deleted/outdated facts), Longbow's **Fragmentation-Aware Compaction** automatically cleans up the storage in the background:
- **Efficiency**: Sparse batches are merged into dense batches to reclaim memory.
- **Zero Downtime**: The agent continues to function normally while the system optimizes its internal representation of the memory.

---

## Use Cases

### Local Search for AR/VR Agents
An AR assistant can store semantic tags for objects in a room. By using **Geo-Spatial Proximity**, the agent can retrieve the name of a device just by "looking" at its coordinates, while **Turboquant** ensures the entire room's data fits on the headset's limited memory.

### Massive History for Personal AI
A personal assistant storing every conversation for years can utilize **Turboquant** to compress history by 8x. When a user asks "Where was that Italian place we went to in SF?", the agent uses **Hybrid Search** (Italian) + **Geo-Spatial** (SF) + **Temporal Awareness** (past history) to find the exact memory in under 5ms.
