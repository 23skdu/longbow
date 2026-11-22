---
name: longbow-memory
description: Store and retrieve AI agent memories using Longbow vector engine with temporal, graph, geo-spatial, and hybrid search capabilities. Use when building AI agents that need persistent memory, semantic search, temporal queries, knowledge graphs, or location-based lookups.
---

# Longbow Agent Memory

Longbow provides specialized features for high-performance LLM Agent memory, focusing on long-latency retrieval, semantic awareness, and adaptive performance tuning.

## Core Concepts

### Hybrid Search (Dense + Sparse)

Agents often need to retrieve specific facts (matching a keyword like a project name) alongside semantic concepts. Longbow combines:

- **Vector Search (Dense)**: Semantic similarity via OpenAI, Cohere, ONNX, or WASM embeddings.
- **BM25 Search (Sparse)**: Full-text keyword matching using an integrated inverted index.
- **Rank Fusion**: Automatically merges results to ensure the most relevant context is provided to the LLM agent.

### Adaptive Learned Index (Production Hardened)

As an agent's memory (collection size) grows or its query patterns change (e.g., switching from general chat to deep codebase analysis), the underlying index must adapt. Longbow's **Learned Index** automatically optimizes itself:

- **k-NN Predictor**: A data-driven model that learns the relationship between query features (vector dimension, search_k, dataset size) and index performance.
- **Semantic Awareness**: The predictor understands that different embedding models (e.g., `text-embedding-3-small` vs `ada-002`) exhibit different performance characteristics and adapts its recommendations accordingly.
- **Live Migration**: The system can perform zero-downtime, background migrations between index types (e.g., from HNSW to DiskANN) if it detects a more optimal configuration.
- **Performance Rollback**: If a suggested adaptation degrades performance, the system automatically detects the regression and rolls back to the previous stable state, recording the failure to avoid repeating the mistake.

### Temporal Awareness

Agent memory is often time-sensitive. Longbow supports:

- **Time-based Queries**: Filtering and ranking results by age.
- **Recency Biasing**: Boosting newer memories that are likely more relevant to the current conversation context.

### Geo-Spatial Awareness

Agents operating in the physical world (e.g., delivery bots, drone swarms, or augmented reality assistants) require memories tied to coordinates. Longbow provides:

- **Proximity Retrieval**: Quickly finding relevant memories within a specific radius of the agent's current location.
- **Geographic Re-ranking**: Weighting semantic relevance against physical distance to provide contextually accurate answers.

### Turboquant (High-Speed Compression)

For agents running on "the edge" or requiring massive memory pools with sub-millisecond latency, **Turboquant V2** provides:

- **4x-8x Memory Reduction**: Using **Learnable Bit-Widths** (adaptive 1/2/4-bit quantization) to store millions of memories in a fraction of the RAM.
- **Hardware Acceleration**: Leveraging **AVX-512**, **AVX2**, and **ARM Neon** kernels for blazing fast dot product calculations directly on the compressed data.

### Memory Hygiene and Multi-Tenancy

For complex AI agents, managing memory is not just about retrieval; it is about isolation, selective forgetting, and resource efficiency.

#### Session Isolation via Namespaces

Agents often handle multiple users or sessions simultaneously. Longbow's **Namespaces** allow you to isolate these contexts completely:

- **Tenancy**: Create a separate namespace for each user (`user_123`, `user_456`). This ensures that an agent never accidentally retrieves one user's private data for another.
- **Bulk Cleanup**: When a session ends or a user deletes their profile, a single `delete-namespace` call wipes all associated memories instantly across both RAM and disk.
- **Quota Control**: Prevent a single chatty session from consuming the entire cluster's memory by setting per-namespace vector limits.

#### Selective Forgetting with Tombstones

Agents frequently need to "forget" or update specific facts without restarting the system:

- **Soft-Deletes**: Deleting a specific memory (via `Delete`) marks it with a **Tombstone**. This is a sub-millisecond operation that ensures the memory is immediately excluded from all future searches.
- **Fact Updates**: When an agent learns new information about an existing topic (e.g., a user's changed preference), re-ingesting the memory with the same ID automatically tombstones the old version and indexes the new one, ensuring the agent's knowledge remains current.

#### Namespace vs. Tombstone: When to use what?

| Feature | Scope | Latency | Reclamation | Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **Namespace** | Bulk / logical container | Instant (Metadata) | Background (Recursive Drop) | New User, Project, or Customer Tenant |
| **Tombstone** | Granular / per-record | Sub-ms (Bitset) | Background (Compaction) | Fact Update, Error Correction, GDPR Forgetting |

#### Background Hygiene (Compaction)

As an agent matures and its memory accumulates tombstones (deleted/outdated facts), Longbow's **Fragmentation-Aware Compaction** automatically cleans up the storage in the background:

- **Efficiency**: Sparse batches are merged into dense batches to reclaim memory.
- **Index Stability**: Unlike naive vector stores that require full rebuilds after many deletions, Longbow performs incremental index updates during compaction.
- **Zero Downtime**: The agent continues to function normally while the system optimizes its internal representation of the memory.

## Python SDK Quick Start

### Installation

Install the Python SDK:

```bash
pip install longbow-sdk
```

Or use directly from source:

```bash
pip install pyarrow pandas
```

### Connection

```python
from longbow import LongbowClient

client = LongbowClient(uri="grpc://localhost:3000")
client.connect()
```

### Enabling the Learned Index

Initialize the store with the Learned Index enabled for fully adaptive memory:

```go
predictor := store.NewIndexPerformancePredictor(store.DefaultLearnedIndexConfig())
adapter := store.NewRuntimeIndexAdapter(logger, predictor, store.DefaultAdaptationConfig(), store)
adapter.Start()
```

When performing searches, the store will now collect performance feedback to refine its internal models, ensuring that as your agent grows, its memory access becomes faster and more reliable.

### Storing Memories

#### Basic Vector Memory

```python
# Store memories as embeddings with metadata
memories = [
    {"id": "user_pref_1", "text": "User prefers dark mode", "vector": [0.1, 0.3, ...]},
    {"id": "conversation_1", "text": "User asked about API", "vector": [0.2, 0.5, ...]},
]
client.insert("memories", memories)
```

#### With Auto-Embedding

```python
# Store text directly - embedding handled externally
client.insert("memories", [
    {"id": "memory_1", "text": "User prefers concise answers", "timestamp": 1700000000000000000},
    {"id": "memory_2", "text": "Project deadline is Friday", "timestamp": 1700100000000000000},
])
```

### Dataset Management

```python
# Create namespace/dataset
client.create_namespace(
    name="memories",
    dims=384,
    data_type="float32"
)

# List datasets
datasets = client.list_namespaces()

# Delete
client.delete_namespace("memories")
```

## Search Types

### Semantic / Vector Search

```python
results = client.search(
    dataset="memories",
    vector=[0.1, 0.2, 0.3, ...],
    k=5,
    filters=[{"field": "type", "op": "eq", "value": "preference"}]
)
# Returns DataFrame with id, text, distance columns
```

### Hybrid Search (Vector + Text)

```python
results = client.search(
    dataset="memories",
    vector=[0.1, 0.2, ...],
    text_query="user preferences",
    alpha=0.7,  # 1.0 = full text, 0.0 = full vector
    k=5
)
```

### Temporal Search

#### As-of search (state at specific time)

```python
results = client.temporal_search(
    search_type="as_of",
    timestamp=1700050000000000000,  # unix nanoseconds
    k=10
)
```

#### Range search (time window)

```python
results = client.temporal_search(
    search_type="range",
    start_time=1700000000000000000,
    end_time=1700100000000000000,
    k=10
)
```

#### Sliding window (last N items)

```python
results = client.temporal_search(
    search_type="sliding_window",
    window_size=100,
    k=10
)
```

#### Sliding window by duration

```python
results = client.temporal_search(
    search_type="sliding_window_time",
    duration="1h",  # "30m", "2h", "1d"
    k=10
)
```

#### Version history

```python
versions = client.temporal_version_history(vector_id=12345)
```

### Geo-Spatial Search

#### Radius search

```python
results = client.geo_search(
    dataset="locations",
    center={"lat": 37.7749, "lon": -122.4194},
    radius_km=10,
    search_type="radius",
    k=10
)
```

#### Bounding box search

```python
results = client.geo_search(
    dataset="locations",
    box={"min_lat": 37.7, "max_lat": 37.8, "min_lon": -122.5, "max_lon": -122.4},
    search_type="box",
    k=10
)
```

#### Hybrid (vector + geo)

```python
results = client.geo_search(
    dataset="locations",
    center={"lat": 37.7749, "lon": -122.4194},
    radius_km=5,
    search_type="hybrid",
    k=10
)
```

### GraphRAG (Knowledge Graph)

#### Add edges to create knowledge graph

```python
client.add_edge(
    dataset="knowledge",
    subject=1,
    predicate="knows",
    object=2,
    weight=1.0
)
```

#### Traverse graph

```python
results = client.traverse(
    dataset="knowledge",
    start=1,
    max_hops=2,
    decay=0.5  # decay factor per hop
)
```

#### Get recommendations (hybrid vector-graph)

```python
results = client.recommend(
    dataset="memories",
    seed_ids=["memory_1", "memory_2"],
    k=10,
    alpha=0.5,  # 1.0 = vector, 0.0 = graph
    max_hops=2
)
```

#### PageRank centrality

```python
scores = client.calculate_pagerank(
    dataset="knowledge",
    damping_factor=0.85,
    max_iterations=20
)
```

#### Community detection

```python
communities = client.detect_communities(
    dataset="knowledge",
    max_iterations=10
)
```

### Metadata Filters

All search methods support filters:

```python
filters = [
    {"field": "source", "op": "eq", "value": "chat"},
    {"field": "importance", "op": "gte", "value": 5},
]
```

Supported operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `like`

## Expanded Search Modalities for Agent Memory

Longbow exposes a broad set of search modalities so an agent can retrieve the right memory for the right situation. Each modality below includes a short explanation and a typical agent use case.

### Dense (Semantic) Search

Finds memories whose embedding is closest to a query vector. This is the default "what do I know about X?" lookup.

- **Use case:** An agent recalling prior conversations that are semantically related to the current user request.

### Sparse / BM25 Search

Keyword-driven full-text search over the inverted index, with no vector required.

- **Use case:** An agent looking up an exact project name, ticket ID, or entity string that must match verbatim.

### Hybrid Search (Dense + Sparse)

Blends vector similarity and BM25 with a tunable `alpha` (1.0 = full text, 0.0 = full vector).

- **Use case:** An agent answering "tell me about the Atlas migration" where both the concept and the literal name matter.

### Filtered Search

Dense search constrained by metadata predicates (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `like`).

- **Use case:** An agent retrieving only high-importance memories (`importance >= 5`) from a specific source.

### Range Search

Returns all memories within a distance threshold of a query vector, up to a cap.

- **Use case:** An agent finding every memory "close enough" to a concept rather than just the top-k.

### Search by ID (Neighbor Expansion)

Uses a stored memory as the query and returns its nearest neighbors.

- **Use case:** An agent expanding from a known fact to discover related context.

### Temporal Search

Time-aware retrieval over the temporal index.

- **As-of:** state of memory at a specific timestamp.
- **Range:** memories within a time window.
- **Sliding window:** the last N memories.
- **Sliding window by duration:** memories from the last `1h` / `30m` / `1d`.
- **Use case:** An agent answering "what did we decide yesterday?" or maintaining a rolling session context.

### Temporal Version History

Returns all versions of a memory identified by its vector ID.

- **Use case:** An agent auditing how a fact evolved over time or rolling back to a previous version.

### Temporal Aggregation

Aggregates temporal buckets (counts/averages) over a time range.

- **Use case:** An agent summarizing activity volume per hour/day for a user or project.

### Geo-Spatial Search

Location-aware retrieval.

- **Radius:** memories within N km of a center point.
- **Bounding box:** memories inside a lat/lon rectangle.
- **Hybrid:** vector similarity constrained to a geographic radius.
- **Use case:** An embodied/AR agent retrieving memories tied to the user's current physical location.

### GraphRAG Expansion

Expands a set of seed nodes through the knowledge graph and blends graph reachability with vector similarity.

- **Use case:** An agent answering multi-hop questions ("who collaborates with the person who owns this project?").

### Graph Traversal

Walks the knowledge graph from a start node up to N hops with a decay factor.

- **Use case:** An agent exploring related entities and concepts from a known anchor.

### Recommendation (Hybrid Vector-Graph)

Combines seed-vector similarity with graph proximity to recommend related memories.

- **Use case:** An agent suggesting follow-up topics or next-best memories after a conversation turn.

### PageRank Centrality

Computes importance scores for nodes in the knowledge graph.

- **Use case:** An agent prioritizing authoritative entities when ranking retrieved context.

### Community Detection

Clusters the knowledge graph into communities.

- **Use case:** An agent grouping related topics or users for summarization or routing.

### Multimodal Search

Cross-modal retrieval (text-to-image, image-to-text, audio) through the multimodal pipeline.

- **Use case:** An agent retrieving an image memory from a text description, or vice versa.

### Federated Search

Scatter-gathers search across multiple collections/datasets.

- **Use case:** An agent searching across several user namespaces or tenants in one call.

### Advanced Text Search

Boolean, phrase, and fuzzy text queries over the inverted index.

- **Use case:** An agent running precise queries like `"project Atlas" AND deadline`.

### Learned (Adaptive) Search

Uses the learned index to adapt retrieval to query features and dataset size, improving latency as the agent grows.

- **Use case:** A long-running agent whose memory grows from hundreds to millions of records without manual tuning.

### Facets and Analytics

Returns aggregated facet counts and search analytics over a dataset.

- **Use case:** An agent inspecting memory composition (e.g., how many memories per source/type) before deciding a strategy.

### Vector and Graph Lookups

Direct fetch of a stored vector by ID and graph statistics for a dataset.

- **Use case:** An agent verifying a specific memory or checking graph health before traversal.


## Memory Patterns

### Agent Memory Storage

```python
def store_memory(client, memory_text, metadata=None):
    """Store an agent memory with embedding."""
    memory = {
        "id": f"mem_{metadata.get('timestamp', time.time_ns())}",
        "text": memory_text,
        "timestamp": time.time_ns(),
        **(metadata or {})
    }
    client.insert("agent_memory", [memory])
    return memory["id"]

def recall_memories(client, query_vector, k=5, time_range=None):
    """Recall relevant memories."""
    kwargs = {"dataset": "agent_memory", "vector": query_vector, "k": k}
    if time_range:
        kwargs["filters"] = [
            {"field": "timestamp", "op": "gte", "value": time_range[0]},
            {"field": "timestamp", "op": "lte", "value": time_range[1]},
        ]
    return client.search(**kwargs)
```

### Session Context

```python
def get_session_context(client, session_id, max_age="1h"):
    """Get memories from current session."""
    results = client.temporal_search(
        search_type="sliding_window_time",
        duration=max_age,
        k=20
    )
    return [r["text"] for r in results if r.get("session_id") == session_id]
```

### Local Search for AR/VR Agents

An AR assistant can store semantic tags for objects in a room. By using **Geo-Spatial Proximity**, the agent can retrieve the name of a device just by "looking" at its coordinates, while **Turboquant** ensures the entire room's data fits on the headset's limited memory.

### Massive History for Personal AI

A personal assistant storing every conversation for years can utilize **Turboquant** to compress history by 8x. When a user asks "Where was that Italian place we went to in SF?", the agent uses **Hybrid Search** (Italian) + **Geo-Spatial** (SF) + **Temporal Awareness** (past history) to find the exact memory in under 5ms.
