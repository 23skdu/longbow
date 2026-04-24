---
name: longbow-memory
description: Store and retrieve AI agent memories using Longbow vector database with temporal, graph, geo-spatial, and hybrid search capabilities. Use when building AI agents that need persistent memory, semantic search, temporal queries, knowledge graphs, or location-based lookups.
---

# Longbow Memory Skill

Longbow is a high-performance distributed vector cache with support for multiple search types. This skill enables AI agents to store memories and perform various searches.

## Installation

Install the Python SDK:

```bash
pip install longbow-sdk
```

Or use directly from source:

```bash
pip install pyarrow pandas
```

## Connection

```python
from longbow import LongbowClient

client = LongbowClient(uri="grpc://localhost:3000")
client.connect()
```

## Storing Memories

### Basic Vector Memory

```python
# Store memories as embeddings with metadata
memories = [
    {"id": "user_pref_1", "text": "User prefers dark mode", "vector": [0.1, 0.3, ...]},
    {"id": "conversation_1", "text": "User asked about API", "vector": [0.2, 0.5, ...]},
]
client.insert("memories", memories)
```

### With Auto-Embedding

```python
# Store text directly - embedding handled externally
client.insert("memories", [
    {"id": "memory_1", "text": "User prefers concise answers", "timestamp": 1700000000000000000},
    {"id": "memory_2", "text": "Project deadline is Friday", "timestamp": 1700100000000000000},
])
```

## Search Types

### 1. Semantic/Vector Search

```python
results = client.search(
    dataset="memories",
    vector=[0.1, 0.2, 0.3, ...],
    k=5,
    filters=[{"field": "type", "op": "eq", "value": "preference"}]
)
# Returns DataFrame with id, text, distance columns
```

Hybrid search with text query:

```python
results = client.search(
    dataset="memories",
    vector=[0.1, 0.2, ...],
    text_query="user preferences",
    alpha=0.7,  # 1.0 = full text, 0.0 = full vector
    k=5
)
```

### 2. Temporal Search

```python
# As-of search (state at specific time)
results = client.temporal_search(
    search_type="as_of",
    timestamp=1700050000000000000,  # unix nanoseconds
    k=10
)

# Range search (time window)
results = client.temporal_search(
    search_type="range",
    start_time=1700000000000000000,
    end_time=1700100000000000000,
    k=10
)

# Sliding window (last N items)
results = client.temporal_search(
    search_type="sliding_window",
    window_size=100,
    k=10
)

# Sliding window by duration
results = client.temporal_search(
    search_type="sliding_window_time",
    duration="1h",  # "30m", "2h", "1d"
    k=10
)
```

Get version history for a memory:

```python
versions = client.temporal_version_history(vector_id=12345)
```

### 3. Geo-Spatial Search

```python
# Radius search
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
    search_type="hybrid",
    k=10
)
```

### 4. GraphRAG (Knowledge Graph)

```python
# Add edges to create knowledge graph
client.add_edge(
    dataset="knowledge",
    subject=1,
    predicate="knows",
    object=2,
    weight=1.0
)

# Traverse graph
results = client.traverse(
    dataset="knowledge",
    start=1,
    max_hops=2,
    decay=0.5  # decay factor per hop
)

# Get recommendations (hybrid vector-graph)
results = client.recommend(
    dataset="memories",
    seed_ids=["memory_1", "memory_2"],
    k=10,
    alpha=0.5,  # 1.0 = vector, 0.0 = graph
    max_hops=2
)

# PageRank centrality
scores = client.calculate_pagerank(
    dataset="knowledge",
    damping_factor=0.85,
    max_iterations=20
)

# Community detection
communities = client.detect_communities(
    dataset="knowledge",
    max_iterations=10
)
```

## Metadata Filters

All search methods support filters:

```python
filters = [
    {"field": "source", "op": "eq", "value": "chat"},
    {"field": "importance", "op": "gte", "value": 5},
]
```

Operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `like`

## Dataset Management

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

## Common Patterns

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