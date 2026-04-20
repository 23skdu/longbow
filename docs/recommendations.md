# Hybrid Recommendations API

Longbow provides a hybrid recommendation engine that combines vector similarity with graph connectivity to produce context-aware recommendations.

## Overview

The Recommend API blends two signals:

1. **Vector Similarity (ANN)**: Finds vectors closest to the centroid of seed vectors
2. **Graph Connectivity**: Traverses the knowledge graph to find nodes connected to seeds

This hybrid approach produces more relevant recommendations than pure similarity search, especially for recommendation scenarios where explicit relationships between items matter.

## How It Works

1. **Seed Resolution**: Resolve seed IDs to their vector representations
2. **Centroid Computation**: Calculate the centroid of all seed vectors
3. **ANN Search**: Find top-K candidates using approximate nearest neighbor search
4. **Graph Traversal**: Perform BFS from seeds up to `max_hops`, applying `decay` factor
5. **SQL Filtering** (Optional): Apply complex relational predicates via **CTEs and Subqueries** to pre-filter seeds or post-filter the final candidate set (see [Advanced SQL](sql.md)).
6. **Hybrid Scoring**: Combine scores using the formula:
   ```
   score = α × similarity(centroid, vector) + (1-α) × connectivity(seeds, vector)
   ```
7. **Ranking**: Sort by hybrid score and return top-K

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dataset` | string | required | Dataset name |
| `seed_ids` | []string | required | List of seed IDs to base recommendations on |
| `k` | int | 10 | Number of recommendations to return |
| `alpha` | float32 | 0.5 | Blend coefficient: 1.0=pure ANN, 0.0=pure graph |
| `max_hops` | int | 2 | Maximum BFS depth for graph traversal |
| `decay` | float32 | 0.5 | Multi-hop connectivity decay factor (0.0-1.0) |

### Alpha Tuning

- **Alpha = 1.0**: Pure ANN - recommendations based solely on vector similarity
- **Alpha = 0.0**: Pure Graph - recommendations based solely on graph connectivity
- **Alpha = 0.5**: Hybrid - balanced blend of both signals

### Decay Factor

The decay factor controls how quickly connectivity scores decrease with hop distance:
- **Decay = 1.0**: No decay - all hops weighted equally
- **Decay = 0.5**: Standard decay - score halves at each hop
- **Decay = 0.0**: Nearest neighbors only - only direct connections

## Usage

### Python SDK

```python
from longbow import LongbowClient
import pandas as pd

client = LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")

# Basic recommendation
results = client.recommend(
    dataset="products",
    seed_ids=["item_123", "item_456"],
    k=10
)

# Hybrid with explicit parameters
results = client.recommend(
    dataset="products",
    seed_ids=["item_123"],
    k=10,
    alpha=0.5,      # Balanced hybrid
    max_hops=2,     # Explore 2 hops in graph
    decay=0.5       # Decay factor for connectivity
)

# Pure ANN (no graph)
results = client.recommend(
    dataset="products",
    seed_ids=["item_123"],
    k=10,
    alpha=1.0
)

# Pure graph connectivity
results = client.recommend(
    dataset="products",
    seed_ids=["item_123"],
    k=10,
    alpha=0.0,
    max_hops=3
)
```

### gRPC/Arrow Flight

Send a JSON ticket via DoGet:

```python
import json
import pyarrow.flight as flight

ticket = {
    "recommend": {
        "dataset": "products",
        "seed_ids": ["item_123", "item_456"],
        "k": 10,
        "alpha": 0.5,
        "max_hops": 2,
        "decay": 0.5
    }
}

client = flight.connect("grpc://localhost:3000")
reader = client.do_get(flight.Ticket(json.dumps(ticket).encode()))
table = reader.read_all()
df = table.to_pandas()
```

## Use Cases

### E-Commerce Recommendations

```python
# Recommend products similar to user's purchase history
results = client.recommend(
    dataset="products",
    seed_ids=purchased_item_ids,  # User's purchase history
    k=10,
    alpha=0.6,  # Slightly more weight on similarity
    max_hops=2,
    decay=0.5
)
```

### Content Recommendations

```python
# Recommend articles based on reading history
results = client.recommend(
    dataset="articles",
    seed_ids=read_article_ids,
    k=5,
    alpha=0.7,  # More weight on content similarity
    max_hops=3,
    decay=0.3   # Prefer closer connections
)
```

### Graph-Based Discovery

```python
# Discover entities through knowledge graph connections
results = client.recommend(
    dataset="entities",
    seed_ids=[known_entity_id],
    k=20,
    alpha=0.3,  # More weight on graph connections
    max_hops=4,
    decay=0.7   # Slow decay for deep exploration
)
```

## Benchmarking

Run recommend benchmarks using the unified benchmark script:

```bash
# Test hybrid vs ANN performance
python3 scripts/unified_benchmark.py --mode recommend \
  --alpha-values 0.0,0.5,1.0 \
  --k-values 5,10,20 \
  --num-seeds 5 \
  --max-hops 2 \
  --decay 0.5 \
  --queries 1000
```

This compares:
- Alpha = 0.0: Pure graph connectivity
- Alpha = 0.5: Hybrid blend
- Alpha = 1.0: Pure ANN similarity

## Metrics

Prometheus metrics are exported for recommendations:

| Metric | Type | Description |
|--------|------|-------------|
| `longbow_recommendations_total` | Counter | Total recommendation requests |
| `longbow_recommendations_latency_seconds` | Histogram | Request latency |
| `longbow_recommendations_seed_count` | Histogram | Number of seeds per request |

Scrape on port 9090:
```bash
curl http://localhost:9090/metrics | grep recommendations
```

## Edge Cases

### Empty Seeds

If no valid seeds are found, the API returns an error:
```
Error: no valid seeds found in dataset
```

### Invalid Alpha Values

- Alpha > 1.0: Clamped to 1.0 (pure ANN)
- Alpha < 0.0: Clamped to 0.0 (pure graph)

### Decay Edge Cases

- Decay <= 0: Defaults to 0.5
- Decay > 1.0: Clamped to 1.0

### Graph Without Edges

If the graph has no edges, the system behaves as pure ANN (alpha effectively becomes 1.0 for scoring).

## Performance Considerations

1. **Index Size**: Larger K values require more ANN candidates (internally searches K×3)
2. **Graph Depth**: Deeper max_hops increases BFS complexity
3. **Seed Count**: More seeds require centroid computation across more vectors
4. **Hybrid Overhead**: Hybrid scoring adds graph traversal latency vs pure ANN

For optimal performance:
- Use smaller K values when latency is critical
- Limit max_hops to 2-3 for real-time applications
- Pre-warm the graph cache for frequently accessed nodes
