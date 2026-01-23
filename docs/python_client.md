# Longbow Python SDK

Longbow includes a high-performance Python SDK built on `pyarrow.flight`.

## Installation

```bash
pip install ./longbowclientsdk
```

## Features

- **Zero-Copy Retrieval**: `download_arrow()` retrieves datasets as native Arrow Tables.
- **Memory-Efficient Streaming**: `download_stream()` iterates over `RecordBatch` chunks.
- **Integrated Search**: Unified `search()` method supporting both pure vector and filtered queries.
- **Graph Operations**: Dedicated methods for `add_edge()`, `traverse()`, and `get_graph_stats()`.

## Quick Start

```python
from longbow import LongbowClient

# Initialize client (defaults to localhost:3000)
client = LongbowClient()

# Insert data (Pandas DataFrame)
import pandas as pd
df = pd.DataFrame({
    "id": ["1", "2"],
    "vector": [[0.1, 0.2], [0.3, 0.4]],
    "metadata": ['{"key": "val"}', '{"key": "val2"}']
})
client.insert("my_dataset", df)

# Search
results = client.search("my_dataset", vector=[0.1, 0.2], k=5)
print(results) # Returns Pandas DataFrame

# High-Performance Download
table = client.download_arrow("my_dataset")
print(f"Rows: {table.num_rows}")

# Graph Traversal
paths = client.traverse("my_graph", start=101, max_hops=2)
```

## API Reference

### Data Operations

- `insert(dataset, data)`: Ingest Pandas DataFrames or lists of dicts.
- `search(dataset, vector, k, filters)`: Perform K-NN search with optional post-filtering.
- `download_arrow(dataset, filter)`: Download entire dataset as `pyarrow.Table`.
- `download_stream(dataset, filter)`: Generator yielding `pyarrow.RecordBatch`.

### Management & Control

- `create_namespace(name)`: Create a new namespace.
- `delete_namespace(name)`: Delete an entire namespace.
- `list_namespaces()`: List all active datasets.
- `snapshot()`: Trigger a manual snapshot (**Note**: Placeholder in SDK; backend implementation pending).
- `delete(dataset, ids)`: Delete records (**Note**: Record deletion by ID is currently a stub;
  works for deleting entire namespaces).

### Graph RAG

- `add_edge(dataset, subject, predicate, object, weight)`: Add directed edge.
- `traverse(dataset, start, max_hops)`: Breadth-first graph traversal.
- `get_graph_stats(dataset)`: Metrics on edges and node degrees.
