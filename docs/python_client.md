# Longbow Python SDK

Longbow includes a high-performance Python SDK built on `pyarrow.flight`.

## Installation

```bash
pip install ./longbowclientsdk
```

## Features

- **Geospatial Search**: Native `geo_search()` for radius and bounding box queries.
- **TurboQuant (TQ) & Quantization**: Support for SIMD-accelerated bit-packing during ingestion.
- **Disk-ANN Offloading**: Configurable SSD-based storage for massive datasets.
- **Integrated Search**: Unified `search()` method supporting pure vector, filtered, and **Advanced SQL (CTEs/Subqueries)** queries.
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

# Geospatial Search (0.1.9)
results = client.geo_search(
    "cities", 
    center={"lat": 40.7, "lon": -74.0}, 
    radius_km=10.0,
    k=10
)
```

## API Reference

### Data Operations

- `insert(dataset, data)`: Ingest Pandas DataFrames or lists of dicts.
- `search(dataset, vector, k, filters)`: Perform K-NN search with optional post-filtering.
- `download_arrow(dataset, filter)`: Download entire dataset as `pyarrow.Table`.
- `download_stream(dataset, filter)`: Generator yielding `pyarrow.RecordBatch`.

### Management & Control

- `create_namespace(name)`: Create a new tenant isolation namespace.
- `create_dataset(name, dimensions, vector_type="float32", geo_enabled=True)`: Create a dataset with 0.1.9 features.
- `delete_namespace(name)`: Delete an entire namespace.
- `list_namespaces()`: List all active datasets.
- `snapshot()`: Trigger a manual snapshot.
- `delete(dataset, ids)`: Delete records by ID or delete entire dataset if IDs omitted.

### Graph RAG

- `add_edge(dataset, subject, predicate, object, weight)`: Add directed edge.
- `traverse(dataset, start, max_hops)`: Breadth-first graph traversal.
- `get_graph_stats(dataset)`: Metrics on edges and node degrees.
