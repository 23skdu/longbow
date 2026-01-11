# Longbow Python SDK

The official Python client for [Longbow](https://github.com/23skdu/longbow), a high-performance vector database.

## Features

- **High-Performance Ingestion**: Native Arrow Flight support for bulk loading.
- **Dask Integration**: Seamlessly handle large-scale datasets with Dask DataFrames.
- **Type Safety**: Pydantic models for request/response validation.
- **Easy Querying**: Returns results as Dask or Pandas DataFrames.

## Installation

```bash
pip install longbow
```

## Quick Start

```python
from longbow import LongbowClient
import dask.dataframe as dd
import pandas as pd
import numpy as np

# Connect to Longbow (Data Port 3000, Meta Port 3001)
client = LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")

# Create generic data
df = pd.DataFrame({
    "id": range(1000),
    "vector": [np.random.rand(128).tolist() for _ in range(1000)],
    "metadata": ["test" for _ in range(1000)]
})
ddf = dd.from_pandas(df, npartitions=2)

# Ingest Dask DataFrame
client.insert("my_dataset", ddf)

# Search
results = client.search(
    dataset="my_dataset",
    vector=np.random.rand(128).tolist(),
    k=5
)
print(results.compute()) # Materialize Dask result
```
