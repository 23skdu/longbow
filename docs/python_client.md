# Longbow Python SDK

Longbow now includes a dedicated Python SDK for easier interaction, capable of handling large datasets via Dask.

## Installation

The SDK is located in `pythonsdk/`. You can install it directly:

```bash
pip install ./pythonsdk
# OR
pip install -e ./pythonsdk  # Editable mode
```

## Features

- **Dask Integration**: Pass `dask.dataframe.DataFrame` directly to `client.insert()`. The SDK handles partition iteration and bulk upload.
- **Lazy Results**: `client.search()` returns Dask DataFrames (lazy) for consistent API, allowing scalable post-processing.
- **Robust Ingestion**: improved handling of stringified vectors (common in CSVs) and automatic Arrow schema inference.

## Quick Start

```python
from longbow import LongbowClient
import dask.dataframe as dd

client = LongbowClient(uri="grpc://localhost:3000")

# Load huge CSV with Dask
ddf = dd.read_csv("huge_data.csv") 

# Insert (iterates partitions automatically)
client.insert("my_dataset", ddf)

# Search
results = client.search("my_dataset", vector=[0.1, ...], k=10)
print(results.compute())
```

See [pythonsdk/README.md](../pythonsdk/README.md) for full details.
