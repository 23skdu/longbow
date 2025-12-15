# Longbow Performance & Test Scripts

This directory contains scripts for testing and benchmarking the Longbow Vector Store.

## perf_test.py

A Python script using `pyarrow` and `dask` to interact with the Longbow server. It supports separate ports for data and metadata operations.

### Requirements

```bash
pip install pyarrow polars numpy dask[dataframe]
```

### Usage

Basic run:
```bash
python3 perf_test.py
```

Specify ports:
```bash
python3 perf_test.py --data-port 3000 --meta-port 3001
```

Apply Filters:
```bash
# Filter by ID
python3 perf_test.py --filter "id:<:50"

# Filter by Timestamp (RFC3339)
python3 perf_test.py --filter "timestamp:>:2024-01-01T00:00:00Z"
```

### Features
- **Dual-Port Support**: Simulates architecture where data and metadata planes are separated.
- **Dask Integration**: Automatically converts retrieved Arrow tables to Dask DataFrames for scalable analysis.
- **Filter Testing**: Easy CLI interface to test the server's filtering capabilities.
