# Zero-Copy HNSW Implementation

This document describes the implementation of the Hierarchical Navigable Small
World (HNSW) index in Longbow, designed with a "Zero-Copy" philosophy.

## Architecture

We utilize the [coder/hnsw](https://github.com/coder/hnsw) library to manage the
graph structure. Crucially, the graph itself stores **only** int32 (aliased as
VectorID) identifiers. It does not store the vector data itself.

### Zero-Copy Lookup

The distance function required by HNSW is implemented to look up vector data
directly from the underlying Apache Arrow buffers. This avoids duplicating
vector data into the index memory space, significantly reducing memory overhead.

1. **ID Mapping**: A lightweight Location struct maps a VectorID to a

 specific BatchIndex and RowIndex within the Arrow Dataset.

1. **Direct Access**: When the distance between two nodes is calculated, the

 system resolves their locations and accesses the float32 slices directly
 from the Arrow FixedSizeList arrays.

## Supported Metrics

Currently, Longbow supports the following distance metric:

* **Euclidean Distance (L2)**: The straight-line distance between two vectors.

*Note: Cosine similarity and Dot Product are planned for future releases.*

## Usage

The HNSWIndex is attached to each Dataset. When records are added via
DoPut, they should be indexed by calling Index.Add(batchIdx, rowIdx).

```go
// Example usage within store
idx := NewHNSWIndex(dataset)
idx.Add(0, 0) // Add first row of first batch
```

## Performance Considerations

* **Memory Efficiency**: By avoiding data duplication, Longbow reduces the RAM
  footprint of the index by approximately 50% compared to standard HNSW
  implementations that copy vectors.
* **Concurrency**: The coder/hnsw library supports concurrent inserts and

 searches. Our wrapper protects the location mapping with a mutex.

## Analytics with DuckDB

Longbow integrates with DuckDB to provide powerful analytical capabilities on top of the stored Parquet snapshots. This
allows users to execute SQL queries directly against the historical data without needing to load it all into memory.

### Features

* **SQL Interface**: Execute standard SQL queries on your vector data.
* **Zero-Copy Reads**: DuckDB reads Parquet files directly, minimizing overhead.
* **Aggregations**: Perform complex aggregations (COUNT, AVG, SUM) efficiently.

### DuckDB Usage

The DuckDBAdapter exposes a QuerySnapshot method that takes a dataset name and a SQL query string. It returns the result
as a JSON string.

go
adapter := store.NewDuckDBAdapter("/data/path")
jsonResult, err := adapter.QuerySnapshot("my_dataset", "SELECT count(*) FROM my_dataset WHERE value > 0.5")
