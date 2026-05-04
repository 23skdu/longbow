# Longbow Bench-Tool Reference

The `bench-tool` is a high-performance benchmarking utility designed to stress-test Longbow's ingestion and search capabilities across a wide range of data types and configurations.

## Usage

```bash
bench-tool [options]
```

## Options

| Flag | Type | Description |
| :--- | :--- | :--- |
| `-uri` | string | Longbow server URI (default: `127.0.0.1:3000`) |
| `-dataset` | string | Base name for the benchmark dataset (default: `bench_go`) |
| `-dim` | int | Vector dimensions (default: 128) |
| `-scale` | int | Total number of vectors to ingest (default: 1000) |
| `-dtype` | string | Data type: `float32`, `int8`, `int32`, `turboquant`, etc. |
| `-queries` | int | Number of queries to run for each search mode (default: 1000) |
| `-workers` | int | Number of concurrent search workers (default: 1) |
| `-drop` | bool | Automatically drop the dataset after benchmark completion (default: false) |
| `-json` | string | Path to save benchmark results as JSON |
| `-tq-bits` | int | Quantization bits for TurboQuant (2, 4, 8) (default: 4) |
| `-fbin` | string | Path to Arrow IPC binary file for ingestion (bypasses generation) |

## Benchmark Suite Coverage

For every dataset, `bench-tool` executes the following operations:

1. **DoPut**: Measures bulk ingestion throughput (vec/s and MB/s).
2. **DoGet**: Measures raw retrieval performance.
3. **Search Dense**: Standard HNSW vector search.
4. **Search Hybrid**: Combined vector and text search (BM25 + HNSW).
5. **Search Filtered**: Vector search with complex boolean filters (Arrow-based).
6. **Search Sparse**: Pure text/keyword search.
7. **Search ByID**: Primary key lookups.
8. **Search GraphRAG**: Multi-hop graph expansion + vector similarity.
9. **Search Geo**: Geospatial radius search.
10. **Search Temporal**: Point-in-time and range-based temporal queries.
11. **Search LearnedIndex**: Experimental learned index traversal.

## Example

```bash
# Benchmark 100k vectors of float32 in 128 dimensions from a binary file
./bin/bench-tool -dataset test_fbin -fbin data.fbin -queries 5000 -workers 8

# Benchmark with random generation
./bin/bench-tool -dataset test_rand -scale 100000 -dtype float32 -dim 128
```

## Output

Results are printed to stdout and saved as a JSON file if `-json` is specified. The output includes:

- **Throughput**: Vectors per second and MB per second.
- **Latency**: P50, P95, and P99 latencies in milliseconds.
- **Stability**: Success/failure counts for each query type.
- **Indexing Time**: Time taken for background HNSW building.
