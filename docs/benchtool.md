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
| `-metrics` | string | Metrics server URI (default: `127.0.0.1:9090`) |
| `-dataset` | string | Base name for the benchmark dataset |
| `-dim` | int | Vector dimensions (default: 128) |
| `-count` | int | Total number of vectors to ingest (default: 1000) |
| `-type` | string | Data type: `float32`, `float64`, `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`, `complex64`, `complex128`, `turboquant2`, `turboquant4`, `turboquant8` |
| `-queries` | int | Number of queries to run for each search mode (default: 1000) |
| `-workers` | int | Number of concurrent search workers (default: 1) |
| `-drop` | bool | Automatically drop the dataset after benchmark completion (default: true) |
| `-wait` | bool | Wait for indexing to complete before starting searches (default: true) |
| `-bits` | int | Quantization bits for TurboQuant (default: 4) |

## Benchmark Suite Coverage

For every dataset, `bench-tool` executes the following operations:

1.  **DoPut**: Measures bulk ingestion throughput (vec/s and MB/s).
2.  **DoGet**: Measures raw retrieval performance.
3.  **Search Dense**: Standard HNSW vector search.
4.  **Search Hybrid**: Combined vector and text search (BM25 + HNSW).
5.  **Search Filtered**: Vector search with complex boolean filters (Arrow-based).
6.  **Search Sparse**: Pure text/keyword search.
7.  **Search ByID**: Primary key lookups.
8.  **Search GraphRAG**: Multi-hop graph expansion + vector similarity.
9.  **Search Geo**: Geospatial radius search.
10. **Search Temporal**: Point-in-time and range-based temporal queries.
11. **Search LearnedIndex**: Experimental learned index traversal.

## Example

```bash
# Benchmark 100k vectors of float16 in 384 dimensions
./bin/bench-tool -dataset test_f16 -type float16 -dim 384 -count 100000 -queries 5000 -workers 8 -drop
```

## Output

Results are printed to stdout and saved as a JSON file (e.g., `bench_results/bench_float32_128_1000.json`). The output includes:
-   **Throughput**: Vectors per second and MB per second.
-   **Latency**: P50, P95, and P99 latencies in milliseconds.
-   **Stability**: Success/failure counts for each query type.
