# Longbow Bench-Tool Reference

The `bench-tool` (also referred to as `benchmark-tool`) is a high-performance benchmarking utility designed to stress-test Longbow's ingestion and search capabilities across a wide range of data types and configurations. It is a standalone client driver written in Go using the `apache/arrow-go` library and the `SmartClient` SDK, ensuring minimal overhead and maximum throughput accuracy.

## Key Goals

- **Performance**: Remove Python allocation and dynamic typing overheads for small-batch throughput accuracy.
- **Type Support**: Support a comprehensive scalar type matrix (`float32`, `float64`, `float16`, `int8`, `uint8`, `int16`, `uint16`, `int32`, `uint32`, `int64`, `uint64`, `complex64`, `complex128`) with exact memory layout verification.
- **Full Coverage**: Test all search modalities including Vector, Hybrid, Filtered, Sparse, Geo, Temporal, GraphRAG, and Learned Index paths.

## Installation

Build the tool from source:

```bash
go build -o bin/bench-tool ./cmd/bench-tool
```

## Usage

```bash
./bin/bench-tool [options]
```

## Options

| Flag | Type | Description | Default |
| :--- | :--- | :--- | :--- |
| `-uri` | string | Longbow server URI (supports `grpc://`) | `127.0.0.1:3000` |
| `-dataset` | string | Base name for the benchmark dataset | `bench_go` |
| `-dim` | int | Vector dimensions (max: 3072) | `128` |
| `-scale` | int | Total number of vectors to ingest | `1000` |
| `-dtype` | string | Data type (see "Key Goals" for list) | `float32` |
| `-queries` | int | Number of queries to run for each search mode | `1000` |
| `-workers` | int | Number of concurrent search workers | `1` |
| `-drop` | bool | Automatically drop the dataset after completion | `false` |
| `-json` | string | Path to save benchmark results as JSON | `""` |
| `-tq-bits` | int | Quantization bits for TurboQuant (2, 4, 8) | `4` |
| `-fbin` | string | Path to Arrow IPC binary file for ingestion | `""` |

## Benchmark Suite Coverage

For every dataset, `bench-tool` executes the following operations in sequence:

1. **DoPut**: Measures bulk ingestion throughput (vec/s and MB/s).
2. **Indexing Wait**: Polls the server until background HNSW/Learned Index construction is complete.
3. **DoGet**: Measures raw retrieval performance.
4. **Search Dense**: Standard HNSW vector search.
5. **Search Hybrid**: Combined vector and text search (BM25 + HNSW).
6. **Search Filtered**: Vector search with complex boolean filters (Arrow-based).
7. **Search Sparse**: Pure text/keyword search.
8. **Search ByID**: Primary key lookups.
9. **Search GraphRAG**: Multi-hop graph expansion + vector similarity.
10. **Search Geo**: Geospatial radius search.
11. **Search Temporal**: Point-in-time and range-based temporal queries.
12. **Search LearnedIndex**: Experimental learned index traversal validation.

## Examples

### Random Generation Benchmark

```bash
# Benchmark 100k vectors of float32 in 128 dimensions
./bin/bench-tool -dataset test_rand -scale 100000 -dtype float32 -dim 128 -workers 8
```

### Binary File Ingestion

```bash
# Benchmark vectors from an Arrow IPC binary file
./bin/bench-tool -dataset test_fbin -fbin data.fbin -queries 5000 -workers 16
```

### TurboQuant Stress Test

```bash
# Test 1M vectors with 2-bit TurboQuant
./bin/bench-tool -scale 1000000 -dtype turboquant -tq-bits 2 -dim 384 -drop
```

## Batch Orchestration

For large-scale matrix testing, use the provided scripts:

- `scripts/benchmark_tool_incremental.sh`: Runs fully isolated incremental cycles through type lists.
- `scripts/unified_benchmark.py`: Python orchestrator for multi-host, multi-architecture (CPU/Metal/CUDA) validation.

## Output and Monitoring

The tool provides real-time progress for ingestion and indexing. Final results include:

- **Throughput**: Vectors per second and MB per second.
- **Latency**: P50, P95, and P99 latencies in milliseconds.
- **Accuracy**: Recall metrics for HNSW and Learned Index paths (if ground truth available).
- **Stability**: Success/failure counts for each query type.
