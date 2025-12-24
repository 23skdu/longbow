# Fuzzing Corpus for HNSW2

This directory contains the fuzzing corpus for testing the Arrow-native HNSW implementation.

## Structure

- `search/` - Fuzzing corpus for search operations
- `insert/` - Fuzzing corpus for insert operations
- `mixed/` - Fuzzing corpus for mixed workloads

## Running Fuzz Tests

```bash
# Fuzz search operations
go test -fuzz=FuzzSearch -fuzztime=1m ./internal/store/hnsw2

# Fuzz insert operations  
go test -fuzz=FuzzInsert -fuzztime=1m ./internal/store/hnsw2
```

## Corpus Generation

The corpus is automatically generated from:

1. Real workload patterns captured from production
2. Edge cases (empty queries, k=0, k>size, etc.)
3. Random valid inputs
