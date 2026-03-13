# Reranking in Longbow

Longbow provides a multi-stage reranking pipeline to improve search result quality. This document explains the reranking architecture, available implementations, and how to configure them.

## Overview

Reranking is the process of re-ordering initial search results using a more sophisticated scoring mechanism. Longbow implements a two-stage pipeline:

1. **First Stage (Retrieval)**: Fast vector similarity search using HNSW, IVF-Flat, or DiskANN
2. **Second Stage (Reranking)**: More accurate scoring using ML models or advanced heuristics

## Architecture

```
Query ──▶ Vector Search ──▶ Initial Results ──▶ Reranker ──▶ Final Results
                │                                      │
                ▼                                      ▼
         [HNSW/IVF/DiskANN]                   [Heuristic/ONNX/Metal]
```

## Reranker Interface

All rerankers implement the `Reranker` interface:

```go
type Reranker interface {
    Rerank(ctx context.Context, query string, results []SearchResult) ([]SearchResult, error)
}
```

## Available Implementations

### 1. Heuristic Reranker (Default)

The default reranker uses a combination of:
- **Distance score** (70% weight): Inverse of vector distance
- **Text match score** (30% weight): Keyword matching on metadata fields

```go
reranker := &store.CrossEncoderReranker{
    ModelName: "default",
}
```

**Advantages**: No dependencies, fast, works with any metadata

**Use Cases**: Quick reranking, fallback when ML unavailable

### 2. ONNX Reranker

Uses ONNX Runtime for cross-encoder model inference.

```go
reranker, err := store.NewONNXReranker("/path/to/model.onnx")
```

**Advantages**: Full ML model support, industry standard

**Requirements**: ONNX Runtime library installed

### 3. Metal Reranker (Native Apple Silicon)

Native Metal-based inference for Apple Silicon Macs.

```go
reranker, err := store.NewMetalReranker("/path/to/model.onnx")
```

**Advantages**: No external dependencies, GPU-accelerated, optimized for M1/M2/M3

**Requirements**: macOS with Apple Silicon

### 4. WASM Reranker

WebAssembly-based inference for portable deployment.

```go
reranker, err := store.NewWASMModelRunner("/path/to/model.wasm")
```

**Advantages**: Cross-platform, sandboxed execution

## Configuration

### Using the Hybrid Pipeline

```go
pipeline := store.NewHybridPipeline(store.HybridPipelineConfig{
    Alpha:     0.5,      // 0 = keyword, 1 = vector
    RRFk:     60,        // RRF parameter
    UseColumnIndex: true,
})

// Set reranker
pipeline.SetReranker(reranker)

// Execute search
results, err := pipeline.Search(ctx, query, k)
```

### Factory Pattern

```go
factory := store.NewDefaultRerankerFactory()

config := map[string]interface{}{
    "type":       "metal",
    "model_path": "/models/ms-marco-MiniLM-L-6-v2.onnx",
}

reranker, err := factory.CreateReranker(config)
```

### Auto-Selection

```go
// Automatically selects best available reranker
reranker := store.AutoSelectReranker()
```

## Cross-Encoder Models

Longbow supports any ONNX-format cross-encoder model. Recommended models:

| Model | Dimensions | Speed | Quality |
|-------|-----------|-------|---------|
| ms-marco-MiniLM-L-6-v2 | 384 | Fast | Good |
| ms-marco-MiniLM-L-12-v2 | 384 | Medium | Better |
| cross-encoder-ms-marco-MiniLM-L-6-v2 | 384 | Fast | Good |
| cross-encoder-ms-marco-MiniLM-L-12-v2 | 384 | Medium | Better |
| bge-reranker-base | 768 | Medium | Best |

## Performance

### Batch Processing

For high throughput, use batch scoring:

```go
// Score multiple queries at once
queries := []string{"query1", "query2", "query3"}
documents := []string{"doc1", "doc2", "doc3"}

scores, err := reranker.ScoreBatch(ctx, queries, documents)
```

### Metrics

Longbow exports Prometheus metrics for reranking:

```
longbow_reranker_inference_duration_seconds
longbow_reranker_batch_size
longbow_reranker_scores_computed_total
longbow_reranker_errors_total
longbow_reranker_model_load_duration_seconds
```

## Examples

### Basic Usage

```go
// Create dataset with reranking
dataset, err := store.NewDataset(store.DatasetConfig{
    IndexType: "hnsw",
    Reranker: store.AutoSelectReranker(),
})

// Search automatically uses reranking
results, err := dataset.Search(ctx, "your query", 10)
```

### Custom Heuristic

```go
reranker := &store.CrossEncoderReranker{
    ModelName: "custom",
}

// Search result contains reranked results
results, err := dataset.Search(ctx, query, k)
for _, r := range results {
    fmt.Printf("ID: %d, Score: %.4f\n", r.ID, r.Score)
}
```

## Troubleshooting

### Metal Not Available

If Metal reranker fails:

```go
// Check availability
if !store.IsMetalAvailable() {
    // Fallback to heuristic
    reranker = &store.CrossEncoderReranker{}
}
```

### ONNX Load Errors

Ensure the model file is valid ONNX format:

```bash
python3 -c "import onnx; onnx.load('model.onnx')"
```

### Memory Issues

For large models, adjust batch size:

```go
config := map[string]interface{}{
    "type":       "metal", 
    "model_path": "/path/to/model.onnx",
    "max_batch_size": 32,  // Reduce if OOM
}
```

## See Also

- [ONNX Metal Runtime](onnx.md) - Native Metal ONNX implementation
- [Vector Search](vectorsearch.md) - First-stage search
- [Hybrid Pipeline](hybrid_search.md) - Combined search + reranking
