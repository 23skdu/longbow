# ONNX Metal Runtime

Longbow includes a native Apple Metal-based ONNX Runtime implementation that provides high-performance ML inference on macOS without external dependencies.

## Overview

The Metal ONNX Runtime is designed specifically for Apple Silicon (M1/M2/M3) Macs, leveraging:

- **Metal Performance Shaders (MPS)** - GPU-accelerated matrix operations
- **Unified Memory** - Zero-copy access between CPU and GPU
- **Apple Neural Engine** - Hardware acceleration (via MPS)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Metal ONNX Engine                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │    ONNX     │───▶│    Metal     │───▶│   Output     │  │
│  │   Parser    │    │   Compute    │    │   Tensors    │  │
│  └─────────────┘    │   Pipeline   │    └──────────────┘  │
│         │           └──────────────┘           │           │
│         ▼                │                    ▼           │
│  ┌─────────────┐         ▼            ┌──────────────┐   │
│  │   Graph     │   ┌──────────────┐    │   Tensor     │   │
│  │  Optimizer  │──▶│   Tensor     │───▶│    Pool      │   │
│  └─────────────┘   │   Manager    │    └──────────────┘   │
│                    └──────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Supported Operations

- Matrix multiplication (MatMul)
- Element-wise operations (Add, Mul, Sub, Div)
- Activation functions (ReLU, Sigmoid, Tanh, Softmax)
- Layer normalization
- Attention mechanisms
- Pooling operations

### Memory Management

- **Tensor Pool**: Reuses GPU buffers to reduce allocations
- **Unified Memory**: Automatic CPU-GPU memory sharing
- **Reference Counting**: Automatic buffer lifecycle management

### Performance Optimizations

- Kernel fusion for sequential operations
- Batched inference for multiple queries
- Asynchronous command encoding
- Streaming inference support

## Usage

### Basic Initialization

```go
// Create Metal ONNX engine
engine, err := onnx.NewMetalEngine()
if err != nil {
    log.Fatal(err)
}
defer engine.Close()

// Check if Metal is available
if !onnx.IsMetalAvailable() {
    fmt.Println("Metal not available, using fallback")
}
```

### Loading Models

```go
// Load ONNX model
err = engine.LoadModel("/models/cross-encoder.onnx")
if err != nil {
    return err
}

// Get model info
info, err := engine.ModelInfo()
fmt.Printf("Inputs: %v, Outputs: %v\n", info.Inputs, info.Outputs)
```

### Inference

```go
// Single query-document scoring
score, err := engine.Score("your query", []string{"document text"})
if err != nil {
    return err
}
fmt.Printf("Relevance score: %.4f\n", score)

// Batch scoring
scores, err := engine.ScoreBatch(
    []string{"query1", "query2"},
    []string{"doc1", "doc2"},
)
```

### Integration with Reranker

```go
// Use Metal in reranker pipeline
reranker, err := store.NewMetalReranker("/models/ms-marco-MiniLM-L-6-v2.onnx")
if err != nil {
    return err
}

results, err := pipeline.SetReranker(reranker).Search(ctx, query, k)
```

## Configuration

### Engine Options

```go
opts := onnx.MetalEngineOptions{
    MaxBatchSize:      32,
    MaxSequenceLength: 512,
    EnableProfiling:   false,
    CacheDir:          "/tmp/onnx_cache",
}

engine, err := onnx.NewMetalEngine(opts)
```

### Session Options

```go
sessionOpts := onnx.SessionOptions{
    ExecutionMode:    onnx.ExecutionModeSequential,
    GraphOptimization: onnx.GraphOptimizationLevelAll,
    IntraOpThreads:    4,
    InterOpThreads:   2,
}

session, err := engine.NewSession(sessionOpts)
```

## Supported Models

Longbow's Metal ONNX runtime supports cross-encoder models in ONNX format:

| Model | Input Length | Output |
|-------|--------------|--------|
| ms-marco-MiniLM-L-6-v2 | 512 | 1 (score) |
| ms-marco-MiniLM-L-12-v2 | 512 | 1 (score) |
| bge-reranker-base | 512 | 1 (score) |

### Converting Models

Convert HuggingFace models to ONNX:

```python
from transformers import AutoModelForSequenceClassification
import torch

model = AutoModelForSequenceClassification.from_pretrained(
    "cross-encoder/ms-marco-MiniLM-L-6-v2"
)

# Export to ONNX
torch.onnx.export(
    model,
    (torch.tensor([[1]]), torch.tensor([[1]])),  # dummy input
    "model.onnx",
    input_names=["input_ids", "attention_mask"],
    output_names=["logits"],
    dynamic_axes={
        "input_ids": {0: "batch", 1: "sequence"},
        "attention_mask": {0: "batch", 1: "sequence"},
        "logits": {0: "batch"}
    }
)
```

## Metrics

The Metal ONNX runtime exports Prometheus metrics:

```go
// Included metrics
onnx_metal_inference_duration_seconds    // Histogram
onnx_metal_memory_used_bytes            // Gauge  
onnx_metal_batch_size                  // Histogram
onnx_metal_model_load_duration_seconds // Histogram
onnx_metal_inference_errors_total     // Counter
onnx_metal_tensor_allocations_total    // Counter
```

### Viewing Metrics

```bash
curl localhost:9090/metrics | grep onnx_metal
```

## Performance

### Benchmarks (M2 Pro)

| Operation | Latency |
|-----------|---------|
| Single inference (512 tokens) | ~2ms |
| Batch 32 inference | ~15ms |
| Model load | ~500ms |

### Tips for Performance

1. **Use batching**: Batch multiple queries for higher throughput
2. **Pre-warm**: Call `engine.Warmup()` before production use
3. **Cache models**: Models are cached in memory after first load
4. **Unified memory**: Ensure sufficient RAM (16GB+ recommended)

## Troubleshooting

### Metal Not Available

```go
if !onnx.IsMetalAvailable() {
    // Use fallback CPU or heuristic reranker
}
```

Common causes:
- Running on Intel Mac (use ONNX Runtime instead)
- Metal framework not found
- GPU disabled in system preferences

### Out of Memory

```go
// Reduce batch size
opts := onnx.MetalEngineOptions{
    MaxBatchSize: 16,  // Reduce from default 32
}
```

### Model Load Errors

Verify model format:
```bash
python3 -c "import onnx; m = onnx.load('model.onnx'); print(m.graph.input)"
```

## API Reference

### Types

```go
type MetalEngine interface {
    LoadModel(path string) error
    Score(query string, documents []string) ([]float32, error)
    ScoreBatch(queries, documents []string) ([][]float32, error)
    Warmup() error
    ModelInfo() (*ModelInfo, error)
    Close() error
}

type ModelInfo struct {
    Name        string
    Inputs      []TensorInfo
    Outputs     []TensorInfo
    Parameters  int64
}
```

### Functions

```go
// Check Metal availability
func IsMetalAvailable() bool

// Create engine with default options
func NewMetalEngine() (MetalEngine, error)

// Create engine with custom options
func NewMetalEngineWithOptions(opts MetalEngineOptions) (MetalEngine, error)

// Create Metal reranker
func NewMetalReranker(modelPath string) (Reranker, error)
```

## See Also

- [Reranking](rerank.md) - Using rerankers in Longbow
- [GPU Support](gpu.md) - Metal GPU integration
- [Vector Search](vectorsearch.md) - First-stage search
