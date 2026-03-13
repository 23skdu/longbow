# Native ONNX Metal Runtime for Longbow Reranking

## Overview

This document outlines the architecture and implementation plan for a native Apple Metal-based ONNX Runtime for Longbow's reranking pipeline, enabling high-performance ML inference on macOS without external dependencies.

## Goals

1. **Zero External Dependencies** - Pure Go + Metal framework bindings
2. **Native Apple Silicon** - Optimized for M1/M2/M3 Macs
3. **High Performance** - GPU-accelerated matrix operations
4. **Production Ready** - Tests, metrics, documentation

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Reranker Pipeline                       │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌─────────────┐  │
│  │   Input      │───▶│   Metal     │───▶│   Output    │  │
│  │   Processing │    │   ONNX      │    │   Scores    │  │
│  └──────────────┘    │   Engine    │    └─────────────┘  │
│                      └──────────────┘                      │
│                           │                                │
│         ┌─────────────────┼─────────────────┐             │
│         ▼                 ▼                 ▼             │
│  ┌────────────┐    ┌───────────┐    ┌────────────┐      │
│  │  Tokenizer │    │  Compute   │    │  Tensor    │      │
│  │  (BPE)     │    │  Pipeline  │    │  Pool      │      │
│  └────────────┘    └───────────┘    └────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### Core Modules

1. **MetalONNXEngine** - Main inference engine
2. **Tokenizers** - BPE/WordPiece tokenization
3. **TensorPool** - GPU memory management
4. **ComputePipeline** - Matrix operations via MPS (Metal Performance Shaders)

## Implementation Plan

### Phase 1: Core Infrastructure

- [ ] `internal/onnx/metal/engine.go` - Main Metal ONNX engine
- [ ] `internal/onnx/metal/tensor.go` - GPU tensor management
- [ ] `internal/onnx/metal/pipeline.go` - Compute pipeline
- [ ] `internal/onnx/tokenizer.go` - BPE tokenization

### Phase 2: Model Support

- [ ] Cross-encoder score computation
- [ ] Batch inference optimization
- [ ] Attention mechanism (simplified)
- [ ] Model loading (ONNX format)

### Phase 3: Testing & Metrics

- [ ] Unit tests for all components
- [ ] Fuzz tests for tokenizer
- [ ] Integration tests with real models
- [ ] Prometheus metrics export
- [ ] Benchmark tests

### Phase 4: Documentation

- [ ] `docs/rerank.md` - Reranker usage guide
- [ ] `docs/onnx.md` - ONNX Metal runtime docs
- [ ] API reference

## Technical Details

### Metal Performance Shaders (MPS)

We'll use MPS for:
- Matrix multiplication (`MPSMatrixMultiplication`)
- Element-wise operations (`MPSMatrixElementwise`)
- Softmax (`MPSMatrixSoftmax`)
- Activation functions (`MPSMatrixNeuron`)

### Memory Management

- Tensor pool to reduce allocations
- Unified memory (Apple Silicon)
- Reference counting for buffers

### Tokenization

- Simple BPE implementation in Go
- Support for common vocabularies
- Fallback to whitespace tokenization

## API Design

```go
// MetalONNXEngine is the main inference engine
type MetalONNXEngine struct {
    device    *MTLDevice
    queue     *MTLCommandQueue
    pool      *TensorPool
    pipeline  *ComputePipeline
}

// NewMetalONNXEngine creates a new Metal ONNX engine
func NewMetalONNXEngine() (*MetalONNXEngine, error)

// LoadModel loads an ONNX model
func (e *MetalONNXEngine) LoadModel(path string) error

// Score computes relevance scores for query-document pairs
func (e *MetalONNXEngine) Score(query string, documents []string) ([]float32, error)

// ScoreBatch scores multiple queries
func (e *MetalONNXEngine) ScoreBatch(queries []string, documents []string) ([][]float32, error)

// Close releases resources
func (e *MetalONNXEngine) Close() error

// IsAvailable checks if Metal is available
func IsAvailable() bool

// MetalReranker wraps MetalONNXEngine for reranking
type MetalReranker struct {
    engine    *MetalONNXEngine
    tokenizer Tokenizer
}
```

## Testing Strategy

### Unit Tests

- Tensor creation and manipulation
- Matrix operations correctness
- Tokenizer accuracy
- Pool lifecycle

### Fuzz Tests

- Tokenizer with random inputs
- Model inference edge cases
- Memory pressure scenarios

### Integration Tests

- Real ONNX model loading
- End-to-end reranking pipeline
- Performance benchmarks

## Metrics

```go
// Prometheus metrics
var (
    onnxInferenceDuration = prometheus.NewHistogramVec(...)
    onnxMemoryUsed = prometheus.NewGauge(...)
    onnxBatchSize = prometheus.NewHistogram(...)
    onnxModelLoadDuration = prometheus.NewHistogram(...)
    onnxInferenceErrors = prometheus.NewCounter(...)
)
```

## Future Enhancements

1. **Full ONNX Opset Support** - More operators
2. **Quantization** - INT8/FP16 inference
3. **Multi-model** - Ensemble support
4. **Streaming** - Real-time inference
