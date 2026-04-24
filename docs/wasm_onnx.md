# Local ML Inference (WASM & ONNX)

Longbow supports high-performance local ML inference for embedding generation, reranking, and custom model execution. It provides two primary runtimes for local execution: **ONNX Runtime** (with native acceleration) and **WebAssembly (WASM)** (for sandboxed portability).

## Runtimes Overview

### 1. ONNX Runtime
Provides high-performance execution using native libraries. It supports CPU, CUDA (NVIDIA), and a custom **Metal** backend for Apple Silicon.

- **Best for**: Maximum performance, GPU acceleration, and production workloads on supported hardware.
- **Backends**: 
  - **CPU/CUDA**: Via standard ONNX Runtime shared libraries.
  - **Metal**: A custom-built, near-native performance backend for macOS (ARM64) optimized for transformer-based models.

### 2. WebAssembly (WASM)
Uses the [Wazero](https://wazero.io/) runtime for sandboxed, cross-platform inference.

- **Best for**: Edge deployments, untrusted model execution (security), and environments where native libraries cannot be installed.
- **Pros**: Zero-dependency, strictly sandboxed, runs anywhere (Linux, macOS, Windows).
- **Cons**: Slower than native acceleration (Metal/CUDA).

---

## Configuration

Enable local inference by setting the following environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LONGBOW_ML_RUNNER` | `wazero` | Set to `onnx` or `wazero` to select the runtime. |
| `ONNX_RUNTIME_LIB_PATH` | - | Path to `libonnxruntime.dylib` or `.so` (required for ONNX CPU/CUDA). |

---

## Architecture & Implementation

### ONNX Integration
- **`internal/onnx`**: A Go wrapper around `onnxruntime_go`.
- **`internal/onnx/metal`**: Custom Metal kernels for macOS (ARM64) providing optimized transformer execution.

### WASM Integration
- **`internal/wasm`**: Manages the Wazero runtime, handles WASM module loading, and manages tensor I/O mapping.

---

## Usage & Model Support

Longbow is optimized for transformer-based models:
- **Embedding Models**: BERT-style encoders (e.g., `bge-small-en`, `all-MiniLM-L6-v2`).
- **Reranking Models**: Cross-encoders (e.g., `bge-reranker-base`).

### Requirements
- **Format**: `.onnx` for ONNX runtime or `.wasm` for WASM runtime.
- **Tokenizer**: Include a `vocab.txt` file in the model directory for native tokenization.

### Performance Tuning (ONNX/Metal)
- **Batching**: Supported for both scoring and embeddings. Larger batches improve throughput but increase latency.
- **Pooling Strategies**:
  - `Mean`: Average of all token embeddings (default).
  - `CLS`: Use the `[CLS]` token embedding.
  - `Max`: Maximum value across all tokens.
  - *Configure via the `Session.SetPoolingMode` API.*

---

## Troubleshooting

### ONNX: Library Not Found
Ensure `libonnxruntime` is in your library path (e.g., `/usr/local/lib`) or set `ONNX_RUNTIME_LIB_PATH` explicitly.

### WASM: Out of Memory
If you encounter OOM errors in WASM, adjust the memory limits in `internal/wasm/runner.go`.

### Metal: Not Available
Ensure you are on Apple Silicon and the binary was built with `-tags gpu`.
