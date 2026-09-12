# Local ML Inference (WASM & ONNX)

Longbow supports high-performance local ML inference for embedding generation, reranking, and custom model execution. It provides two primary runtimes for local execution: **ONNX Runtime** (with native acceleration) and **WebAssembly (WASM)** (for sandboxed portability).

## Runtimes Overview

### 1. ONNX Runtime
Provides high-performance execution using native libraries. Longbow's ONNX integration is designed for zero-copy data flow and hardware acceleration.

- **Best for**: Maximum performance, GPU acceleration, and production workloads on supported hardware.
- **Backends**:
  - **Metal (Darwin/ARM64)**: Custom Metal shader backend for ultra-low latency on macOS.
  - **CUDA (Linux)**: NVIDIA GPUs via the CUDA Execution Provider.
  - **CPU**: Fallback to highly optimized AVX-512/NEON SIMD kernels.

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
| `LONGBOW_ONNX_THREADS` | (logical cores) | Number of intra-op threads for ONNX execution. |

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
- **Tokenizer**: Include a `vocab.txt` file in the model directory for native tokenization. Longbow includes a built-in WordPiece tokenizer for BERT/RoBERTa/MiniLM models, removing the need for external pre-processing.

### Model Management

Longbow can download models directly from Hugging Face using the CLI.

**Downloading a Model:**

```bash
longbow-cli download-model -repo <huggingface_repo_id> -dest <local_directory>
```

The CLI will attempt to download:

- `model.onnx`: The core model graph.
- `config.json`: Model configuration.
- `vocab.txt`: Tokenizer vocabulary.

**Recommended Model for Testing:**

For users looking for a balance of speed and accuracy, we recommend:

- **Model**: `sentence-transformers/all-MiniLM-L6-v2`
- **Why**:
  - **Small Size**: ~80MB in ONNX format.
  - **Fast**: Optimized for CPU and edge inference.
  - **Accurate**: State-of-the-art performance for its parameter count.

```bash
longbow-cli download-model -repo sentence-transformers/all-MiniLM-L6-v2 -dest models/all-mini
```

### Cross-Encoding for Re-ranking

Longbow uses ONNX cross-encoders to refine search results. By providing a query and a set of candidate documents, Longbow can compute precise similarity scores that account for complex semantic interactions between words.

```python
from longbow import LongbowClient

client = LongbowClient("grpc://localhost:3000")
client.load_model("models/cross-encoder", mode="cross-encoder")

results = client.search("my-dataset", query="How does vector sharding work?", rerank=True)
```

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
