# ONNX Inference Integration

Longbow supports high-performance ML inference via ONNX Runtime and a custom Metal backend for Apple Silicon. This allows for local execution of embedding and reranking models without external dependencies.

## Architecture

The ONNX integration is split into two main components:
1. **`internal/onnx`**: A Go wrapper around `onnxruntime_go` for CPU and CUDA inference.
2. **`internal/onnx/metal`**: A custom Metal-accelerated backend for macOS (ARM64) that provides near-native performance for transformer-based models.

## Prerequisites

### ONNX Runtime

To use the CPU/CUDA backend, you must have the ONNX Runtime shared library (`libonnxruntime`) installed.

- **macOS**: `brew install onnxruntime`
- **Linux**: Download from [ONNX Runtime Releases](https://github.com/microsoft/onnxruntime/releases).

By default, Longbow searches for the library in `/usr/local/lib` and `/opt/homebrew/lib`. You can override this by setting the `ONNX_RUNTIME_LIB_PATH` environment variable.

### Metal (macOS only)

Metal acceleration is automatically enabled on Apple Silicon Macs when building with the `gpu` tag. No additional libraries are required.

## Configuration

Enable ONNX inference by setting the following environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LONGBOW_ML_RUNNER` | `wazero` | Set to `onnx` to enable ONNX inference. |
| `ONNX_RUNTIME_LIB_PATH` | - | Path to `libonnxruntime.dylib` or `.so`. |

## Supported Models

Longbow is optimized for:

- **Embedding Models**: BERT-style encoders (e.g., `bge-small-en`, `all-MiniLM-L6-v2`).
- **Reranking Models**: Cross-encoders (e.g., `bge-reranker-base`).

Models should be in `.onnx` format and include a `vocab.txt` file in the same directory for tokenization.

## Performance Tuning

### Batching

The Metal backend supports batch inference for both scoring and embeddings. Larger batches generally improve throughput but increase latency for individual requests.

### Pooling

Longbow supports multiple pooling strategies for embedding generation:

- `Mean`: Average of all token embeddings (default).
- `CLS`: Use the `[CLS]` token embedding.
- `Max`: Maximum value across all tokens.

Configure this via the `Session.SetPoolingMode` API.

## Troubleshooting

### Library Not Found

If you see `failed to initialize ONNX Runtime: library not found`, ensure that `libonnxruntime` is in your library path or set `ONNX_RUNTIME_LIB_PATH`.

### Metal Not Available

Ensure you are running on an Apple Silicon Mac and the binary was built with `-tags gpu`. Check `IsAvailable()` in the `metal` package for runtime detection.
