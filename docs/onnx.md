# ONNX Integration in Longbow

Longbow integrates the **ONNX Runtime (ORT)** to provide high-performance inference for embeddings, cross-encoders, and custom ML-driven search scoring. By using ONNX, Longbow can execute models on a variety of hardware backends without changing the application logic.

## Architecture

Longbow's ONNX integration is designed for zero-copy data flow and hardware acceleration:

1. **Hardware Providers**: Longbow automatically detects and enables the best available provider:
   - **Metal (Darwin/ARM64)**: Uses the custom Metal shader backend for ultra-low latency on macOS.
   - **CUDA (Linux)**: Uses NVIDIA GPUs via the CUDA Execution Provider.
   - **CPU**: Fallback to highly optimized AVX-512/NEON SIMD kernels.
2. **Smart Pooling**: Longbow supports Mean, Max, and CLS pooling strategies for transformer-based models, allowing you to generate sentence embeddings directly from hidden states.
3. **Tokenizer Integration**: Includes a built-in WordPiece tokenizer for BERT/RoBERTa/MiniLM models, removing the need for external pre-processing.

## Model Management

Longbow can download models directly from Hugging Face using the CLI.

### Downloading a Model

```bash
longbow-cli download-model -repo <huggingface_repo_id> -dest <local_directory>
```

The CLI will attempt to download:

- `model.onnx`: The core model graph.
- `config.json`: Model configuration.
- `vocab.txt`: Tokenizer vocabulary.

### Recommended Model for Testing

For users looking for a balance of speed and accuracy, we recommend:

- **Model**: `sentence-transformers/all-MiniLM-L6-v2`
- **Why**:
  - **Small Size**: ~80MB in ONNX format.
  - **Fast**: Optimized for CPU and edge inference.
  - **Accurate**: State-of-the-art performance for its parameter count.

**Download Command:**

```bash
longbow-cli download-model -repo sentence-transformers/all-MiniLM-L6-v2 -dest models/all-mini
```

## Advanced Configuration

### Environment Variables

- `ONNX_RUNTIME_LIB_PATH`: Path to the `libonnxruntime` shared library if installed in a non-standard location.
- `LONGBOW_ONNX_THREADS`: Number of intra-op threads (default: number of logical cores).

### Pooling Modes

When using the `internal/onnx` package or the Python SDK, you can specify the pooling mode:

- `PoolingMean` (Default): Averaging all non-padding token embeddings.
- `PoolingCLS`: Using the first token ([CLS]) as the sequence representation.
- `PoolingMax`: Taking the maximum value across the sequence dimension for each feature.

## Cross-Encoding for Re-ranking

Longbow uses ONNX cross-encoders to refine search results. By providing a query and a set of candidate documents, Longbow can compute precise similarity scores that account for complex semantic interactions between words.

**Example SDK Usage:**

```python
from longbow import LongbowClient

client = LongbowClient("grpc://localhost:3000")
# Load cross-encoder model
client.load_model("models/cross-encoder", mode="cross-encoder")

# Search and re-rank
results = client.search("my-dataset", query="How does vector sharding work?", rerank=True)
```
