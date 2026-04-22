# WASM Inference Integration

Longbow supports ML inference via WebAssembly (WASM) using the [Wazero](https://wazero.io/) runtime. This provides a sandboxed, cross-platform environment for running model inference without needing native libraries like ONNX Runtime or Metal.

## Architecture

The WASM integration is handled by:
- **`internal/wasm`**: Contains the `Runner` which manages the Wazero runtime, loads WASM modules, and handles input/output tensor mapping.

## Prerequisites

To use the WASM runner, you need a model compiled to WASM that follows the Longbow inference interface (typically a simplified version of a transformer encoder).

## Configuration

Enable the WASM runner by setting the following environment variable:

| Variable | Default | Description |
|----------|---------|-------------|
| `LONGBOW_ML_RUNNER` | `wazero` | Set to `wazero` to enable WASM inference. |

## Usage

WASM models are particularly useful for:
- **Edge Deployment**: Running on platforms where native ML libraries are unavailable.
- **Security**: Running untrusted models in a strictly sandboxed environment.
- **Portability**: The same WASM module can run on Linux, macOS, and Windows.

## Performance Considerations

While Wazero is a high-performance JIT compiler for WASM, it will generally be slower than native Metal or CUDA acceleration. It is recommended for scenarios where portability and security are prioritized over raw throughput.

## Troubleshooting

### Module Not Found
Ensure the `.wasm` file is in the path specified by your configuration.

### Memory Limits
If you encounter out-of-memory errors, you may need to adjust the Wazero memory limits in `internal/wasm/runner.go`.
