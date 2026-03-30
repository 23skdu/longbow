# GPU Acceleration Guide

Longbow supports GPU-accelerated vector search on NVIDIA (CUDA) and Apple Silicon (Metal) platforms. This guide covers setup, configuration, and integration.

## Quick Start

```bash
# Build with GPU support
go build -tags=gpu -o longbow cmd/longbow/main.go

# Run with GPU enabled
GPU_ENABLED=true ./longbow
```

## Platform Support

### Apple Silicon (Metal) - macOS ARM64

**Requirements:**
- macOS 12.0+ (Monterey or later)
- Apple Silicon (M1/M2/M3/M4)
- Xcode Command Line Tools
- Metal framework (included with macOS)

**Installation:**
```bash
# No additional dependencies required
# Metal and Accelerate are included with macOS

# Build with Metal GPU support
go build -tags=gpu -o longbow-metal ./cmd/longbow

# Run with GPU enabled
GPU_ENABLED=true ./longbow-metal
```

**Performance:**
- Optimized for Apple's unified memory architecture
- Uses vDSP (Accelerate) for efficient distance calculations
- No CPU↔GPU memory transfers required
- Expected 2-4x speedup vs CPU-only for large datasets

### NVIDIA GPUs (CUDA) - Linux

**Requirements:**
- NVIDIA GPU with compute capability 6.0+ (Pascal or newer)
- CUDA Toolkit 11.8 or later
- FAISS library with GPU support
- CGO enabled

**Installation** (Ubuntu/Debian):
```bash
# Install CUDA Toolkit
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-ubuntu2204.pin
sudo mv cuda-ubuntu2204.pin /etc/apt/preferences.d/cuda-repository-pin-600
sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/3bf863cc.pub
sudo add-apt-repository "deb https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/ /"
sudo apt update
sudo apt install cuda-toolkit-11-8

# Build FAISS with GPU support
git clone https://github.com/facebookresearch/faiss.git
cd faiss
cmake -B build -DFAISS_ENABLE_GPU=ON -DFAISS_ENABLE_PYTHON=OFF
make -C build -j
sudo make -C build install

# Build Longbow with CUDA
CGO_ENABLED=1 go build -tags=gpu -o longbow-cuda ./cmd/longbow
```

## Configuration

### Environment Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `GPU_ENABLED` | bool | `false` | Enable GPU acceleration |
| `GPU_DEVICE_ID` | int | `0` | GPU device ID for multi-GPU systems |
| `CUDA_HOME` | string | `/usr/local/cuda` | Path to CUDA installation |
| `LONGBOW_GPU_BACKEND` | string | `auto` | Force backend: `cuda`, `metal`, or `cpu` |

### Example Configuration

```bash
# Single GPU system
export GPU_ENABLED=true
export GPU_DEVICE_ID=0

# Multi-GPU system (use second GPU)
export GPU_ENABLED=true
export GPU_DEVICE_ID=1
```

## Build System

### Build Tags

| Tag | Description |
|-----|-------------|
| `-tags gpu` | Enable GPU support (auto-detects CUDA or Metal) |
| `-tags gpu,cuda` | Force CUDA backend |
| `-tags gpu,metal` | Force Metal backend |
| (no tags) | CPU-only build |

### Build Commands

```bash
# CPU-only (default, works everywhere)
go build ./cmd/longbow

# GPU-enabled (auto-detects backend)
go build -tags=gpu ./cmd/longbow

# Platform-specific
CGO_ENABLED=1 go build -tags gpu -o bin/longbow-cuda ./cmd/longbow   # Linux
CGO_ENABLED=1 go build -tags gpu -o bin/longbow-metal ./cmd/longbow  # macOS

# Test GPU package
go test -tags=gpu ./internal/gpu/...
```

## How It Works

### Hybrid CPU/GPU Search

Longbow uses a hybrid approach for optimal performance:

1. **GPU Candidate Generation**: GPU performs brute-force search to find top-(k×10) candidates
2. **CPU Refinement**: CPU HNSW graph filters tombstones and refines to top-k results
3. **Automatic Fallback**: If GPU fails, seamlessly falls back to CPU-only

### Automatic Initialization

- GPU index is initialized automatically when a dataset is created
- If initialization fails, Longbow logs a warning and continues with CPU-only
- No manual intervention required

## API Usage

### Go Integration

```go
import "github.com/23skdu/longbow/internal/gpu"

// Configuration
cfg := gpu.GPUConfig{
    Backend:       gpu.BackendCUDA,    // or BackendMetal, BackendCPU
    DeviceID:      0,
    Dimension:     128,
    Enabled:       true,
    SyncBatchSize: 1000,
}

// Create index with specific backend
index, err := gpu.NewIndexWithBackend(cfg, gpu.BackendCUDA)
if err != nil {
    log.Fatal(err)
}
defer index.Close()
```

### Backend Detection

```go
// Detect available GPU backend
backend := gpu.DetectGPUBackend()
switch backend {
case gpu.BackendCUDA:
    log.Println("Using NVIDIA CUDA")
case gpu.BackendMetal:
    log.Println("Using Apple Metal")
case gpu.BackendCPU:
    log.Println("Using CPU fallback")
}
```

### Search with GPU

```go
// Search returns IDs and distances
ids, distances, err := index.Search(query, k)
if err != nil {
    log.Fatal(err)
}

for i := 0; i < len(ids); i++ {
    log.Printf("ID: %d, Distance: %f", ids[i], distances[i])
}
```

### GPU Memory Management

```go
// Create memory pool
pool, err := gpu.NewGPUMemPool(gpu.BackendCUDA, 0)
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

// Monitor memory usage
total, free, used := pool.GetUsedMemory(), pool.GetTotalMemory()
log.Printf("GPU Memory: %d used / %d total", used, total)
```

## Recent Enhancements (v0.1.8)

### FP16 (Half-Precision) Metal Kernels

Added in v0.1.8-rc1: Half-precision Metal compute shaders for memory-bandwidth-bound workloads:

- `compute_l2_distances_fp16` - L2 distance with FP16 storage, FP32 accumulation
- `compute_cosine_similarity_fp16` - Cosine similarity with FP16
- `compute_dot_product_fp16` - Dot product with FP16

Benefits: ~50% memory reduction, 2x faster for memory-bandwidth-bound operations.

### SIMD/Warp-Level Reductions

Added in v0.1.8-rc1: Warp-level parallel reductions using Apple Silicon's 32-thread warps:

- `compute_l2_distances_warp` - Uses `simd_shuffle_down` for efficient warp reductions
- `compute_l2_and_topk_warp` - Fused distance + top-k kernel

Benefits: Reduced memory traffic, improved occupancy on Apple Silicon GPUs.

### Multiple Index Types (CUDA)

CUDA backend via FAISS supports multiple index types:

- **Flat** - Brute-force exact search (fastest for small datasets)
- **IVF-Flat** - Inverted File with flat quantization (balanced)
- **IVF-PQ** - IVF with Product Quantization (compressed, large datasets)

### Memory Pooling

Cross-backend GPU memory pooling implemented in `internal/gpu/memory/memory_pool.go`:

- Small buffer pool (≤64KB) for frequent allocations
- Large buffer pool for bulk operations
- Automatic hit/miss tracking

## Performance Considerations

### When to Use GPU

**Good for:**
- Large datasets (>100K vectors)
- High-dimensional vectors (>128 dimensions)
- Batch search operations
- Dedicated GPU hardware available

**Not recommended for:**
- Small datasets (<10K vectors)
- Low-dimensional vectors (<64 dimensions)
- Single-query workloads
- Shared GPU resources

### Optimal Vector Dimensions

GPU acceleration is most effective for:

| Dimensions | Expected Speedup |
|-----------|-----------------|
| 128-512 | 2-5x |
| 512-1536 | 5-15x |
| 1536+ | 10-50x |

### Memory Management

- GPU memory is limited compared to system RAM
- Each index consumes GPU memory proportional to dataset size
- Monitor with `longbow_gpu_memory_bytes` metric

## Troubleshooting

### GPU Initialization Failed

```
WARN  GPU initialization failed, using CPU-only  error="failed to initialize GPU resources"
```

**Causes:**
- CUDA not installed or misconfigured
- GPU device not available
- Insufficient GPU memory
- Wrong `GPU_DEVICE_ID`

**Solution:** Longbow automatically falls back to CPU. Check CUDA installation and GPU availability.

### Build Errors

```
undefined: gpu.NewIndexWithConfig
```

**Cause:** Building without `-tags=gpu` but GPU code is referenced.

**Solution:** Either build with `-tags=gpu` or ensure `GPU_ENABLED=false`.

### CUDA Version Mismatch

```
version `CUDA_X.Y' not found
```

**Cause:** FAISS compiled with different CUDA version than runtime.

**Solution:** Rebuild FAISS with matching CUDA version or update CUDA runtime.

### Metal Issues

**Error: "Metal framework not found"**
- Ensure macOS 12.0 or higher
- Verify Metal support: `system_profiler SPDisplaysDataType`

**Error: "Metal initialization failed"**
- Check that you're running on Apple Silicon (not Intel Mac)
- Verify Xcode Command Line Tools are installed

## Performance Benchmarks

See [Performance Documentation](performance.md) for detailed benchmarks comparing CPU vs Metal vs CUDA performance across all supported data types.

## Future Enhancements

- [ ] GPU-accelerated index building (HNSW construction)
- [ ] Multi-GPU support for large indexes
- [ ] cuVS/Tensor Core paths for CUDA FP16
- [ ] SoA memory layout optimization

## Changelog

| Version | Changes |
|---------|---------|
| v0.1.8 | FP16 Metal kernels, SIMD/warp reductions, memory pooling, IVF-PQ |
| v0.1.7 | Hybrid GPU/CPU search, circuit breaker |
| v0.1.6 | Metal GPU support, CUDA FAISS integration |

## References

- [FAISS GPU Documentation](https://github.com/facebookresearch/faiss/wiki/Faiss-on-the-GPU)
- [CUDA Installation Guide](https://docs.nvidia.com/cuda/cuda-installation-guide-linux/)
- [Apple Metal Performance Shaders](https://developer.apple.com/documentation/metalperformanceshaders)
