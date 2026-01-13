# GPU Acceleration Guide

Longbow supports optional GPU acceleration for vector search operations using NVIDIA CUDA or CPU-optimized alternatives.

## Quick Start

### Enable GPU Acceleration

```bash
# Build with GPU support
go build -tags=gpu -o longbow cmd/longbow/main.go

# Run with GPU enabled
GPU_ENABLED=true GPU_DEVICE_ID=0 ./longbow
```

### CPU-Only Build (Default)

```bash
# Standard build (no GPU)
go build -o longbow cmd/longbow/main.go

# GPU settings are ignored
./longbow
```

## Configuration

### Environment Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `GPU_ENABLED` | bool | `false` | Enable GPU acceleration |
| `GPU_DEVICE_ID` | int | `0` | GPU device ID for multi-GPU systems |

### Example Configuration

```bash
# Single GPU system
export GPU_ENABLED=true
export GPU_DEVICE_ID=0

# Multi-GPU system (use second GPU)
export GPU_ENABLED=true
export GPU_DEVICE_ID=1
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

## Platform Support

### NVIDIA GPUs (CUDA)

**Status**: ✅ Supported

**Requirements**:

- NVIDIA GPU with compute capability 6.0+ (Pascal or newer)
- CUDA Toolkit 11.8 or later
- FAISS library with GPU support (`libfaiss_gpu.so`)
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
```

### Apple Silicon (M3/M4)

**Status**: ✅ Supported (Metal GPU)

**Requirements**:

- macOS 13.0+ (Ventura or later)
- Apple Silicon (M1/M2/M3/M4)
- Xcode Command Line Tools
- Metal framework (included with macOS)
- Accelerate framework (included with macOS)
- CGO enabled

**Implementation**: Uses Apple's native Metal Performance Shaders and Accelerate framework for GPU-accelerated vector operations.

**Installation**:

```bash
# No additional dependencies required!
# Metal and Accelerate are included with macOS

# Build with Metal GPU support
go build -tags=gpu -o longbow cmd/longbow/main.go

# Run with GPU enabled
GPU_ENABLED=true ./longbow
```

**Performance**:

- Optimized for Apple's unified memory architecture
- Uses vDSP (Accelerate) for efficient distance calculations
- No CPU↔GPU memory transfers required
- Expected 2-4x speedup vs CPU-only for large datasets

**Technical Details**:

- Distance calculation: `vDSP_distancesq` from Accelerate framework
- Memory management: Shared memory buffers (MTLResourceStorageModeShared)
- GPU selection: Uses system default Metal device
- Fallback: Automatic CPU fallback if Metal initialization fails

## Build System

### Build Tags

```go
//go:build gpu        // GPU-enabled code
//go:build !gpu       // CPU-only stub
```

### Build Commands

```bash
# CPU-only (default, works everywhere)
go build ./cmd/longbow

# GPU-enabled (requires CUDA)
go build -tags=gpu ./cmd/longbow

# Test GPU package
go test -tags=gpu ./internal/gpu/...

# Benchmark GPU vs CPU
go test -tags=gpu -bench=. ./internal/gpu/...
```

## Performance Considerations

### When to Use GPU

✅ **Good for**:

- Large datasets (>100K vectors)
- High-dimensional vectors (>128 dimensions)
- Batch search operations
- Dedicated GPU hardware available

❌ **Not recommended for**:

- Small datasets (<10K vectors)
- Low-dimensional vectors (<64 dimensions)
- Single-query workloads
- Shared GPU resources

### Memory Management

- GPU memory is limited compared to system RAM
- Each index consumes GPU memory proportional to dataset size
- Future versions will support GPU memory limits and LRU eviction

## Troubleshooting

### GPU Initialization Failed

```text
WARN  GPU initialization failed, using CPU-only  error="failed to initialize GPU resources"
```

**Causes**:

- CUDA not installed or misconfigured
- GPU device not available
- Insufficient GPU memory
- Wrong `GPU_DEVICE_ID`

**Solution**: Longbow automatically falls back to CPU. Check CUDA installation and GPU availability.

### Build Errors

```text
undefined: gpu.NewIndexWithConfig
```

**Cause**: Building without `-tags=gpu` but GPU code is referenced.

**Solution**: Either build with `-tags=gpu` or ensure `GPU_ENABLED=false`.

### CUDA Version Mismatch

```text
version `CUDA_X.Y' not found
```

**Cause**: FAISS compiled with different CUDA version than runtime.

**Solution**: Rebuild FAISS with matching CUDA version or update CUDA runtime.

## Monitoring

### Logs

GPU status is logged during startup:

```text
INFO  GPU acceleration enabled  device=0 dimensions=128
```

Or if GPU fails:

```text
WARN  GPU initialization failed, using CPU-only  error="..."
```

### Metrics

Future versions will include:

- `longbow_gpu_search_duration_seconds`
- `longbow_gpu_memory_bytes`
- `longbow_gpu_fallback_total`

## API

### Internal API (for developers)

```go
// Initialize GPU for an index
err := hnswIndex.InitGPU(deviceID, logger)

// Hybrid search (GPU + CPU)
results := hnswIndex.SearchHybrid(query, k)

// Check if GPU is enabled
if hnswIndex.IsGPUEnabled() {
    // GPU active
}

// Sync vectors to GPU
err := hnswIndex.SyncGPU(ids, vectors)

// Close GPU resources
err := hnswIndex.CloseGPU()
```

## Future Enhancements

- [ ] GPU memory management and limits
- [ ] Multi-GPU support for large indexes
- [ ] GPU-accelerated index building
- [ ] Performance metrics and monitoring
- [ ] Configurable hybrid search parameters

## References

- [FAISS GPU Documentation](https://github.com/facebookresearch/faiss/wiki/Faiss-on-the-GPU)
- [CUDA Installation Guide](https://docs.nvidia.com/cuda/cuda-installation-guide-linux/)
- [Apple Metal Performance Shaders](https://developer.apple.com/documentation/metalperformanceshaders)
