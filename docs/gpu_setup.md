# GPU Setup Guide

This guide covers setting up GPU acceleration for Longbow vector search on both NVIDIA (CUDA) and Apple Silicon (Metal) platforms.

## Table of Contents

- [Overview](#overview)
- [NVIDIA CUDA Support](#nvidia-cuda-support)
- [Apple Metal Support](#apple-metal-support)
- [Building with GPU Support](#building-with-gpu-support)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## Overview

Longbow supports GPU-accelerated vector search on:

- **Linux AMD64**: NVIDIA GPUs via CUDA
- **macOS ARM64**: Apple Silicon GPUs via Metal

GPU acceleration provides significant performance improvements for:

- Large-scale vector search operations
- Batch queries
- High-dimensional vectors (128-1536 dimensions)

## NVIDIA CUDA Support

### Prerequisites

1. **NVIDIA GPU** with compute capability 3.5 or higher
2. **CUDA Toolkit** 11.0 or higher
3. **FAISS GPU** library (optional but recommended)

### Installation

#### 1. Install CUDA Toolkit

**Ubuntu/Debian:**

```bash
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.0-1_all.deb
sudo dpkg -i cuda-keyring_1.0-1_all.deb
sudo apt-get update
sudo apt-get -y install cuda
```

**RHEL/CentOS/Fedora:**

```bash
sudo dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel8/x86_64/cuda-rhel8.repo
sudo dnf -y install cuda
```

#### 2. Set Environment Variables

Add to your `~/.bashrc` or `~/.zshrc`:

```bash
export CUDA_HOME=/usr/local/cuda
export PATH=$CUDA_HOME/bin:$PATH
export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$LD_LIBRARY_PATH
```

Then reload:

```bash
source ~/.bashrc  # or ~/.zshrc
```

#### 3. Install FAISS GPU (Optional)

**Conda:**

```bash
conda install -c pytorch faiss-gpu
```

**From Source:**

```bash
git clone https://github.com/facebookresearch/faiss.git
cd faiss
mkdir build && cd build
cmake -DFAISS_ENABLE_GPU=ON -DCUDA_TOOLKIT_ROOT_DIR=$CUDA_HOME ..
make -j$(nproc)
sudo make install
```

Set FAISS environment variable:

```bash
export FAISS_HOME=/usr/local  # or where you installed FAISS
```

### Verification

Check CUDA installation:

```bash
nvidia-smi
nvcc --version
```

## Apple Metal Support

### Prerequisites

1. **macOS** 12.0 (Monterey) or higher
2. **Apple Silicon** Mac (M1, M2, M3, or newer)
3. **Xcode** Command Line Tools

### Installation

Metal support is built into macOS. No additional installation required, but ensure you have:

```bash
# Install Xcode Command Line Tools if not already installed
xcode-select --install
```

### Verification

Check Metal availability:

```bash
system_profiler SPDisplaysDataType | grep "Metal"
```

## Building with GPU Support

### Automatic GPU Detection

Build with automatic backend detection:

```bash
make build-gpu
```

This will:

- Build with **Metal** on macOS ARM64
- Build with **CUDA** on Linux AMD64

### Manual Backend Selection

#### CUDA (Linux AMD64)

```bash
make build-cuda
```

Or manually:

```bash
CGO_ENABLED=1 go build -tags gpu -o bin/longbow-cuda ./cmd/longbow
```

#### Metal (macOS ARM64)

```bash
make build-metal
```

Or manually:

```bash
CGO_ENABLED=1 go build -tags gpu -o bin/longbow-metal ./cmd/longbow
```

### Build Tags Reference

- `-tags gpu`: Enable GPU support (auto-detects CUDA or Metal)
- `-tags gpu,cuda`: Force CUDA backend
- `-tags gpu,metal`: Force Metal backend
- No tags: CPU-only build

## Configuration

### Environment Variables

| Variable | Description | Platform |
|----------|-------------|----------|
| `CUDA_HOME` | Path to CUDA installation | Linux |
| `FAISS_HOME` | Path to FAISS installation | Linux |
| `LONGBOW_GPU_BACKEND` | Force GPU backend (cuda/metal/cpu) | All |
| `LONGBOW_GPU_DEVICE` | Select GPU device ID (default: 0) | All |

### Runtime Configuration

Example Go code:

```go
import "github.com/23skdu/longbow/internal/gpu"

// Configure GPU backend
cfg := gpu.GPUConfig{
    Backend:   gpu.BackendCUDA,  // or BackendMetal, BackendCPU
    DeviceID:  0,
    Dimension: 128,
    Enabled:   true,
}

// Create GPU-accelerated index
index, err := gpu.NewIndex(cfg)
if err != nil {
    log.Fatal(err)
}
defer index.Close()

// Use the index...
```

## Troubleshooting

### CUDA Issues

**Error: "CUDA libraries not found"**

- Ensure `CUDA_HOME` is set correctly
- Check that `$CUDA_HOME/lib64/libcudart.so` exists
- Add CUDA to `LD_LIBRARY_PATH`

**Error: "no CUDA-capable device is detected"**

- Run `nvidia-smi` to verify GPU is detected
- Check driver installation: `cat /proc/driver/nvidia/version`
- Ensure user has permissions to access `/dev/nvidia*`

**Error: "CUDA out of memory"**

- Reduce batch size
- Use a GPU with more memory
- Enable memory pooling in configuration

### Metal Issues

**Error: "Metal framework not found"**

- Ensure macOS 12.0 or higher
- Verify Metal support: `system_profiler SPDisplaysDataType`

**Error: "Metal initialization failed"**

- Check that you're running on Apple Silicon (not Intel Mac)
- Verify Xcode Command Line Tools are installed

### Build Issues

**Error: "undefined: gpu.NewIndex"**

- Ensure you're building with `-tags gpu`
- Check that CGO is enabled: `CGO_ENABLED=1`

**Error: "cgo: C compiler not found"**

- Install build essentials: `sudo apt-get install build-essential` (Ubuntu)
- Or: `sudo dnf install gcc` (RHEL/Fedora)

### Performance Issues

**GPU not being used**

- Check GPU backend detection: `gpu.DetectGPUBackend()`
- Verify GPU is enabled in config: `cfg.Enabled = true`
- Check GPU memory utilization during operations

**Slower than CPU**

- For small datasets (<10k vectors), CPU may be faster due to transfer overhead
- Ensure vectors are properly batched
- Check that GPU memory is sufficient for your dataset

## Additional Resources

- [CUDA Toolkit Documentation](https://docs.nvidia.com/cuda/)
- [FAISS Documentation](https://github.com/facebookresearch/faiss/wiki)
- [Metal Programming Guide](https://developer.apple.com/metal/)
- [Longbow GPU Integration Guide](gpu_integration.md)
