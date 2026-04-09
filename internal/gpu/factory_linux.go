//go:build gpu && linux && amd64

package gpu

import (
	"fmt"

	"github.com/23skdu/longbow/internal/gpu/cuda"
	"github.com/23skdu/longbow/internal/gpu/faiss"
)

func newGPUIndexImpl(cfg GPUConfig, backend GPUBackend) (Index, error) {
	switch backend {
	case BackendCUDA:
		// Use raw CUDA index implementation
		// FAISS GPU requires faiss-gpu which isn't available in Ubuntu repo
		return cuda.NewCUDAIndexImpl(cfg)
	default:
		// Try FAISS for other backends (if available)
		if isFAISSAvailable() {
			return faiss.NewFaissGPUIndex(cfg)
		}
		return nil, fmt.Errorf("unsupported GPU backend for Linux/AMD64: %v", backend)
	}
}

func isFAISSAvailable() bool {
	return false // Disable FAISS by default since we don't have faiss-gpu
}

// NewIndexWithConfig is maintained for backward compatibility
func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	backend := DetectGPUBackend()
	return newGPUIndexImpl(cfg, backend)
}
