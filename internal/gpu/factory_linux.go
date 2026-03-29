//go:build gpu && linux && amd64

package gpu

import (
	"fmt"

	"github.com/23skdu/longbow/internal/gpu/faiss"
)

func newGPUIndexImpl(cfg GPUConfig, backend GPUBackend) (Index, error) {
	switch backend {
	case BackendCUDA:
		// Try FAISS first for performance, fallback to raw CUDA if needed
		return faiss.NewFaissGPUIndex(cfg)
	default:
		return nil, fmt.Errorf("unsupported GPU backend for Linux/AMD64: %v", backend)
	}
}

// NewIndexWithConfig is maintained for backward compatibility
func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	backend := DetectGPUBackend()
	return newGPUIndexImpl(cfg, backend)
}
