//go:build gpu && linux && amd64

package gpu

import (
	"fmt"

	"github.com/23skdu/longbow/internal/gpu/cuda"
	"github.com/23skdu/longbow/internal/gpu/tpu"
)

func newGPUIndexImpl(cfg GPUConfig, backend GPUBackend) (Index, error) {
	switch backend {
	case BackendCUDA:
		return cuda.NewCUDAIndexImpl(cfg)
	case BackendTPU:
		return tpu.NewTPUIndexImpl(cfg)
	default:
		return nil, fmt.Errorf("unsupported GPU backend for Linux/AMD64: %v", backend)
	}
}

func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	backend := DetectGPUBackend()
	return newGPUIndexImpl(cfg, backend)
}
