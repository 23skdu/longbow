//go:build gpu && darwin && arm64

package gpu

import (
	"fmt"

	"github.com/23skdu/longbow/internal/gpu/metal"
)

func newGPUIndexImpl(cfg GPUConfig, backend GPUBackend) (Index, error) {
	switch backend {
	case BackendMetal:
		return metal.NewMetalIndexImpl(cfg)
	default:
		return nil, fmt.Errorf("unsupported GPU backend for Darwin/ARM64: %v", backend)
	}
}

// NewIndexWithConfig is maintained for backward compatibility
func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	backend := DetectGPUBackend()
	return newGPUIndexImpl(cfg, backend)
}
