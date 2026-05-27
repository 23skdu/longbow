//go:build gpu && darwin && arm64

package gpu

import (
	"fmt"

	"github.com/23skdu/longbow/internal/gpu/metal"
)

func newGPUIndexImpl(cfg GPUConfig, backend GPUBackend) (Index, error) {
	switch backend {
	case BackendMetal:
		return metal.NewMetalIndexOptimized(cfg)
	default:
		return nil, fmt.Errorf("unsupported GPU backend for Darwin/ARM64: %v", backend)
	}
}

// NewIndexWithConfig is maintained for backward compatibility
func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	if cfg.Dimension <= 0 {
		return nil, fmt.Errorf("invalid dimension %d", cfg.Dimension)
	}
	backend := DetectGPUBackend()
	return newGPUIndexImpl(cfg, backend)
}
