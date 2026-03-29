//go:build !gpu

package gpu

import "fmt"

func newGPUIndexImpl(cfg GPUConfig, backend GPUBackend) (Index, error) {
	return nil, fmt.Errorf("GPU support not compiled in: build with -tags gpu")
}

// NewIndexWithConfig is maintained for backward compatibility (stub)
func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	return nil, fmt.Errorf("GPU support not compiled in: build with -tags gpu")
}

// NewMetalIndexImpl is maintained for backward compatibility (stub)
func NewMetalIndexImpl(cfg GPUConfig) (Index, error) {
	return nil, fmt.Errorf("Metal support not compiled in: build with -tags gpu on macOS arm64")
}
