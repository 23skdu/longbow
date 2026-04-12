//go:build !gpu

package gpu

import "fmt"

func newGPUIndexImpl(_ GPUConfig, _ GPUBackend) (Index, error) {
	return nil, fmt.Errorf("GPU support not compiled in: build with -tags gpu")
}

// NewIndexWithConfig is maintained for backward compatibility (stub)
func NewIndexWithConfig(_ GPUConfig) (Index, error) {
	return nil, fmt.Errorf("GPU support not compiled in: build with -tags gpu")
}

// NewMetalIndexImpl is maintained for backward compatibility (stub)
func NewMetalIndexImpl(_ GPUConfig) (Index, error) {
	return nil, fmt.Errorf("Metal support not compiled in: build with -tags gpu on macOS arm64")
}
