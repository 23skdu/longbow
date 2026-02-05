//go:build !gpu

package gpu

import "fmt"

// NewIndexWithConfig creates a new GPU index with configuration (stub for non-GPU builds)
func NewIndexWithConfig(cfg GPUConfig) (Index, error) {
	return nil, fmt.Errorf("GPU support not compiled in: build with -tags gpu")
}

// NewMetalIndex creates a new Metal-based GPU index (stub for non-Metal builds)
func NewMetalIndex(cfg GPUConfig) (Index, error) {
	return nil, fmt.Errorf("Metal support not compiled in: build with -tags gpu on macOS arm64")
}
