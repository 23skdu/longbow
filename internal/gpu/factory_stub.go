// Covers !gpu, plus -tags gpu on platforms with no real backend
// implementation (i.e. anything that is not darwin/arm64 or linux/amd64).
//go:build !gpu || ((!darwin || !arm64) && (!linux || !amd64))

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
