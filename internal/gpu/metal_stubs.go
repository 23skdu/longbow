//go:build !darwin && !metal
// +build !darwin,!metal

package gpu

import "fmt"

// NewMetalIndexImpl is a stub for non-Metal platforms
func NewMetalIndexImpl(cfg GPUConfig) (Index, error) {
	return nil, fmt.Errorf("Metal support not compiled in. Build on macOS or with the metal tag")
}
