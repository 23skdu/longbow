//go:build !gpu || !darwin || !arm64
// +build !gpu !darwin !arm64

package memory

import (
	"fmt"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
)

// allocateMetalMemoryImpl is a stub for non-Metal platforms
func (p *GPUMemPool) allocateMetalMemoryImpl(_ int64) (unsafe.Pointer, error) {
	return nil, fmt.Errorf("Metal memory allocation not available")
}

// freeMetalMemoryImpl is a stub for non-Metal platforms
func (p *GPUMemPool) freeMetalMemoryImpl(_ unsafe.Pointer) {
}

// metalMemcpyHostToDeviceImpl is a stub for non-Metal platforms
func (p *GPUMemPool) metalMemcpyHostToDeviceImpl(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("Metal memcpy not available")
}

// metalMemcpyDeviceToHostImpl is a stub for non-Metal platforms
func (p *GPUMemPool) metalMemcpyDeviceToHostImpl(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("Metal memcpy not available")
}

// NewMetalIndexImpl is a stub for non-Metal platforms
func NewMetalIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	return nil, fmt.Errorf("Metal support not compiled in")
}
