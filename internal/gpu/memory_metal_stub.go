//go:build !metal
// +build !metal

package gpu

import "C"

import (
	"fmt"
	"unsafe"
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
func NewMetalIndexImpl(cfg GPUConfig) (Index, error) {
	return nil, fmt.Errorf("Metal support not compiled in")
}
