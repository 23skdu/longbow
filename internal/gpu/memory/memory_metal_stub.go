//go:build !gpu || !darwin || !arm64 || !cgo

package memory

import (
	"fmt"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
)

// allocateMetalMemoryImpl is a stub for non-Metal platforms
func (p *GPUMemPool) allocateMetalMemoryImpl(_ int64) (unsafe.Pointer, error) {
	return nil, fmt.Errorf("metal memory allocation not available")
}

// freeMetalMemoryImpl is a stub for non-Metal platforms
func (p *GPUMemPool) freeMetalMemoryImpl(_ unsafe.Pointer) {
}

// metalMemcpyHostToDeviceImpl is a stub for non-Metal platforms
func (p *GPUMemPool) metalMemcpyHostToDeviceImpl(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("metal memcpy not available")
}

// metalMemcpyDeviceToHostImpl is a stub for non-Metal platforms
func (p *GPUMemPool) metalMemcpyDeviceToHostImpl(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("metal memcpy not available")
}

// NewMetalIndexImpl is a stub for non-Metal platforms
func NewMetalIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	return nil, fmt.Errorf("metal support not compiled in")
}

// MTLBufferPool stubs for non-Metal platforms
type MTLBufferPool struct{}

// PooledBuffer represents a pooled buffer value.
type PooledBuffer struct{}

// GetMTLBufferPool returns the mtl buffer pool.
func GetMTLBufferPool(_ interface{}) *MTLBufferPool {
	return &MTLBufferPool{}
}

// Get returns the .
func (p *MTLBufferPool) Get(_ int) interface{} {
	return nil
}

// Put runs the put operation.
func (p *MTLBufferPool) Put(_ interface{}) {}

// Stats runs the stats operation.
func (p *MTLBufferPool) Stats() (int64, int64) {
	return 0, 0
}

// NewPooledBuffer constructs a new PooledBuffer.
func NewPooledBuffer(_ interface{}, _ int, _ *MTLBufferPool) (*PooledBuffer, error) {
	return nil, fmt.Errorf("metal not available")
}

// Buffer runs the buffer operation.
func (pb *PooledBuffer) Buffer() interface{} {
	return nil
}

// Release runs the release operation.
func (pb *PooledBuffer) Release() {}
