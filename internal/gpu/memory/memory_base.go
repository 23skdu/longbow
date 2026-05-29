package memory

import (
	"sync"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
)

type GPUMemPool struct {
	backend     types.GPUBackend
	deviceID    int32
	totalBytes  int64
	usedBytes   int64
	allocations map[unsafe.Pointer]int64
	mu          sync.RWMutex
}

// SetTotalMemory sets the total memory limit for this pool (from GPUConfig.MaxMemory or device query).
func (p *GPUMemPool) SetTotalMemory(total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.totalBytes = total
}

// NewGPUMemPool creates a new GPU memory pool.
// totalBytes should be set after creation via SetTotalMemory once the device limit is known.
func NewGPUMemPool(backend types.GPUBackend, deviceID int32) (*GPUMemPool, error) {
	pool := &GPUMemPool{
		backend:     backend,
		deviceID:    deviceID,
		totalBytes:  0,
		usedBytes:   0,
		allocations: make(map[unsafe.Pointer]int64),
	}

	return pool, nil
}
