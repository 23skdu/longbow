//go:build gpu && darwin && arm64

package gpu

import (
	"fmt"
	"unsafe"
)

// GPUMemPool is defined in memory_base.go

type GPUAllocation struct {
	Ptr       unsafe.Pointer
	Size      int64
	DevicePtr unsafe.Pointer
}

func (p *GPUMemPool) AllocateGPU(size int64) (unsafe.Pointer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.usedBytes+size > p.totalBytes {
		return nil, fmt.Errorf("GPU out of memory: requested %d bytes, available %d bytes", size, p.totalBytes-p.usedBytes)
	}

	switch p.backend {
	case BackendMetal:
		return p.allocateMetalMemoryImpl(size)
	case BackendCPU:
		return p.allocateCPUMemory(size)
	default:
		return nil, fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

func (p *GPUMemPool) FreeGPU(ptr unsafe.Pointer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	size, ok := p.allocations[ptr]
	if !ok {
		return fmt.Errorf("pointer not allocated: %v", ptr)
	}

	delete(p.allocations, ptr)
	p.usedBytes -= size

	switch p.backend {
	case BackendMetal:
		p.freeMetalMemoryImpl(ptr)
		return nil
	case BackendCPU:
		return nil
	default:
		return fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

func (p *GPUMemPool) GetTotalMemory() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalBytes
}

func (p *GPUMemPool) GetUsedMemory() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.usedBytes
}

func (p *GPUMemPool) GetAvailableMemory() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalBytes - p.usedBytes
}

func (p *GPUMemPool) allocateCPUMemory(size int64) (unsafe.Pointer, error) {
	ptr := make([]byte, size)
	p.allocations[unsafe.Pointer(&ptr[0])] = size
	p.usedBytes += size
	return unsafe.Pointer(&ptr[0]), nil
}

func (p *GPUMemPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allocations = nil
	return nil
}
