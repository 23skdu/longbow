package memory

import (
	"fmt"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
)

// GPUBackend common types from types subpackage for convenience
type GPUBackend = types.GPUBackend

const (
	// BackendCPU is a backend cpu constant.
	BackendCPU = types.BackendCPU
	// BackendCUDA is a backend cuda constant.
	BackendCUDA = types.BackendCUDA
	// BackendMetal is a backend metal constant.
	BackendMetal = types.BackendMetal
)

// AllocateGPU runs the allocate gpu operation.
func (p *GPUMemPool) AllocateGPU(size int64) (unsafe.Pointer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.totalBytes > 0 && p.usedBytes+size > p.totalBytes {
		return nil, fmt.Errorf("GPU out of memory: requested %d bytes, available %d bytes", size, p.totalBytes-p.usedBytes)
	}

	switch p.backend {
	case BackendMetal:
		return p.allocateMetalMemoryImpl(size)
	case BackendCUDA:
		return p.allocateCUDAMemory(size)
	case BackendCPU:
		return p.allocateCPUMemory(size)
	default:
		return nil, fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

// FreeGPU runs the free gpu operation.
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
	case BackendCUDA:
		return p.freeCUDAMemory(ptr)
	case BackendCPU:
		return nil
	default:
		return fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

// GetTotalMemory returns the total memory.
func (p *GPUMemPool) GetTotalMemory() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalBytes
}

// GetUsedMemory returns the used memory.
func (p *GPUMemPool) GetUsedMemory() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.usedBytes
}

// GetAvailableMemory returns the available memory.
func (p *GPUMemPool) GetAvailableMemory() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.totalBytes == 0 {
		return 0
	}
	return p.totalBytes - p.usedBytes
}

func (p *GPUMemPool) allocateCPUMemory(size int64) (unsafe.Pointer, error) {
	ptr := make([]byte, size)
	p.allocations[unsafe.Pointer(&ptr[0])] = size // #nosec G103
	p.usedBytes += size
	return unsafe.Pointer(&ptr[0]), nil // #nosec G103
}

// Close runs the close operation.
func (p *GPUMemPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allocations = nil
	return nil
}

// Backend runs the backend operation.
func (p *GPUMemPool) Backend() GPUBackend {
	return p.backend
}

// DeviceID runs the device id operation.
func (p *GPUMemPool) DeviceID() int32 {
	return p.deviceID
}

// MemcpyHostToDevice runs the memcpy host to device operation.
func (p *GPUMemPool) MemcpyHostToDevice(devicePtr, hostPtr unsafe.Pointer, size int64) error {
	switch p.backend {
	case BackendMetal:
		return p.metalMemcpyHostToDeviceImpl(hostPtr, devicePtr, size)
	case BackendCUDA:
		return p.cudaMemcpyHostToDevice(hostPtr, devicePtr, size)
	case BackendCPU:
		copy(unsafe.Slice((*byte)(devicePtr), size), unsafe.Slice((*byte)(hostPtr), size)) // #nosec G103
		return nil
	default:
		return fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

// MemcpyDeviceToHost runs the memcpy device to host operation.
func (p *GPUMemPool) MemcpyDeviceToHost(hostPtr, devicePtr unsafe.Pointer, size int64) error {
	switch p.backend {
	case BackendMetal:
		return p.metalMemcpyDeviceToHostImpl(devicePtr, hostPtr, size)
	case BackendCUDA:
		return p.cudaMemcpyDeviceToHost(devicePtr, hostPtr, size)
	case BackendCPU:
		copy(unsafe.Slice((*byte)(hostPtr), size), unsafe.Slice((*byte)(devicePtr), size)) // #nosec G103
		return nil
	default:
		return fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

// MemcpyDeviceToDevice runs the memcpy device to device operation.
func (p *GPUMemPool) MemcpyDeviceToDevice(dstPtr, srcPtr unsafe.Pointer, size int64) error {
	switch p.backend {
	case BackendCUDA:
		return p.cudaMemcpyDeviceToDevice(dstPtr, srcPtr, size)
	case BackendMetal:
		// Metal doesn't have a direct D2D implementation yet in the pool
		return fmt.Errorf("device-to-device copy not implemented for Metal")
	case BackendCPU:
		copy(unsafe.Slice((*byte)(dstPtr), size), unsafe.Slice((*byte)(srcPtr), size)) // #nosec G103
		return nil
	default:
		return fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

// Memset runs the memset operation.
func (p *GPUMemPool) Memset(ptr unsafe.Pointer, value int, size int64) error {
	switch p.backend {
	case BackendCUDA:
		return p.cudaMemset(ptr, value, size)
	case BackendCPU:
		buf := unsafe.Slice((*byte)(ptr), size) // #nosec G103
		for i := range buf {
			buf[i] = byte(value) // #nosec G115
		}
		return nil
	default:
		return fmt.Errorf("memset not implemented for backend: %v", p.backend)
	}
}
