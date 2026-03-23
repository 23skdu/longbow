//go:build !darwin

package gpu

import (
	"fmt"
	"runtime"
	"unsafe"
)

// AllocateGPU allocates memory on the GPU
func (p *GPUMemPool) AllocateGPU(size int64) (unsafe.Pointer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.usedBytes+size > p.totalBytes {
		return nil, fmt.Errorf("GPU out of memory: requested %d bytes, available %d bytes", size, p.totalBytes-p.usedBytes)
	}

	switch p.backend {
	case BackendCUDA:
		return p.allocateCUDAMemory(size)
	case BackendCPU:
		return p.allocateCPUMemory(size)
	case BackendMetal:
		return p.allocateMetalMemoryImpl(size)
	default:
		return nil, fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

// FreeGPU frees GPU memory
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
	case BackendCUDA:
		return p.freeCUDAMemory(ptr)
	case BackendCPU:
		return p.freeCPUMemory(ptr)
	case BackendMetal:
		p.freeMetalMemoryImpl(ptr)
		return nil
	default:
		return fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

// MemcpyHostToDevice copies data from host memory to device memory
func (p *GPUMemPool) MemcpyHostToDevice(hostPtr, devicePtr unsafe.Pointer, size int64) error {
	switch p.backend {
	case BackendCUDA:
		return p.cudaMemcpyHostToDevice(hostPtr, devicePtr, size)
	case BackendCPU:
		runtime.KeepAlive(hostPtr)
		runtime.KeepAlive(devicePtr)
		return nil
	case BackendMetal:
		return p.metalMemcpyHostToDeviceImpl(hostPtr, devicePtr, size)
	default:
		return fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

// MemcpyDeviceToHost copies data from device memory to host memory
func (p *GPUMemPool) MemcpyDeviceToHost(devicePtr, hostPtr unsafe.Pointer, size int64) error {
	switch p.backend {
	case BackendCUDA:
		return p.cudaMemcpyDeviceToHost(devicePtr, hostPtr, size)
	case BackendCPU:
		runtime.KeepAlive(hostPtr)
		runtime.KeepAlive(devicePtr)
		return nil
	case BackendMetal:
		return p.metalMemcpyDeviceToHostImpl(devicePtr, hostPtr, size)
	default:
		return fmt.Errorf("unsupported backend: %v", p.backend)
	}
}

// GetTotalMemory returns total GPU memory in bytes
func (p *GPUMemPool) GetTotalMemory() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalBytes
}

// GetUsedMemory returns used GPU memory in bytes
func (p *GPUMemPool) GetUsedMemory() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.usedBytes
}

// GetAvailableMemory returns available GPU memory in bytes
func (p *GPUMemPool) GetAvailableMemory() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalBytes - p.usedBytes
}

// allocateCUDAMemory allocates CUDA memory (fallback stub)
// This stub is only used when building without -tags gpu or on non-Linux platforms.
// For actual CUDA support, build with: go build -tags gpu ./...
func (p *GPUMemPool) allocateCUDAMemory(_ int64) (unsafe.Pointer, error) {
	return nil, fmt.Errorf("CUDA support not compiled in. Build with -tags gpu on a Linux system with CUDA installed")
}

// allocateCPUMemory allocates CPU memory (fallback)
func (p *GPUMemPool) allocateCPUMemory(size int64) (unsafe.Pointer, error) {
	ptr := make([]byte, size)
	p.allocations[unsafe.Pointer(&ptr[0])] = size
	p.usedBytes += size
	return unsafe.Pointer(&ptr[0]), nil
}

// freeCUDAMemory frees CUDA memory (stub)
func (p *GPUMemPool) freeCUDAMemory(_ unsafe.Pointer) error {
	return fmt.Errorf("CUDA memory free not implemented yet")
}

// freeCPUMemory frees CPU memory
func (p *GPUMemPool) freeCPUMemory(_ unsafe.Pointer) error {
	return nil
}

// cudaMemcpyHostToDevice copies data from host to device in CUDA (stub)
func (p *GPUMemPool) cudaMemcpyHostToDevice(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("CUDA memcpy not implemented yet")
}

// metalMemcpyHostToDevice copies data from host to device in Metal (stub)
func (p *GPUMemPool) metalMemcpyHostToDevice(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("Metal memcpy not implemented yet")
}

// cudaMemcpyDeviceToHost copies data from device to host in CUDA (stub)
func (p *GPUMemPool) cudaMemcpyDeviceToHost(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("CUDA memcpy not implemented yet")
}

func (p *GPUMemPool) metalMemcpyDeviceToHost(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("Metal memcpy not implemented yet")
}

func (p *GPUMemPool) allocateMetalMemoryImpl(_ int64) (unsafe.Pointer, error) {
	return nil, fmt.Errorf("Metal memory allocation not available")
}

func (p *GPUMemPool) freeMetalMemoryImpl(_ unsafe.Pointer) {
}

func (p *GPUMemPool) metalMemcpyHostToDeviceImpl(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("Metal memcpy not available")
}

func (p *GPUMemPool) metalMemcpyDeviceToHostImpl(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("Metal memcpy not available")
}
