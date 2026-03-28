//go:build !gpu && !metal && !darwin
// +build !gpu,!metal,!darwin

package gpu

import (
	"fmt"
	"unsafe"
)

// allocateCUDAMemory allocates CUDA memory (fallback stub)
func (p *GPUMemPool) allocateCUDAMemory(_ int64) (unsafe.Pointer, error) {
	return nil, fmt.Errorf("CUDA support not compiled in")
}

// freeCUDAMemory frees CUDA memory (stub)
func (p *GPUMemPool) freeCUDAMemory(_ unsafe.Pointer) error {
	return fmt.Errorf("CUDA memory free not implemented")
}

// cudaMemcpyHostToDevice copies data from host to device in CUDA (stub)
func (p *GPUMemPool) cudaMemcpyHostToDevice(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("CUDA memcpy not implemented")
}

// cudaMemcpyDeviceToHost copies data from device to host in CUDA (stub)
func (p *GPUMemPool) cudaMemcpyDeviceToHost(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("CUDA memcpy not implemented")
}

// metalMemcpyHostToDevice copies data from host to device in Metal (stub)
func (p *GPUMemPool) metalMemcpyHostToDevice(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("Metal memcpy not implemented")
}

func (p *GPUMemPool) metalMemcpyDeviceToHost(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("Metal memcpy not implemented")
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
