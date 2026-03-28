//go:build !gpu
// +build !gpu

package gpu

import "C"

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
