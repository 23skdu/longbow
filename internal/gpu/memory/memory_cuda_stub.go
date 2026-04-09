//go:build !gpu || !linux

package memory

import (
	"fmt"
	"unsafe"
)

// allocateCUDAMemory allocates CUDA memory (stub - CUDA not available)
func (p *GPUMemPool) allocateCUDAMemory(_ int64) (unsafe.Pointer, error) {
	return nil, fmt.Errorf("CUDA memory allocation not available: build with -tags gpu,linux to enable CUDA support")
}

// freeCUDAMemory frees CUDA memory (stub - CUDA not available)
func (p *GPUMemPool) freeCUDAMemory(_ unsafe.Pointer) error {
	return fmt.Errorf("CUDA memory free not available: build with -tags gpu,linux to enable CUDA support")
}

// cudaMemcpyHostToDevice copies data from host to device in CUDA (stub - CUDA not available)
func (p *GPUMemPool) cudaMemcpyHostToDevice(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("CUDA memcpy not available: build with -tags gpu,linux to enable CUDA support")
}

// cudaMemcpyDeviceToHost copies data from device to host in CUDA (stub - CUDA not available)
func (p *GPUMemPool) cudaMemcpyDeviceToHost(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("CUDA memcpy not available: build with -tags gpu,linux to enable CUDA support")
}

// cudaMemcpyDeviceToDevice copies data between CUDA devices (stub - CUDA not available)
func (p *GPUMemPool) cudaMemcpyDeviceToDevice(_, _ unsafe.Pointer, _ int64) error {
	return fmt.Errorf("CUDA device-to-device copy not available: build with -tags gpu,linux to enable CUDA support")
}

// cudaMemset sets CUDA device memory (stub - CUDA not available)
func (p *GPUMemPool) cudaMemset(_ unsafe.Pointer, _ int, _ int64) error {
	return fmt.Errorf("CUDA memset not available: build with -tags gpu,linux to enable CUDA support")
}
