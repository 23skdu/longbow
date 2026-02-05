//go:build gpu && linux

package gpu

/*
#cgo LDFLAGS: -lcudart
#include <stdlib.h>
#include <cuda_runtime.h>

// Allocate GPU memory
void* cudaMallocWrap(size_t size) {
    void* ptr = NULL;
    cudaError_t err = cudaMalloc(&ptr, size);
    return (err == cudaSuccess) ? ptr : NULL;
}

// Free GPU memory
int cudaFreeWrap(void* ptr) {
    cudaError_t err = cudaFree(ptr);
    return (err == cudaSuccess) ? 0 : -1;
}

// Copy from host to device
int cudaMemcpyHtoD(void* dst, void* src, size_t size) {
    cudaError_t err = cudaMemcpy(dst, src, size, cudaMemcpyHostToDevice);
    return (err == cudaSuccess) ? 0 : -1;
}

// Copy from device to host
int cudaMemcpyDtoH(void* dst, void* src, size_t size) {
    cudaError_t err = cudaMemcpy(dst, src, size, cudaMemcpyDeviceToHost);
    return (err == cudaSuccess) ? 0 : -1;
}

// Copy from device to device
int cudaMemcpyDtoD(void* dst, void* src, size_t size) {
    cudaError_t err = cudaMemcpy(dst, src, size, cudaMemcpyDeviceToDevice);
    return (err == cudaSuccess) ? 0 : -1;
}

// Memset GPU memory
int cudaMemsetWrap(void* ptr, int value, size_t size) {
    cudaError_t err = cudaMemset(ptr, value, size);
    return (err == cudaSuccess) ? 0 : -1;
}
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// allocateCUDAMemory allocates memory on CUDA GPU
func (p *GPUMemPool) allocateCUDAMemory(size int64) (unsafe.Pointer, error) {
	ptr := C.cudaMallocWrap(C.size_t(size))
	if ptr == nil {
		return nil, fmt.Errorf("failed to allocate %d bytes on CUDA device %d", size, p.deviceID)
	}

	p.allocations[ptr] = size
	p.usedBytes += size

	return ptr, nil
}

// freeCUDAMemory frees memory on CUDA GPU
func (p *GPUMemPool) freeCUDAMemory(ptr unsafe.Pointer) error {
	ret := C.cudaFreeWrap(ptr)
	if ret != 0 {
		return fmt.Errorf("failed to free CUDA memory at %v", ptr)
	}
	return nil
}

// cudaMemcpyHostToDevice copies data from host to CUDA device
func (p *GPUMemPool) cudaMemcpyHostToDevice(hostPtr, devicePtr unsafe.Pointer, size int64) error {
	ret := C.cudaMemcpyHtoD(devicePtr, hostPtr, C.size_t(size))
	if ret != 0 {
		return fmt.Errorf("failed to copy %d bytes from host to CUDA device", size)
	}
	return nil
}

// cudaMemcpyDeviceToHost copies data from CUDA device to host
func (p *GPUMemPool) cudaMemcpyDeviceToHost(devicePtr, hostPtr unsafe.Pointer, size int64) error {
	ret := C.cudaMemcpyDtoH(hostPtr, devicePtr, C.size_t(size))
	if ret != 0 {
		return fmt.Errorf("failed to copy %d bytes from CUDA device to host", size)
	}
	return nil
}

// cudaMemcpyDeviceToDevice copies data between CUDA device buffers
func (p *GPUMemPool) cudaMemcpyDeviceToDevice(dstPtr, srcPtr unsafe.Pointer, size int64) error {
	ret := C.cudaMemcpyDtoD(dstPtr, srcPtr, C.size_t(size))
	if ret != 0 {
		return fmt.Errorf("failed to copy %d bytes on CUDA device", size)
	}
	return nil
}

// cudaMemset sets CUDA device memory to a value
func (p *GPUMemPool) cudaMemset(ptr unsafe.Pointer, value int, size int64) error {
	ret := C.cudaMemsetWrap(ptr, C.int(value), C.size_t(size))
	if ret != 0 {
		return fmt.Errorf("failed to memset CUDA memory")
	}
	return nil
}
