//go:build gpu && linux

package cuda

/*
#cgo LDFLAGS: -lcudart -lcublas
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <stdlib.h>

// Initialize CUDA runtime
int lb_cudaInit() {
    cudaError_t err = cudaFree(0);
    return (err == cudaSuccess) ? 0 : -1;
}

// Get number of CUDA devices
int lb_cudaGetDeviceCount() {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    return (err == cudaSuccess) ? count : -1;
}

// Get device properties
int lb_cudaGetDeviceName(int device, char* name, int maxLen) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device);
    if (err != cudaSuccess) {
        return -1;
    }

    int i;
    for (i = 0; i < maxLen - 1 && prop.name[i] != '\0'; i++) {
        name[i] = prop.name[i];
    }
    name[i] = '\0';
    return 0;
}

// Get compute capability
int lb_cudaGetComputeCapability(int device, int* major, int* minor) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device);
    if (err != cudaSuccess) {
        return -1;
    }
    *major = prop.major;
    *minor = prop.minor;
    return 0;
}

// Get total memory
size_t lb_cudaGetTotalMem(int device) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device);
    return (err == cudaSuccess) ? prop.totalGlobalMem : 0;
}

// Get free and total memory
int lb_cudaGetMemInfo(size_t* free, size_t* total) {
    cudaError_t err = cudaMemGetInfo(free, total);
    return (err == cudaSuccess) ? 0 : -1;
}

// Set device
int lb_cudaSetDevice(int device) {
    cudaError_t err = cudaSetDevice(device);
    return (err == cudaSuccess) ? 0 : -1;
}

// Synchronize device
int lb_cudaDeviceSynchronize() {
    cudaError_t err = cudaDeviceSynchronize();
    return (err == cudaSuccess) ? 0 : -1;
}

// Reset device
int lb_cudaDeviceReset() {
    cudaError_t err = cudaDeviceReset();
    return (err == cudaSuccess) ? 0 : -1;
}

// Check if CUDA is available
int lb_cudaIsAvailable() {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    return (err == cudaSuccess && count > 0) ? 1 : 0;
}
*/
import "C"
import (
	"fmt"
)

// Init initializes the CUDA runtime
func Init() error {
	ret := C.lb_cudaInit()
	if ret != 0 {
		return fmt.Errorf("failed to initialize CUDA runtime")
	}
	return nil
}

// GetDeviceCount returns the number of CUDA-capable devices
func GetDeviceCount() int {
	count := C.lb_cudaGetDeviceCount()
	if count < 0 {
		return 0
	}
	return int(count)
}

// GetDeviceName returns the name of the specified CUDA device
func GetDeviceName(deviceID int) (string, error) {
	buf := make([]C.char, 256)
	ret := C.lb_cudaGetDeviceName(C.int(deviceID), &buf[0], C.int(len(buf)))
	if ret != 0 {
		return "", fmt.Errorf("failed to get device name for device %d", deviceID)
	}
	return C.GoString(&buf[0]), nil
}

// GetComputeCapability returns the compute capability of the specified device
func GetComputeCapability(deviceID int) (major, minor int, err error) {
	var cMajor, cMinor C.int
	ret := C.lb_cudaGetComputeCapability(C.int(deviceID), &cMajor, &cMinor)
	if ret != 0 {
		return 0, 0, fmt.Errorf("failed to get compute capability for device %d", deviceID)
	}
	return int(cMajor), int(cMinor), nil
}

// GetTotalMemory returns the total global memory of the specified device
func GetTotalMemory(deviceID int) uint64 {
	mem := C.lb_cudaGetTotalMem(C.int(deviceID))
	return uint64(mem)
}

// GetMemInfo returns the free and total memory of the current device
func GetMemInfo() (free, total uint64, err error) {
	var cFree, cTotal C.size_t
	ret := C.lb_cudaGetMemInfo(&cFree, &cTotal)
	if ret != 0 {
		return 0, 0, fmt.Errorf("failed to get memory info")
	}
	return uint64(cFree), uint64(cTotal), nil
}

// SetDevice sets the current CUDA device
func SetDevice(deviceID int) error {
	ret := C.lb_cudaSetDevice(C.int(deviceID))
	if ret != 0 {
		return fmt.Errorf("failed to set CUDA device %d", deviceID)
	}
	return nil
}

// Synchronize waits for the current device to finish all operations
func Synchronize() error {
	ret := C.lb_cudaDeviceSynchronize()
	if ret != 0 {
		return fmt.Errorf("failed to synchronize CUDA device")
	}
	return nil
}

// Reset resets the current CUDA device
func Reset() error {
	ret := C.lb_cudaDeviceReset()
	if ret != 0 {
		return fmt.Errorf("failed to reset CUDA device")
	}
	return nil
}

// IsAvailable checks if CUDA is available on the system
func IsAvailable() bool {
	return C.lb_cudaIsAvailable() == 1
}
