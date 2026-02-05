//go:build gpu && linux

package gpu

/*
#cgo LDFLAGS: -lcudart -lcublas
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <stdlib.h>
#include <string.h>

// CUDA initialization functions
int cudaInitDevice(int device) {
    cudaError_t err = cudaSetDevice(device);
    if (err != cudaSuccess) {
        return -1;
    }

    err = cudaFree(0);  // Initialize context
    if (err != cudaSuccess) {
        return -2;
    }

    return 0;
}

int cudaGetDeviceCountWrap(int* count) {
    cudaError_t err = cudaGetDeviceCount(count);
    if (err != cudaSuccess) {
        return -1;
    }
    return 0;
}

int cudaGetDevicePropertiesWrap(int device, char* name, size_t nameLen,
                                 int* major, int* minor, size_t* totalMem) {
    cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device);
    if (err != cudaSuccess) {
        return -1;
    }

    strncpy(name, prop.name, nameLen - 1);
    name[nameLen - 1] = '\0';
    *major = prop.major;
    *minor = prop.minor;
    *totalMem = prop.totalGlobalMem;

    return 0;
}

int cudaGetMemInfo(size_t* free, size_t* total) {
    cudaError_t err = cudaMemGetInfo(free, total);
    if (err != cudaSuccess) {
        return -1;
    }
    return 0;
}
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// CUDADevice represents a CUDA-capable GPU device
type CUDADevice struct {
	ID           int
	Name         string
	ComputeMajor int
	ComputeMinor int
	TotalMemory  uint64
	FreeMemory   uint64
}

// InitializeCUDA initializes the CUDA runtime for the specified device
func InitializeCUDA(deviceID int) error {
	ret := C.cudaInitDevice(C.int(deviceID))
	if ret != 0 {
		return fmt.Errorf("failed to initialize CUDA device %d: error code %d", deviceID, ret)
	}
	return nil
}

// GetCUDADeviceCount returns the number of CUDA-capable devices
func GetCUDADeviceCount() int {
	var count C.int
	ret := C.cudaGetDeviceCountWrap(&count)
	if ret != 0 {
		return 0
	}
	return int(count)
}

// GetCUDADeviceInfo retrieves information about a CUDA device
func GetCUDADeviceInfo(deviceID int) (*CUDADevice, error) {
	nameBuf := make([]C.char, 256)
	var major, minor C.int
	var totalMem C.size_t

	ret := C.cudaGetDevicePropertiesWrap(
		C.int(deviceID),
		&nameBuf[0],
		C.size_t(len(nameBuf)),
		&major,
		&minor,
		&totalMem,
	)

	if ret != 0 {
		return nil, fmt.Errorf("failed to get CUDA device %d properties", deviceID)
	}

	device := &CUDADevice{
		ID:           deviceID,
		Name:         C.GoString(&nameBuf[0]),
		ComputeMajor: int(major),
		ComputeMinor: int(minor),
		TotalMemory:  uint64(totalMem),
	}

	// Get current memory info
	var free, total C.size_t
	ret = C.cudaGetMemInfo(&free, &total)
	if ret == 0 {
		device.FreeMemory = uint64(free)
	}

	return device, nil
}

// GetCUDAMemoryInfo returns the free and total memory for the current device
func GetCUDAMemoryInfo(deviceID int) (free, total uint64, err error) {
	if err := InitializeCUDA(deviceID); err != nil {
		return 0, 0, err
	}

	var cFree, cTotal C.size_t
	ret := C.cudaGetMemInfo(&cFree, &cTotal)
	if ret != 0 {
		return 0, 0, fmt.Errorf("failed to get CUDA memory info")
	}

	return uint64(cFree), uint64(cTotal), nil
}

// IsCUDAAvailable checks if CUDA runtime is available on the system
func IsCUDAAvailable() bool {
	return GetCUDADeviceCount() > 0
}
