//go:build gpu && linux

package gpu

/*
#include "cuda_backend.h"
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
	ret := C.lb_cuda_init_device(C.int(deviceID))
	if ret != 0 {
		return fmt.Errorf("failed to initialize CUDA device %d: error code %d", deviceID, ret)
	}
	return nil
}

// GetCUDADeviceCount returns the number of CUDA-capable devices
func GetCUDADeviceCount() int {
	var count C.int
	ret := C.lb_cuda_get_device_count(&count)
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

	ret := C.lb_cuda_get_device_properties(
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
	ret = C.lb_cuda_get_mem_info(&free, &total)
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
	ret := C.lb_cuda_get_mem_info(&cFree, &cTotal)
	if ret != 0 {
		return 0, 0, fmt.Errorf("failed to get CUDA memory info")
	}

	return uint64(cFree), uint64(cTotal), nil
}

// IsCUDAAvailable checks if CUDA runtime is available on the system
func IsCUDAAvailable() bool {
	return GetCUDADeviceCount() > 0
}
