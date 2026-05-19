//go:build cgo
package tpu

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Simulated libtpu.so headers
typedef enum {
    TPU_SUCCESS = 0,
    TPU_ERROR_INTERNAL = 1,
    TPU_ERROR_OUT_OF_MEMORY = 2,
} tpu_status_t;

typedef struct {
    uint64_t hbm_total;
    uint64_t hbm_free;
} tpu_device_info_t;

// Stubs for libtpu.so functions
tpu_status_t tpu_initialize() { return TPU_SUCCESS; }
tpu_status_t tpu_get_device_info(int device_id, tpu_device_info_t* info) {
    if (info) {
        info->hbm_total = 192ULL * 1024 * 1024 * 1024;
        info->hbm_free = 190ULL * 1024 * 1024 * 1024;
    }
    return TPU_SUCCESS;
}
tpu_status_t tpu_enqueue_batch(int device_id, const float* data, int size) { return TPU_SUCCESS; }

// XLA-compiled kernel dispatch stubs
tpu_status_t tpu_malloc(int device_id, size_t size, void** ptr) {
    if (ptr) *ptr = malloc(size);
    return TPU_SUCCESS;
}
tpu_status_t tpu_free(void* ptr) {
    if (ptr) free(ptr);
    return TPU_SUCCESS;
}
tpu_status_t tpu_memcpy_h2d(void* dst, const void* src, size_t size) {
    if (dst && src) memcpy(dst, src, size);
    return TPU_SUCCESS;
}
tpu_status_t tpu_memcpy_d2h(void* dst, const void* src, size_t size) {
    if (dst && src) memcpy(dst, src, size);
    return TPU_SUCCESS;
}
tpu_status_t tpu_launch_xla(int device_id, const char* kernel_name, void** args, int num_args) {
    return TPU_SUCCESS;
}
*/
import "C"
import (
	"fmt"
	"math"
	"unsafe"
)

// Wrapper for tpu_initialize
func tpuInitialize() error {
	status := C.tpu_initialize()
	if status != C.TPU_SUCCESS {
		return fmt.Errorf("tpu_initialize failed with status %d", status)
	}
	return nil
}

// Wrapper for tpu_get_device_info
func tpuGetDeviceInfo(deviceID int32) (total, free uint64, err error) {
	var info C.tpu_device_info_t
	// #nosec G115
	status := C.tpu_get_device_info(C.int(deviceID), &info)
	if status != C.TPU_SUCCESS {
		return 0, 0, fmt.Errorf("tpu_get_device_info failed with status %d", status)
	}
	return uint64(info.hbm_total), uint64(info.hbm_free), nil
}

// Wrapper for tpu_enqueue_batch
func tpuEnqueueBatch(deviceID int32, data []float32) error {
	if len(data) == 0 {
		return nil
	}
	size := len(data)
	if size > math.MaxInt32 {
		return fmt.Errorf("batch size too large: %d", size)
	}
	
	// #nosec G115
	sizei32 := int32(size)
	
	// #nosec G115
	status := C.tpu_enqueue_batch(C.int(deviceID), (*C.float)(unsafe.Pointer(&data[0])), C.int(sizei32))
	if status != C.TPU_SUCCESS {
		return fmt.Errorf("tpu_enqueue_batch failed with status %d", status)
	}
	return nil
}

func tpuMalloc(deviceID int32, size int64) (unsafe.Pointer, error) {
	if size < 0 {
		return nil, fmt.Errorf("invalid tpuMalloc size: %d", size)
	}
	var ptr unsafe.Pointer
	cDeviceID := C.int(deviceID) // #nosec G115
	cSize := C.size_t(uint64(size)) // #nosec G115
	status := C.tpu_malloc(cDeviceID, cSize, &ptr)
	if status != C.TPU_SUCCESS {
		return nil, fmt.Errorf("tpu_malloc failed with status %d", status)
	}
	return ptr, nil
}

func tpuFree(ptr unsafe.Pointer) error {
	status := C.tpu_free(ptr)
	if status != C.TPU_SUCCESS {
		return fmt.Errorf("tpu_free failed with status %d", status)
	}
	return nil
}

func tpuMemcpyH2D(dst unsafe.Pointer, src []float32) error {
	if len(src) == 0 {
		return nil
	}
	// Use uint64 to avoid overflow before conversion to size_t
	size := uint64(len(src)) * 4
	s := unsafe.Pointer(&src[0])
	cSize := C.size_t(size) // #nosec G115
	status := C.tpu_memcpy_h2d(dst, s, cSize)
	if status != C.TPU_SUCCESS {
		return fmt.Errorf("tpu_memcpy_h2d failed with status %d", status)
	}
	return nil
}

func tpuMemcpyD2H(dst []float32, src unsafe.Pointer) error {
	if len(dst) == 0 {
		return nil
	}
	// Use uint64 to avoid overflow before conversion to size_t
	size := uint64(len(dst)) * 4
	d := unsafe.Pointer(&dst[0])
	cSize := C.size_t(size) // #nosec G115
	status := C.tpu_memcpy_d2h(d, src, cSize)
	if status != C.TPU_SUCCESS {
		return fmt.Errorf("tpu_memcpy_d2h failed with status %d", status)
	}
	return nil
}

func tpuLaunchXLA(deviceID int32, name string, args []unsafe.Pointer) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	
	cDeviceID := C.int(deviceID) // #nosec G115
	if len(args) == 0 {
		status := C.tpu_launch_xla(cDeviceID, cName, nil, 0)
		if status != C.TPU_SUCCESS {
			return fmt.Errorf("tpu_launch_xla failed with status %d", status)
		}
		return nil
	}

	numArgs := len(args)
	if numArgs > math.MaxInt32 {
		return fmt.Errorf("too many TPU arguments: %d", numArgs)
	}
	cNumArgs := C.int(numArgs) // #nosec G115

	status := C.tpu_launch_xla(cDeviceID, cName, &args[0], cNumArgs)
	if status != C.TPU_SUCCESS {
		return fmt.Errorf("tpu_launch_xla failed with status %d", status)
	}
	return nil
}
