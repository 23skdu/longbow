//go:build cgo
package tpu

/*
#include <stdint.h>
#include <stdlib.h>

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
*/
import "C"
import (
	"fmt"
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
func tpuGetDeviceInfo(deviceID int) (total, free uint64, err error) {
	if deviceID < 0 {
		return 0, 0, fmt.Errorf("invalid deviceID: %d", deviceID)
	}
	var info C.tpu_device_info_t
	status := C.tpu_get_device_info(C.int(deviceID), &info) // #nosec G115
	if status != C.TPU_SUCCESS {
		return 0, 0, fmt.Errorf("tpu_get_device_info failed with status %d", status)
	}
	return uint64(info.hbm_total), uint64(info.hbm_free), nil
}

// Wrapper for tpu_enqueue_batch
func tpuEnqueueBatch(deviceID int, data []float32) error {
	if len(data) == 0 {
		return nil
	}
	if len(data) > 2147483647 { // math.MaxInt32
		return fmt.Errorf("batch size too large: %d", len(data))
	}
	if deviceID < 0 || deviceID > 2147483647 {
		return fmt.Errorf("invalid deviceID: %d", deviceID)
	}
	status := C.tpu_enqueue_batch(C.int(deviceID), (*C.float)(unsafe.Pointer(&data[0])), C.int(len(data))) // #nosec G115
	if status != C.TPU_SUCCESS {
		return fmt.Errorf("tpu_enqueue_batch failed with status %d", status)
	}
	return nil
}
