//go:build !cgo
package tpu

import (
	"fmt"
	"unsafe"
)

func tpuInitialize() error {
	return fmt.Errorf("TPU support requires CGO")
}

func tpuGetDeviceInfo(_ int32) (total, free uint64, err error) {
	return 0, 0, fmt.Errorf("TPU support requires CGO")
}

func tpuEnqueueBatch(_ int32, _ []float32) error {
	return fmt.Errorf("TPU support requires CGO")
}

func tpuMalloc(_ int32, _ int64) (unsafe.Pointer, error) {
	return nil, fmt.Errorf("TPU support requires CGO")
}

func tpuFree(_ unsafe.Pointer) error {
	return fmt.Errorf("TPU support requires CGO")
}

func tpuMemcpyH2D(_ unsafe.Pointer, _ []float32) error {
	return fmt.Errorf("TPU support requires CGO")
}

func tpuMemcpyD2H(_ []float32, _ unsafe.Pointer) error {
	return fmt.Errorf("TPU support requires CGO")
}

func tpuLaunchXLA(_ int32, _ string, _ []unsafe.Pointer) error {
	return fmt.Errorf("TPU support requires CGO")
}
