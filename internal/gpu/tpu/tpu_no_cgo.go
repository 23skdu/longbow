//go:build !cgo
package tpu

import "fmt"

func tpuInitialize() error {
	return fmt.Errorf("TPU support requires CGO")
}

func tpuGetDeviceInfo(deviceID int) (total, free uint64, err error) {
	return 0, 0, fmt.Errorf("TPU support requires CGO")
}

func tpuEnqueueBatch(deviceID int, data []float32) error {
	return fmt.Errorf("TPU support requires CGO")
}
