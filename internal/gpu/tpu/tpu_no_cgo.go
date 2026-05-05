//go:build !cgo
package tpu

import "fmt"

func tpuInitialize() error {
	return fmt.Errorf("TPU support requires CGO")
}

func tpuGetDeviceInfo(_ int32) (total, free uint64, err error) {
	return 0, 0, fmt.Errorf("TPU support requires CGO")
}

func tpuEnqueueBatch(_ int32, _ []float32) error {
	return fmt.Errorf("TPU support requires CGO")
}
