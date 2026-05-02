//go:build !cgo
package tpu

import "fmt"

func tpuInitialize() error {
	return fmt.Errorf("TPU support requires CGO")
}

func tpuGetDeviceInfo(_ int) (total, free uint64, err error) {
	return 0, 0, fmt.Errorf("TPU support requires CGO")
}

func tpuEnqueueBatch(_ int, _ []float32) error {
	return fmt.Errorf("TPU support requires CGO")
}
