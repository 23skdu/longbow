package gpu

import (
	"sync"
	"unsafe"
)

type GPUMemPool struct {
	backend     GPUBackend
	deviceID    int
	totalBytes  int64
	usedBytes   int64
	allocations map[unsafe.Pointer]int64
	mu          sync.RWMutex
}

func NewGPUMemPool(backend GPUBackend, deviceID int) (*GPUMemPool, error) {
	pool := &GPUMemPool{
		backend:     backend,
		deviceID:    deviceID,
		totalBytes:  0,
		usedBytes:   0,
		allocations: make(map[unsafe.Pointer]int64),
	}

	info, err := GetDeviceInfo(deviceID)
	if err == nil {
		if info.MemoryMB > 0 {
			pool.totalBytes = info.MemoryMB * 1024 * 1024
		}
	}

	return pool, nil
}
