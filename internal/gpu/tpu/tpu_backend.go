package tpu

import (
	"fmt"
	"sync"

	"github.com/23skdu/longbow/internal/gpu/types"
)

type TPUBackend struct {
	deviceID int
	mu       sync.Mutex
	hbm      *HBMManager
	vmem     *VMEMManager
	initialized bool
}

func NewTPUBackend(deviceID int) (*TPUBackend, error) {
	return &TPUBackend{
		deviceID: deviceID,
		hbm:      &HBMManager{total: 192 * 1024 * 1024 * 1024}, // 192GB for v7x
		vmem:     &VMEMManager{total: 16 * 1024 * 1024},      // 16MB SRAM scratchpad
	}, nil
}

func (b *TPUBackend) Initialize() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.initialized {
		return nil
	}
	if err := tpuInitialize(); err != nil {
		return err
	}
	b.initialized = true
	return nil
}

type HBMManager struct {
	total int64
	used  int64
	mu    sync.Mutex
}

func (m *HBMManager) Allocate(size int64) (uintptr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.used+size > m.total {
		return 0, fmt.Errorf("out of HBM: total %d, used %d, requested %d", m.total, m.used, size)
	}
	m.used += size
	// Placeholder for actual TPU allocation
	return uintptr(m.used), nil // #nosec G115 -- intentional conversion
}

type VMEMManager struct {
	total int64
	used  int64
	mu    sync.Mutex
}

func (b *TPUBackend) GetDeviceInfo() (*types.GPUInfo, error) {
	total, _, err := tpuGetDeviceInfo(b.deviceID)
	if err != nil {
		return nil, err
	}
	return &types.GPUInfo{
		Backend:  types.BackendTPU,
		Name:     "Google TPU v7x (Ironwood)",
		MemoryMB: int64(total / (1024 * 1024)), // #nosec G115 -- safe division
		DeviceID: b.deviceID,
		Vendor:   "Google",
	}, nil
}
