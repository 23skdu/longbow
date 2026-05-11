package tpu

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
)

type TPUBackend struct {
	deviceID int32
	mu       sync.Mutex
	hbm      *HBMManager
	vmem     *VMEMManager
	initialized bool
}

func NewTPUBackend(deviceID int32) (*TPUBackend, error) {
	return &TPUBackend{
		deviceID: deviceID,
		hbm:      NewHBMManager(192 * 1024 * 1024 * 1024), // 192GB for v7x
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

// HBMManager manages High Bandwidth Memory on the TPU.
// It uses a slab-like allocation strategy to minimize fragmentation.
type HBMManager struct {
	total int64
	used  int64
	mu    sync.Mutex
	
	// Allocation map to track blocks for deallocation
	allocations map[unsafe.Pointer]int64
}

func NewHBMManager(total int64) *HBMManager {
	return &HBMManager{
		total:       total,
		allocations: make(map[unsafe.Pointer]int64),
	}
}

func (m *HBMManager) Allocate(deviceID int32, size int64) (unsafe.Pointer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.used+size > m.total {
		return nil, fmt.Errorf("out of HBM: total %d, used %d, requested %d", m.total, m.used, size)
	}
	
	ptr, err := tpuMalloc(deviceID, size)
	if err != nil {
		return nil, err
	}
	
	m.allocations[ptr] = size
	m.used += size
	
	return ptr, nil
}

func (m *HBMManager) Free(ptr unsafe.Pointer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	size, ok := m.allocations[ptr]
	if !ok {
		return fmt.Errorf("invalid HBM pointer: %v", ptr)
	}
	
	if err := tpuFree(ptr); err != nil {
		return err
	}
	
	delete(m.allocations, ptr)
	m.used -= size
	return nil
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
