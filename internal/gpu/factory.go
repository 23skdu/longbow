package gpu

import (
	"fmt"
)

// NewIndexWithBackend creates a GPU index with specified backend (delegates to existing implementation)
func NewIndexWithBackend(cfg GPUConfig, backend GPUBackend) (Index, error) {
	switch backend {
	case BackendCUDA:
		return NewIndexWithConfig(cfg)
	case BackendMetal:
		return NewMetalIndex(cfg)
	case BackendCPU, BackendOpenCL:
		return NewCPUIndex(cfg)
	default:
		return nil, fmt.Errorf("unsupported GPU backend: %v", backend)
	}
}

// NewIndex creates a GPU index with auto-detected backend (delegates to existing implementation)
func NewIndex(cfg GPUConfig) (Index, error) {
	if cfg.Backend == BackendCPU || !cfg.Enabled {
		return NewCPUIndex(cfg)
	}

	preferredBackend := DetectGPUBackend()

	switch preferredBackend {
	case BackendCUDA:
		return NewIndexWithConfig(cfg)
	case BackendMetal:
		return NewMetalIndex(cfg)
	default:
		return NewCPUIndex(cfg)
	}
}

// NewCPUIndex creates a CPU-only fallback index
type CPUIndex struct{}

func NewCPUIndex(cfg GPUConfig) (Index, error) {
	return &CPUIndex{}, nil
}

func (i *CPUIndex) Add(ids []int64, vectors []float32) error {
	return fmt.Errorf("CPU index not implemented in this stub")
}

func (i *CPUIndex) Search(vector []float32, k int) (ids []int64, distances []float32, err error) {
	return nil, nil, fmt.Errorf("CPU index not implemented in this stub")
}

func (i *CPUIndex) Close() error {
	return nil
}

func (i *CPUIndex) Backend() GPUBackend {
	return BackendCPU
}

func (i *CPUIndex) GetDeviceInfo() (*GPUInfo, error) {
	return &GPUInfo{
		Backend:  BackendCPU,
		Name:     "CPU",
		MemoryMB: 0,
	}, nil
}

func (i *CPUIndex) GetMemoryInfo() (total, free, used int64, err error) {
	return 0, 0, 0, nil
}

func (i *CPUIndex) GetDeviceCount() int {
	return 0
}

func (i *CPUIndex) Initialize(deviceID int) error {
	return nil
}
