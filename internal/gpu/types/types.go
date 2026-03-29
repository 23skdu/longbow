package types

import (
	"os"
	"runtime"
	"strconv"
	"time"
)

type GPUBackend int

const (
BackendCPU GPUBackend = iota
BackendCUDA
BackendMetal
BackendOpenCL
)

func (b GPUBackend) String() string {
	switch b {
	case BackendCPU: return "CPU"
	case BackendCUDA: return "CUDA"
	case BackendMetal: return "Metal"
	case BackendOpenCL: return "OpenCL"
	default: return "Unknown"
	}
}

type GPUConfig struct {
	Backend   GPUBackend
	DeviceID  int
	Dimension int
	Enabled   bool
	CUDAHome  string
	FAISSHome string
	MetalUnifiedMemory bool
	SyncBatchSize int
	SyncInterval  time.Duration
	MaxMemory     int64
}

func DefaultGPUConfig() GPUConfig {
	maxMemStr := os.Getenv("LONGBOW_MAX_MEMORY")
	maxMem := int64(0)
	if maxMemStr != "" {
		if m, err := strconv.ParseInt(maxMemStr, 10, 64); err == nil {
			maxMem = m
		}
	}
	return GPUConfig{
		Backend:            BackendCPU,
		DeviceID:           0,
		Dimension:          128,
		Enabled:            false,
		SyncBatchSize:      1000,
		SyncInterval:       5 * time.Second,
		MaxMemory:          maxMem,
		CUDAHome:           os.Getenv("CUDA_HOME"),
		FAISSHome:          os.Getenv("FAISS_HOME"),
		MetalUnifiedMemory: runtime.GOOS == "darwin",
	}
}

type GPUInfo struct {
	Name         string
	Backend      GPUBackend
	DeviceID     int
	MemoryMB     int64
	ComputeMajor int
	ComputeMinor int
}

type Index interface {
	Add(ids []int64, vectors []float32) error
	Search(vector []float32, k int) (ids []int64, distances []float32, err error)
	SearchBatch(vectors [][]float32, k int) (ids [][]int64, distances [][]float32, err error)
	Close() error
	Backend() GPUBackend
	GetDeviceInfo() (*GPUInfo, error)
	GetMemoryInfo() (total, free, used int64, err error)
	GetUtilization() (float32, error)
}

func DetectGPUBackend() GPUBackend {
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		return BackendMetal
	}
	// Simplified check for NVIDIA
	return BackendCPU
}
