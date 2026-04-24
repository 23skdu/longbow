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
	BackendTPU
)

func (b GPUBackend) String() string {
	switch b {
	case BackendCPU:
		return "CPU"
	case BackendCUDA:
		return "CUDA"
	case BackendMetal:
		return "Metal"
	case BackendTPU:
		return "TPU"
	default:
		return "Unknown"
	}
}

type GPUConfig struct {
	Backend            GPUBackend
	DeviceID           int
	Dimension          int
	Enabled            bool
	CUDAHome           string
	FAISSHome          string
	MetalUnifiedMemory bool
	SyncBatchSize      int
	SyncInterval       time.Duration
	MaxMemory          int64
	VendorID           string // e.g., "nvidia", "amd", "intel", "apple"
	DriverVersion      string // GPU driver version
	OpenCLVersion      string // OpenCL platform version
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
		VendorID:           "",
		DriverVersion:      "",
		OpenCLVersion:      "",
	}
}

type GPUInfo struct {
	Name             string
	Backend          GPUBackend
	DeviceID         int
	MemoryMB         int64
	ComputeMajor     int
	ComputeMinor     int
	Vendor           string // e.g., "NVIDIA", "AMD", "Intel", "Apple"
	VendorID         string // hex vendor ID (e.g., "0x10de" for NVIDIA)
	DriverVersion    string // driver version string
	OpenCLVersion    string // OpenCL device version
	Profile          string // OpenCL profile (FULL_PROFILE or EMBEDDED_PROFILE)
	MaxComputeUnits  int    // max parallel compute units
	MaxWorkGroupSize int64  // max work group size
	MaxWorkItemDims  []int  // max work item dimensions
}

type Index interface {
	Add(ids []int64, vectors []float32) error
	Search(vector []float32, k int) (ids []int64, distances []float32, err error)
	SearchBatch(vectors [][]float32, k int) (ids [][]int64, distances [][]float32, err error)
	SearchPQ(lookupTable []float32, m int, k int) (ids []int64, distances []float32, err error)
	TrainPQ(vectors []float32, m int, k int) error
	EncodePQ(vectors []float32) ([]byte, error)
	Close() error
	Backend() GPUBackend
	DeviceID() int // Returns the device ID this index runs on
	GetDeviceInfo() (*GPUInfo, error)
	GetMemoryInfo() (total, free, used int64, err error)
	GetUtilization() (float32, error)

	// Typed search methods for different vector types
	SearchFloat16(vector []uint16, k int) (ids []int64, distances []float32, err error)
	SearchComplex64(vector []uint16, k int) (ids []int64, distances []float32, err error)
	SearchComplex128(vector []float32, k int) (ids []int64, distances []float32, err error)
	AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error
	SearchTurboQuant(vector []float32, k int, bitsPerAngle int) (ids []int64, distances []float32, err error)
}

func DetectGPUBackend() GPUBackend {
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		return BackendMetal
	}
	// Simplified check for NVIDIA
	return BackendCPU
}

func GetDeviceCount() int {
	backend := DetectGPUBackend()
	switch backend {
	case BackendMetal:
		return 1 // Mac always has at least one Metal device
	}
	return 0
}

func GetGlobalGPUUtilization() (float32, error) {
	// For now, no-op or simple implementation to break cycle.
	return 0, nil
}
