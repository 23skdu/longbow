package gpu

import (
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/gpu/types"
)

// Re-export basic types from types subpackage for convenience
type GPUBackend = types.GPUBackend

const (
	BackendCPU    = types.BackendCPU
	BackendCUDA   = types.BackendCUDA
	BackendMetal  = types.BackendMetal
	BackendOpenCL = types.BackendOpenCL
)

type GPUConfig = types.GPUConfig
type GPUInfo = types.GPUInfo
type Index = types.Index

// Memory management re-exports
type GPUMemPool = memory.GPUMemPool

func NewGPUMemPool(backend GPUBackend, deviceID int) (*GPUMemPool, error) {
	return memory.NewGPUMemPool(backend, deviceID)
}

// Error type re-exports
type GPUInitializationError = types.GPUInitializationError
type GPUNotAvailableError = types.GPUNotAvailableError
type GPUSyncError = types.GPUSyncError
type GPUMemoryError = types.GPUMemoryError
type GPUComputeError = types.GPUComputeError

// Error helper re-exports
func IsGPUMemoryError(err error) bool {
	return types.IsGPUMemoryError(err)
}

func IsGPUComputeError(err error) bool {
	return types.IsGPUComputeError(err)
}

// GetGPURequirements returns the requirements for GPU acceleration (stub)
func GetGPURequirements(backend GPUBackend) (bool, string, error) {
	return true, "CUDA 11.0+ or Metal-compatible hardware", nil
}

// DefaultGPUConfig returns default GPU configuration
func DefaultGPUConfig() GPUConfig {
	return types.DefaultGPUConfig()
}

// Global detection and helper functions (pure Go)

func DetectGPUBackend() GPUBackend {
	// 1. Check for NVIDIA
	_, err := exec.LookPath("nvidia-smi")
	if err == nil {
		return BackendCUDA
	}

	// 2. Check for Metal (Darwin/Apple Silicon)
	// We assume BackendMetal is the primary accelerator for Darwin.
	if runtime.GOOS == "darwin" {
		return BackendMetal
	}

	return BackendCPU
}

func GetDeviceCount() int {
	backend := types.DetectGPUBackend()
	switch backend {
	case BackendCUDA:
		// Attempt to use nvidia-smi to count
		cmd := exec.Command("nvidia-smi", "-L")
		out, err := cmd.Output()
		if err == nil {
			return len(strings.Split(strings.TrimSpace(string(out)), "\n"))
		}
	case BackendMetal:
		return 1 // Mac always has at least one Metal device
	}
	return 0
}

func GetGlobalGPUUtilization() (float32, error) {
	backend := types.DetectGPUBackend()
	if backend == BackendCUDA {
		cmd := exec.Command("nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits")
		out, err := cmd.Output()
		if err == nil {
			val, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 32)
			if err == nil {
				return float32(val), nil
			}
		}
	}
	return 0, fmt.Errorf("utilization monitoring not supported for current backend")
}
