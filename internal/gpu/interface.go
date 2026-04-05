package gpu

import (
	"fmt"
	"os"
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

// GetGPURequirements returns the requirements for GPU acceleration based on backend
func GetGPURequirements(backend GPUBackend) (bool, string, error) {
	switch backend {
	case BackendCUDA:
		// Check for NVIDIA GPU via nvidia-smi
		_, err := exec.LookPath("nvidia-smi")
		if err != nil {
			return false, "nvidia-smi not found - CUDA requires NVIDIA GPU and driver", nil
		}
		return true, "CUDA 11.0+ with NVIDIA GPU and nvidia-smi available", nil
	case BackendMetal:
		if runtime.GOOS != "darwin" {
			return false, "Metal requires macOS (Apple Silicon or Intel Mac)", nil
		}
		if runtime.GOARCH != "arm64" && runtime.GOARCH != "amd64" {
			return false, "Metal requires macOS with Apple Silicon or Intel Mac", nil
		}
		return true, "Metal requires macOS with Apple Silicon (M1/M2/M3) or Intel Mac with Metal support", nil
	case BackendOpenCL:
		if runtime.GOOS == "darwin" {
			return true, "OpenCL available on macOS (CPU fallback or discrete GPU)", nil
		}
		if runtime.GOOS == "linux" {
			return checkOpenCLOnLinux()
		}
		if runtime.GOOS == "windows" {
			return checkOpenCLOnWindows()
		}
		return false, "OpenCL not available on this platform", nil
	default:
		return false, "Unknown GPU backend", nil
	}
}

func checkOpenCLOnLinux() (bool, string, error) {
	libraryPaths := []string{
		"/usr/lib/x86_64-linux-gnu/libOpenCL.so.1",
		"/usr/lib64/libOpenCL.so.1",
		"/usr/local/lib/libOpenCL.so.1",
		"/opt/intel/opencl/libOpenCL.so.1",
	}
	for _, path := range libraryPaths {
		if _, err := os.Stat(path); err == nil {
			return true, "OpenCL available on Linux (AMD/Intel/NVIDIA)", nil
		}
	}
	return false, "OpenCL libraries not found on Linux", nil
}

func checkOpenCLOnWindows() (bool, string, error) {
	clPath := `C:\Windows\System32\OpenCL.dll`
	if _, err := os.Stat(clPath); err == nil {
		return true, "OpenCL available on Windows", nil
	}
	return false, "OpenCL not found on Windows", nil
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
