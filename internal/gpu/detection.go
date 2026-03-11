package gpu

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// DetectAvailableGPUs returns information about all available GPU devices
func DetectAvailableGPUs() []GPUInfo {
	var gpus []GPUInfo

	// Detect CUDA GPUs on Linux
	if runtime.GOOS == "linux" {
		cudaGPUs := detectCUDAGPUs()
		gpus = append(gpus, cudaGPUs...)
	}

	// Detect Metal GPUs on macOS
	if runtime.GOOS == "darwin" {
		metalGPUs := detectMetalGPUs()
		gpus = append(gpus, metalGPUs...)
	}

	return gpus
}

// detectCUDAGPUs detects all CUDA-capable NVIDIA GPUs
func detectCUDAGPUs() []GPUInfo {
	var gpus []GPUInfo

	// Check if nvidia-smi is available
	nvidiaSmiPath := findNvidiaSmi()
	if nvidiaSmiPath == "" {
		return gpus
	}

	// Query GPU information using nvidia-smi
	cmd := exec.Command(nvidiaSmiPath, "--query-gpu=index,name,memory.total,compute_cap", "--format=csv,noheader,nounits")
	output, err := cmd.Output()
	if err != nil {
		return gpus
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, ", ")
		if len(parts) < 4 {
			continue
		}

		deviceID, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
		name := strings.TrimSpace(parts[1])

		// Parse memory (in MiB)
		memoryStr := strings.TrimSpace(parts[2])
		memoryMiB, _ := strconv.ParseFloat(memoryStr, 64)
		memoryMB := int64(memoryMiB)

		// Parse compute capability
		computeCap := strings.TrimSpace(parts[3])
		majorMinor := strings.Split(computeCap, ".")
		computeMajor := 0
		computeMinor := 0
		if len(majorMinor) >= 2 {
			computeMajor, _ = strconv.Atoi(majorMinor[0])
			computeMinor, _ = strconv.Atoi(majorMinor[1])
		}

		gpu := GPUInfo{
			Backend:      BackendCUDA,
			Name:         name,
			DeviceID:     deviceID,
			MemoryMB:     memoryMB,
			ComputeMajor: computeMajor,
			ComputeMinor: computeMinor,
		}
		gpus = append(gpus, gpu)
	}

	return gpus
}

// detectMetalGPUs detects Metal-capable GPUs on macOS
func detectMetalGPUs() []GPUInfo {
	var gpus []GPUInfo

	// Check for Metal framework
	if _, err := os.Stat("/System/Library/Frameworks/Metal.framework"); os.IsNotExist(err) {
		return gpus
	}

	// On macOS, we typically have one Metal device
	// Try to get more info using system_profiler
	cmd := exec.Command("system_profiler", "SPDisplaysDataType", "-xml")
	output, err := cmd.Output()
	if err == nil {
		// Parse XML output to get GPU info (simplified)
		// In production, you'd use proper XML parsing
		outputStr := string(output)
		if strings.Contains(outputStr, "Metal") {
			// Try to get device name
			name := "Apple Silicon GPU"
			if runtime.GOARCH == "amd64" {
				name = "Intel GPU"
			}

			// Estimate memory (actual detection requires Metal API calls)
			memoryMB := int64(16384) // Default estimate

			// Check for Apple Silicon specific features
			isAppleSilicon := runtime.GOARCH == "arm64"
			if isAppleSilicon {
				name = "Apple Silicon GPU (Metal)"
			}

			gpu := GPUInfo{
				Backend:      BackendMetal,
				Name:         name,
				DeviceID:     0,
				MemoryMB:     memoryMB,
				ComputeMajor: 0, // Metal doesn't use compute capability
				ComputeMinor: 0,
			}
			gpus = append(gpus, gpu)
		}
	} else {
		// Fallback: check Metal framework exists
		if _, err := os.Stat("/System/Library/Frameworks/Metal.framework"); err == nil {
			name := "Apple GPU"
			switch runtime.GOARCH {
			case "arm64":
				name = "Apple Silicon GPU"
			case "amd64":
				name = "Intel/AMD GPU"
			}

			gpu := GPUInfo{
				Backend:  BackendMetal,
				Name:     name,
				DeviceID: 0,
				MemoryMB: 16384,
			}
			gpus = append(gpus, gpu)
		}
	}

	return gpus
}

// findNvidiaSmi finds the nvidia-smi executable
func findNvidiaSmi() string {
	paths := []string{
		"/usr/bin/nvidia-smi",
		"/usr/local/bin/nvidia-smi",
		"/opt/cuda/bin/nvidia-smi",
	}

	// Check CUDA_HOME
	if cudaHome := os.Getenv("CUDA_HOME"); cudaHome != "" {
		paths = append(paths, filepath.Join(cudaHome, "bin", "nvidia-smi"))
	}

	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// Try to find in PATH
	if path, err := exec.LookPath("nvidia-smi"); err == nil {
		return path
	}

	return ""
}

// checkCUDALibraries checks if CUDA libraries are available
func checkCUDALibraries() bool {
	libraryPaths := []string{
		"/usr/local/cuda/lib64/libcudart.so",
		"/usr/lib/x86_64-linux-gnu/libcudart.so",
		"/usr/lib64/libcudart.so",
	}

	if cudaHome := os.Getenv("CUDA_HOME"); cudaHome != "" {
		libraryPaths = append([]string{
			filepath.Join(cudaHome, "lib64", "libcudart.so"),
			filepath.Join(cudaHome, "lib", "libcudart.so"),
		}, libraryPaths...)
	}

	for _, path := range libraryPaths {
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}

	// Also check using ldconfig
	cmd := exec.Command("ldconfig", "-p")
	output, err := cmd.Output()
	if err == nil {
		if strings.Contains(string(output), "libcudart.so") {
			return true
		}
	}

	return false
}

// checkNVIDIADevices checks for NVIDIA GPU devices
func checkNVIDIADevices() bool {
	// Check /dev/nvidia*
	for i := 0; i < 8; i++ {
		devicePath := fmt.Sprintf("/dev/nvidia%d", i)
		if _, err := os.Stat(devicePath); err == nil {
			return true
		}
	}

	// Check for nvidia module
	if _, err := os.Stat("/proc/driver/nvidia"); err == nil {
		return true
	}

	return false
}

// GetPreferredBackend returns the best available GPU backend for the current system
func GetPreferredBackend() GPUBackend {
	gpus := DetectAvailableGPUs()

	if len(gpus) == 0 {
		return BackendCPU
	}

	// Prefer CUDA on Linux, Metal on macOS
	for _, gpu := range gpus {
		if runtime.GOOS == "linux" && gpu.Backend == BackendCUDA {
			return BackendCUDA
		}
		if runtime.GOOS == "darwin" && gpu.Backend == BackendMetal {
			return BackendMetal
		}
	}

	// Return first available backend
	return gpus[0].Backend
}

// ValidateBackend validates if the specified backend is available on the current system
func ValidateBackend(backend GPUBackend) error {
	switch backend {
	case BackendCUDA:
		if runtime.GOOS != "linux" {
			return fmt.Errorf("CUDA is only supported on Linux (current: %s)", runtime.GOOS)
		}

		if !checkNVIDIADevices() {
			return fmt.Errorf("no NVIDIA GPU devices found")
		}

		if !checkCUDALibraries() {
			return fmt.Errorf("CUDA runtime libraries not found. Please install CUDA toolkit or set CUDA_HOME")
		}

		// Check nvidia-smi
		if findNvidiaSmi() == "" {
			return fmt.Errorf("nvidia-smi not found. Please install NVIDIA drivers")
		}

		return nil

	case BackendMetal:
		if runtime.GOOS != "darwin" {
			return fmt.Errorf("Metal is only supported on macOS (current: %s)", runtime.GOOS)
		}

		if _, err := os.Stat("/System/Library/Frameworks/Metal.framework"); os.IsNotExist(err) {
			return fmt.Errorf("Metal framework not found")
		}

		return nil

	case BackendCPU:
		return nil

	default:
		return fmt.Errorf("unsupported backend: %v", backend)
	}
}

// GetBestGPU returns the best available GPU device info
func GetBestGPU() (*GPUInfo, error) {
	gpus := DetectAvailableGPUs()
	if len(gpus) == 0 {
		return nil, fmt.Errorf("no GPU devices available")
	}

	// Return GPU with most memory
	bestGPU := &gpus[0]
	for i := range gpus {
		if gpus[i].MemoryMB > bestGPU.MemoryMB {
			bestGPU = &gpus[i]
		}
	}

	return bestGPU, nil
}

// GetGPUByID returns GPU info for a specific device ID
func GetGPUByID(deviceID int) (*GPUInfo, error) {
	gpus := DetectAvailableGPUs()

	for i := range gpus {
		if gpus[i].DeviceID == deviceID {
			return &gpus[i], nil
		}
	}

	return nil, fmt.Errorf("GPU device %d not found", deviceID)
}
