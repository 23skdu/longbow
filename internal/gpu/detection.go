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
		// Also check for OpenCL GPUs on Linux (AMD, Intel)
		openclGPUs := detectOpenCLGPUs()
		gpus = append(gpus, openclGPUs...)
		// Detect TPU on Linux
		tpuGPUs := detectTPUs()
		gpus = append(gpus, tpuGPUs...)
	}

	// Detect Metal GPUs on macOS (also supports OpenCL fallback)
	if runtime.GOOS == "darwin" {
		metalGPUs := detectMetalGPUs()
		gpus = append(gpus, metalGPUs...)
		// macOS also has OpenCL support
		openclGPUs := detectOpenCLGPUs()
		gpus = append(gpus, openclGPUs...)
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
	// #nosec G204 - nvidiaSmiPath is set from known config/locations, not user input
	cmd := exec.Command(nvidiaSmiPath, "--query-gpu=index,name,memory.total,compute_cap", "--format=csv,noheader,nounits") // #nosec G204
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

		deviceID, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to parse device ID from line '%s': %v\n", line, err)
			continue
		}
		name := strings.TrimSpace(parts[1])

		// Parse memory (in MiB)
		memoryStr := strings.TrimSpace(parts[2])
		memoryMiB, err := strconv.ParseFloat(memoryStr, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Failed to parse memory from '%s': %v\n", memoryStr, err)
			continue
		}
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
	live := detectMetalGPULive()
	if len(live) > 0 {
		return live
	}

	var gpus []GPUInfo
	// Fallback to basic detection if live CGO call fails

	return gpus
}

// detectOpenCLGPUs detects OpenCL-capable GPUs (AMD, Intel on Linux/macOS)
func detectOpenCLGPUs() []GPUInfo {
	var gpus []GPUInfo

	// First check if OpenCL is available at all
	if !checkOpenCLAvailable() {
		return gpus
	}

	// Try to query OpenCL devices using clinfo or similar tools
	// Different platforms have different tools
	switch runtime.GOOS {
	case "linux":
		gpus = detectOpenCLLinux()
	case "darwin":
		gpus = detectOpenCLDarwin()
	case "windows":
		gpus = detectOpenCLWindows()
	}

	return gpus
}

func checkOpenCLAvailable() bool {
	paths := []string{
		"/usr/lib/x86_64-linux-gnu/libOpenCL.so.1",
		"/usr/lib64/libOpenCL.so.1",
		"/usr/local/lib/libOpenCL.so.1",
		"/System/Library/Frameworks/OpenCL.framework",
		`C:\Windows\System32\OpenCL.dll`,
	}

	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}

	// Try using ocl-icd utility if available
	if path, err := exec.LookPath("clinfo"); err == nil {
		cmd := exec.Command(path) // #nosec G204 - path is from LookPath, not user input
		if output, err := cmd.Output(); err == nil && len(output) > 0 {
			return true
		}
	}

	return false
}

func detectOpenCLLinux() []GPUInfo {
	var gpus []GPUInfo

	// Try clinfo to get OpenCL device info
	if path, err := exec.LookPath("clinfo"); err == nil {
		cmd := exec.Command(path) // #nosec G204 - path is from LookPath
		output, err := cmd.Output()
		if err == nil {
			gpus = parseClinfoOutput(string(output), BackendOpenCL)
		}
	}

	// Fallback: check for specific vendor libraries
	if len(gpus) == 0 {
		// Check AMD
		if _, err := os.Stat("/usr/lib/x86_64-linux-gnu/libOpenCL.so.1"); err == nil {
			// Try lspci to detect GPU
			if path, err := exec.LookPath("lspci"); err == nil {
				cmd := exec.Command(path, "-d", "1002:") // #nosec G204 - AMD vendor ID is constant
				output, _ := cmd.Output()
				if strings.Contains(string(output), "VGA") || strings.Contains(string(output), "GPU") {
					gpus = append(gpus, GPUInfo{
						Backend:  BackendOpenCL,
						Name:     "AMD GPU (OpenCL)",
						DeviceID: 0,
						MemoryMB: 8192,
					})
				}
			}
		}

		// Check Intel
		if len(gpus) == 0 {
			if path, err := exec.LookPath("lspci"); err == nil {
				cmd := exec.Command(path, "-d", "8086:") // #nosec G204 - Intel vendor ID is constant
				output, _ := cmd.Output()
				if strings.Contains(string(output), "VGA") || strings.Contains(string(output), "GPU") {
					gpus = append(gpus, GPUInfo{
						Backend:  BackendOpenCL,
						Name:     "Intel GPU (OpenCL)",
						DeviceID: 0,
						MemoryMB: 4096,
					})
				}
			}
		}
	}

	return gpus
}

func detectOpenCLDarwin() []GPUInfo {
	var gpus []GPUInfo

	// macOS has OpenCL through the system
	// Use system_profiler to get GPU info
	cmd := exec.Command("system_profiler", "SPDisplaysDataType", "-xml")
	output, err := cmd.Output()
	if err == nil {
		outputStr := string(output)
		if strings.Contains(outputStr, "OpenGL") || strings.Contains(outputStr, "Metal") {
			gpus = append(gpus, GPUInfo{
				Backend:  BackendOpenCL,
				Name:     "Apple GPU (OpenCL)",
				DeviceID: 0,
				MemoryMB: 16384,
			})
		}
	}

	return gpus
}

func detectOpenCLWindows() []GPUInfo {
	var gpus []GPUInfo

	// Check registry for GPU info or use WMI
	// For now, just check if OpenCL.dll exists
	if _, err := os.Stat(`C:\Windows\System32\OpenCL.dll`); err == nil {
		// Try to get GPU info from registry
		cmd := exec.Command("wmic", "path", "win32_VideoController", "get", "name", "/value")
		output, err := cmd.Output()
		if err == nil {
			outputStr := string(output)
			// Simple parsing - look for common GPU names
			if strings.Contains(outputStr, "NVIDIA") {
				gpus = append(gpus, GPUInfo{
					Backend:  BackendOpenCL,
					Name:     "NVIDIA GPU (OpenCL)",
					DeviceID: 0,
					MemoryMB: 8192,
				})
			} else if strings.Contains(outputStr, "AMD") || strings.Contains(outputStr, "Radeon") {
				gpus = append(gpus, GPUInfo{
					Backend:  BackendOpenCL,
					Name:     "AMD GPU (OpenCL)",
					DeviceID: 0,
					MemoryMB: 8192,
				})
			} else if strings.Contains(outputStr, "Intel") {
				gpus = append(gpus, GPUInfo{
					Backend:  BackendOpenCL,
					Name:     "Intel GPU (OpenCL)",
					DeviceID: 0,
					MemoryMB: 4096,
				})
			}
		}
	}

	return gpus
}

func parseClinfoOutput(output string, backend GPUBackend) []GPUInfo {
	var gpus []GPUInfo

	lines := strings.Split(output, "\n")
	var currentGPU *GPUInfo

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "Platform") {
			if currentGPU != nil && currentGPU.Name != "" {
				gpus = append(gpus, *currentGPU)
			}
			currentGPU = &GPUInfo{
				Backend:  backend,
				DeviceID: len(gpus),
			}
		}

		if currentGPU == nil {
			continue
		}

		if strings.HasPrefix(line, "Name:") {
			currentGPU.Name = strings.TrimSpace(strings.TrimPrefix(line, "Name:"))
		} else if strings.HasPrefix(line, "Device Version:") {
			version := strings.TrimSpace(strings.TrimPrefix(line, "Device Version:"))
			currentGPU.OpenCLVersion = version
		} else if strings.HasPrefix(line, "Global memory size:") {
			memStr := strings.TrimPrefix(line, "Global memory size:")
			memStr = strings.TrimSpace(strings.TrimSuffix(memStr, "MB"))
			if memMB, err := strconv.ParseInt(memStr, 10, 64); err == nil {
				currentGPU.MemoryMB = memMB
			}
		} else if strings.HasPrefix(line, "Vendor:") {
			currentGPU.Vendor = strings.TrimSpace(strings.TrimPrefix(line, "Vendor:"))
			currentGPU.VendorID = getVendorID(currentGPU.Vendor)
		} else if strings.HasPrefix(line, "Profile:") {
			currentGPU.Profile = strings.TrimSpace(strings.TrimPrefix(line, "Profile:"))
		} else if strings.HasPrefix(line, "Max compute units:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "Max compute units:"))
			if units, err := strconv.Atoi(val); err == nil {
				currentGPU.MaxComputeUnits = units
			}
		} else if strings.HasPrefix(line, "Max work group size:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "Max work group size:"))
			if size, err := strconv.ParseInt(val, 10, 64); err == nil {
				currentGPU.MaxWorkGroupSize = size
			}
		}
	}

	if currentGPU != nil && currentGPU.Name != "" {
		gpus = append(gpus, *currentGPU)
	}

	return gpus
}

func getVendorID(vendor string) string {
	vendorLower := strings.ToLower(vendor)
	switch {
	case strings.Contains(vendorLower, "nvidia"):
		return "0x10de"
	case strings.Contains(vendorLower, "amd") || strings.Contains(vendorLower, "advanced"):
		return "0x1002"
	case strings.Contains(vendorLower, "intel"):
		return "0x8086"
	case strings.Contains(vendorLower, "apple"):
		return "0x106b"
	default:
		return ""
	}
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
		if _, err := os.Stat(path); err == nil { // #nosec G703 - pre-defined system locations
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
		if _, err := os.Stat(path); err == nil { // #nosec G703 - pre-defined system library paths
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
var accelSysfsRoot = "/sys/class/accel"

// detectTPUs detects Google Cloud TPU devices (v2-v7x)
func detectTPUs() []GPUInfo {
	return detectTPUsWithRoot(accelSysfsRoot)
}

func detectTPUsWithRoot(root string) []GPUInfo {
	var gpus []GPUInfo

	if runtime.GOOS != "linux" {
		return gpus
	}

	// TPU devices are exposed via the 'accel' class in modern kernels
	accelDir := root
	if _, err := os.Stat(accelDir); err != nil {
		// Fallback for older kernels/drivers: check /dev/tpu*
		for i := 0; i < 8; i++ {
			tpuDev := fmt.Sprintf("/dev/tpu%d", i)
			if _, err := os.Stat(tpuDev); err == nil {
				gpus = append(gpus, GPUInfo{
					Backend:  BackendTPU,
					Name:     "Google TPU",
					DeviceID: i,
					MemoryMB: 16384, // Generic fallback
				})
			}
		}
		return gpus
	}

	devices, err := os.ReadDir(accelDir)
	if err != nil {
		return gpus
	}

	for _, dev := range devices {
		devName := dev.Name()
		if !strings.HasPrefix(devName, "accel") {
			continue
		}

		idStr := strings.TrimPrefix(devName, "accel")
		deviceID, _ := strconv.Atoi(idStr)

		// Check for Google Vendor ID (0x1ae0)
		vendorPath := filepath.Join(accelDir, devName, "device/vendor")
		vendorData, err := os.ReadFile(vendorPath) // #nosec G304
		if err != nil || strings.TrimSpace(string(vendorData)) != "0x1ae0" {
			continue
		}

		// Try to identify the chip version (Ironwood = TPU v7x)
		name := "Google TPU"
		memoryMB := int64(192 * 1024) // Default for v7x

		devicePath := filepath.Join(accelDir, devName, "device/device")
		deviceData, _ := os.ReadFile(devicePath) // #nosec G304
		deviceIDHex := strings.TrimSpace(string(deviceData))

		switch deviceIDHex {
		case "0x0063": // Example ID for Ironwood
			name = "Google TPU v7x (Ironwood)"
		case "0x005e": // Example ID for v5p
			name = "Google TPU v5p"
			memoryMB = 95 * 1024
		}

		// Check for NUMA affinity
		numaNode := -1
		numaPath := filepath.Join(accelDir, devName, "device/numa_node")
		if numaData, err := os.ReadFile(numaPath); err == nil {
			if node, err := strconv.Atoi(strings.TrimSpace(string(numaData))); err == nil {
				numaNode = node
			}
		}

		gpus = append(gpus, GPUInfo{
			Backend:      BackendTPU,
			Name:         name,
			DeviceID:     deviceID,
			MemoryMB:     memoryMB,
			Vendor:       "Google",
			VendorID:     "0x1ae0",
			ComputeMajor: numaNode, // Reusing ComputeMajor to store NUMA node for now
		})
	}

	return gpus
}
