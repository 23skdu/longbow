package simd

import (
	"github.com/klauspost/cpuid/v2"
	"runtime"
)

// CPUFeatures contains detected CPU SIMD capabilities
type CPUFeatures struct {
	Vendor    string
	HasAVX2   bool
	HasAVX512 bool
	HasVNNI      bool // AVX512-VNNI
	HasAVXVNNI   bool // AVX-VNNI (Alder Lake+)
	HasAVX512FP16 bool // AVX512-FP16 (Sapphire Rapids+)
	HasNEON      bool
}

// Global CPU detection state
var (
	features       CPUFeatures
	implementation string
)

// detectCPU detects CPU capabilities and selects the best SIMD implementation
func detectCPU() {
	// Use more comprehensive AVX512 detection
	hasAVX512 := cpuid.CPU.Supports(cpuid.AVX512F) &&
		cpuid.CPU.Supports(cpuid.AVX512DQ) &&
		cpuid.CPU.Supports(cpuid.AVX512BW) &&
		cpuid.CPU.Supports(cpuid.AVX512VL)

	hasVNNI := cpuid.CPU.Supports(cpuid.AVX512VNNI)
	hasAVXVNNI := cpuid.CPU.Supports(cpuid.AVXVNNI)
	hasAVX512FP16 := cpuid.CPU.Supports(cpuid.AVX512FP16)

	// Only detect NEON on ARM platforms
	hasNEON := runtime.GOARCH == "arm64" && cpuid.CPU.Supports(cpuid.ASIMD)

	features = CPUFeatures{
		Vendor:        cpuid.CPU.VendorString,
		HasAVX2:       cpuid.CPU.Supports(cpuid.AVX2),
		HasAVX512:     hasAVX512,
		HasVNNI:       hasVNNI,
		HasAVXVNNI:    hasAVXVNNI,
		HasAVX512FP16: hasAVX512FP16,
		HasNEON:       hasNEON,
	}

	// Select best implementation with fallback logic
	switch {
	case features.HasAVX512:
		implementation = "avx512"
	case features.HasAVX2:
		// Additional check for AVX2 prerequisites
		if cpuid.CPU.Supports(cpuid.FMA3) && cpuid.CPU.Supports(cpuid.BMI1) {
			implementation = "avx2"
		} else {
			implementation = "generic"
		}
	case features.HasNEON:
		implementation = "neon"
	default:
		implementation = "generic"
	}
}

// GetCPUFeatures returns the detected CPU capabilities
func GetCPUFeatures() CPUFeatures {
	return features
}

// GetImplementation returns the selected SIMD implementation name
func GetImplementation() string {
	return implementation
}
