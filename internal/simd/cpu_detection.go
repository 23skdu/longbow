package simd

import (
	"github.com/klauspost/cpuid/v2"
)

// CPUFeatures contains detected CPU SIMD capabilities
type CPUFeatures struct {
	Vendor    string
	HasAVX2   bool
	HasAVX512 bool
	HasNEON   bool
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

	features = CPUFeatures{
		Vendor:    cpuid.CPU.VendorString,
		HasAVX2:   cpuid.CPU.Supports(cpuid.AVX2),
		HasAVX512: hasAVX512,
		HasNEON:   cpuid.CPU.Supports(cpuid.ASIMD), // ARM NEON
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
