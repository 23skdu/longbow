package simd

import (
	"fmt"
	"math"
	"runtime"

	"github.com/klauspost/cpuid/v2"
)

// CPUFeatures contains detected CPU SIMD capabilities
type CPUFeatures struct {
	Vendor        string
	HasAVX2       bool
	HasAVX512     bool
	HasVNNI       bool // AVX512-VNNI
	HasAVXVNNI    bool // AVX-VNNI (Alder Lake+)
	HasAVX512FP16 bool // AVX512-FP16 (Sapphire Rapids+)
	HasVBMI       bool // AVX512-VBMI (Ice Lake+)
	HasNEON       bool
	HasDotProd    bool // ARM64 FEAT_DotProd (udot/sdot)
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
	hasVBMI := cpuid.CPU.Supports(cpuid.AVX512VBMI)

	// Only detect NEON on ARM platforms
	hasNEON := runtime.GOARCH == "arm64" && cpuid.CPU.Supports(cpuid.ASIMD)
	hasDotProd := runtime.GOARCH == "arm64" && cpuid.CPU.Supports(cpuid.ASIMDDP)

	features = CPUFeatures{
		Vendor:        cpuid.CPU.VendorString,
		HasAVX2:       cpuid.CPU.Supports(cpuid.AVX2),
		HasAVX512:     hasAVX512,
		HasVNNI:       hasVNNI,
		HasAVXVNNI:    hasAVXVNNI,
		HasAVX512FP16: hasAVX512FP16,
		HasVBMI:       hasVBMI,
		HasNEON:       hasNEON,
		HasDotProd:    hasDotProd,
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

// GetImplementationDetails returns a structured map describing the selected
// SIMD implementation and detected CPU flags. Used by the /diagnostics endpoint
// to enable remote debugging of SIMD fallback issues (e.g., on ancalagon).
func GetImplementationDetails() map[string]any {
	return map[string]any{
		"simd_impl":       implementation,
		"arch":            runtime.GOARCH,
		"vendor":          features.Vendor,
		"has_avx512":      features.HasAVX512,
		"has_avx2":        features.HasAVX2,
		"has_vnni":        features.HasVNNI,
		"has_avx_vnni":    features.HasAVXVNNI,
		"has_avx512_fp16": features.HasAVX512FP16,
		"has_vbmi":        features.HasVBMI,
		"has_neon":        features.HasNEON,
		"has_dotprod":     features.HasDotProd,
	}
}

// ValidateSIMDKernels runs a known-answer test for the active distance kernels.
// If the result is incorrect (which can happen when AVX-512 instructions trap
// in virtualised environments), this function returns an error so callers can
// fall back to a safer implementation.
//
// The reference case is a 128-dim L2 distance between a zero vector and a
// vector of all-ones, which should equal sqrt(128) ≈ 11.3137.
func ValidateSIMDKernels() error {
	const dim = 128
	a := make([]float32, dim)
	b := make([]float32, dim)
	for i := range b {
		b[i] = 1.0
	}

	// Expected: euclidean(zeros, ones) for dim=128 is sqrt(128) ≈ 11.3137
	const expected = float32(11.313708) // math.Sqrt(128)

	dist, err := EuclideanDistance(a, b)
	if err != nil {
		return fmt.Errorf("SIMD kernel self-test failed (%s): %w", implementation, err)
	}
	if math.IsNaN(float64(dist)) || math.IsInf(float64(dist), 0) {
		return fmt.Errorf("SIMD kernel self-test returned non-finite result (%.6f) for %s", dist, implementation)
	}
	// Allow 0.1% tolerance for FP rounding across implementations
	diff := math.Abs(float64(dist-expected)) / float64(expected)
	if diff > 0.001 {
		return fmt.Errorf(
			"SIMD kernel self-test failed for %s: got %.6f, expected %.6f (%.3f%% error)",
			implementation, dist, expected, diff*100,
		)
	}
	return nil
}
