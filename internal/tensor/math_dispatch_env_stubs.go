//go:build !emlgo

package tensor

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/mathutil"
	"github.com/klauspost/cpuid/v2"
)

// DispatchMode controls which math backend is used.
type DispatchMode int

const (
	// DispatchAuto uses empirical routing rules based on data type and scale.
	DispatchAuto DispatchMode = iota
	// DispatchEML forces emlgo for all operations.
	DispatchEML
	// DispatchStandard forces standard math for all operations.
	DispatchStandard
)

// DefaultMinEMLVectorCount is the baseline minimum vector count before auto mode
// routes any type to emlgo.
const DefaultMinEMLVectorCount = 50000

// DefaultComplex64EMLMaxCount is the baseline count where auto mode still routes
// complex64 to emlgo.
const DefaultComplex64EMLMaxCount = 250000

// DefaultComplex128EMLMaxCount is the baseline count where auto mode routes
// complex128 to emlgo before standard math takes over.
const DefaultComplex128EMLMaxCount = 500000

// DefaultTurboQuantEMLMinCount is the baseline count for TurboQuant EMLGo dispatch.
const DefaultTurboQuantEMLMinCount = 50000

// MinEMLVectorCount mirrors the emlgo build threshold (kept in sync for tests).
const MinEMLVectorCount = DefaultMinEMLVectorCount

// DispatchThresholds specifies the calibrated vector count cutoffs for math dispatch.
type DispatchThresholds struct {
	MinEMLVectorCount     int
	Complex64EMLMaxCount  int
	Complex128EMLMaxCount int
	TurboQuantEMLMinCount int
}

// CPUHardwareProfile captures the hardware traits used to calibrate dispatch thresholds.
type CPUHardwareProfile struct {
	HasAVX512     bool
	HasAVX512VNNI bool
	HasAMX        bool
	L2CacheBytes  int
	L3CacheBytes  int
}

// DefaultThresholds returns the uncalibrated baseline thresholds.
func DefaultThresholds() DispatchThresholds {
	return DispatchThresholds{
		MinEMLVectorCount:     DefaultMinEMLVectorCount,
		Complex64EMLMaxCount:  DefaultComplex64EMLMaxCount,
		Complex128EMLMaxCount: DefaultComplex128EMLMaxCount,
		TurboQuantEMLMinCount: DefaultTurboQuantEMLMinCount,
	}
}

// CalibrateThresholds computes dynamic, hardware-aware dispatch thresholds based on
// CPU instruction set extensions (AVX-512, VNNI, AMX) and L2/L3 cache sizes.
func CalibrateThresholds(profile CPUHardwareProfile) DispatchThresholds {
	t := DefaultThresholds()

	// Tier 1: Advanced Server Matrix / Vector Accelerators (AMX or AVX-512 with VNNI & >=32MB L3)
	if profile.HasAMX || (profile.HasAVX512 && profile.HasAVX512VNNI && profile.L3CacheBytes >= 32*1024*1024) {
		t.MinEMLVectorCount = 25000
		t.TurboQuantEMLMinCount = 25000
		t.Complex64EMLMaxCount = 400000
		t.Complex128EMLMaxCount = 750000
		return t
	}

	// Tier 2: Wide SIMD Server / High-End Workstation (AVX-512 or >=32MB L3)
	if profile.HasAVX512 || profile.L3CacheBytes >= 32*1024*1024 {
		t.MinEMLVectorCount = 35000
		t.TurboQuantEMLMinCount = 35000
		t.Complex64EMLMaxCount = 300000
		t.Complex128EMLMaxCount = 600000
		return t
	}

	// Tier 4: Cache-constrained client / VM (<8MB L3 or undetectable with no AVX512)
	if profile.L3CacheBytes > 0 && profile.L3CacheBytes < 8*1024*1024 {
		t.MinEMLVectorCount = 65536
		t.TurboQuantEMLMinCount = 65536
		t.Complex64EMLMaxCount = 150000
		t.Complex128EMLMaxCount = 300000
		return t
	}

	// Tier 3: Standard Client (AVX2, 8MB-32MB L3)
	return t
}

// DetectHardwareProfile inspects the host CPU for vector extensions and cache topology.
func DetectHardwareProfile() CPUHardwareProfile {
	hasAVX512 := cpuid.CPU.Supports(cpuid.AVX512F) &&
		cpuid.CPU.Supports(cpuid.AVX512DQ) &&
		cpuid.CPU.Supports(cpuid.AVX512BW) &&
		cpuid.CPU.Supports(cpuid.AVX512VL)

	hasVNNI := cpuid.CPU.Supports(cpuid.AVX512VNNI)
	hasAMX := cpuid.CPU.Supports(cpuid.AMXTILE) && os.Getenv("LONGBOW_DISABLE_AMX") == ""

	l2 := cpuid.CPU.Cache.L2
	if l2 < 0 {
		l2 = 0
	}
	l3 := cpuid.CPU.Cache.L3
	if l3 < 0 {
		l3 = 0
	}

	return CPUHardwareProfile{
		HasAVX512:     hasAVX512,
		HasAVX512VNNI: hasVNNI,
		HasAMX:        hasAMX,
		L2CacheBytes:  l2,
		L3CacheBytes:  l3,
	}
}

// ShouldDispatchBatchAMX returns true if host CPU supports Intel AMX and the batch size B >= 32 (Item 41).
func ShouldDispatchBatchAMX(batchSize int) bool {
	profile := DetectHardwareProfile()
	return profile.HasAMX && batchSize >= 32
}

var (
	dispatchMode     DispatchMode = DispatchAuto
	activeThresholds atomic.Pointer[DispatchThresholds]
)

func init() {
	def := DefaultThresholds()
	activeThresholds.Store(&def)
	ApplyDispatchConfig()
}

// GetDispatchThresholds returns the current active dispatch thresholds.
func GetDispatchThresholds() DispatchThresholds {
	p := activeThresholds.Load()
	if p == nil {
		def := DefaultThresholds()
		return def
	}
	return *p
}

// SetDispatchThresholds sets the active dispatch thresholds.
func SetDispatchThresholds(t DispatchThresholds) {
	activeThresholds.Store(&t)
}

// GetDispatchMode returns the current dispatch mode.
func GetDispatchMode() DispatchMode {
	return dispatchMode
}

// SetDispatchMode sets the dispatch mode. Without the emlgo build tag this
// only records the mode; the math backend is always standard.
func SetDispatchMode(m DispatchMode) {
	dispatchMode = m
	if m == DispatchStandard || m == DispatchEML || m == DispatchAuto {
		mathutil.SetBackend(mathutil.BackendStandard)
		SetMathImpl(MathSIMD)
	}
}

// ParseDispatchMode parses a LONGBOW_MATH_DISPATCH env var value.
func ParseDispatchMode(val string) DispatchMode {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "emlgo", "eml":
		return DispatchEML
	case "standard", "std", "go":
		return DispatchStandard
	default:
		return DispatchAuto
	}
}

// ResolveBackend selects the math backend for a given data type and vector count.
// Without the emlgo build tag, all modes fall through to BackendStandard.
func ResolveBackend(typeName string, vectorCount int) mathutil.Backend {
	return mathutil.BackendStandard
}

// ApplyDispatchConfig reads LONGBOW_MATH_DISPATCH and hardware features to configure math thresholds and backend.
func ApplyDispatchConfig() {
	// First calibrate based on detected hardware profile
	calibrated := CalibrateThresholds(DetectHardwareProfile())

	// Apply optional environment variable overrides
	if val, ok := os.LookupEnv("LONGBOW_MATH_DISPATCH_MIN_VECTORS"); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n > 0 {
			calibrated.MinEMLVectorCount = n
			calibrated.TurboQuantEMLMinCount = n
		}
	}
	if val, ok := os.LookupEnv("LONGBOW_MATH_DISPATCH_COMPLEX64_MAX"); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n > 0 {
			calibrated.Complex64EMLMaxCount = n
		}
	}
	if val, ok := os.LookupEnv("LONGBOW_MATH_DISPATCH_COMPLEX128_MAX"); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil && n > 0 {
			calibrated.Complex128EMLMaxCount = n
		}
	}
	SetDispatchThresholds(calibrated)

	if val, ok := os.LookupEnv("LONGBOW_MATH_DISPATCH"); ok {
		SetDispatchMode(ParseDispatchMode(val))
	}
}
