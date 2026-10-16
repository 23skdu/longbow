package tensor

import (
	"os"
	"testing"

	"github.com/23skdu/longbow/internal/mathutil"
	"github.com/stretchr/testify/assert"
)

func TestParseDispatchMode(t *testing.T) {
	tests := []struct {
		input string
		want  DispatchMode
	}{
		{"auto", DispatchAuto},
		{"AUTO", DispatchAuto},
		{"emlgo", DispatchEML},
		{"eml", DispatchEML},
		{"EMLGO", DispatchEML},
		{"standard", DispatchStandard},
		{"std", DispatchStandard},
		{"go", DispatchStandard},
		{"STANDARD", DispatchStandard},
		{"", DispatchAuto},
		{"unknown", DispatchAuto},
		{" emlgo ", DispatchEML},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseDispatchMode(tt.input)
			assert.Equal(t, tt.want, got, "ParseDispatchMode(%q)", tt.input)
		})
	}
}

func TestGetDispatchMode(t *testing.T) {
	origMode := dispatchMode
	defer func() { dispatchMode = origMode }()

	dispatchMode = DispatchAuto
	assert.Equal(t, DispatchAuto, GetDispatchMode())

	dispatchMode = DispatchEML
	assert.Equal(t, DispatchEML, GetDispatchMode())

	dispatchMode = DispatchStandard
	assert.Equal(t, DispatchStandard, GetDispatchMode())
}

func TestResolveBackend_StandardMode(t *testing.T) {
	origMode := dispatchMode
	dispatchMode = DispatchStandard
	defer func() { dispatchMode = origMode }()

	// All types should return standard
	types := []string{"float32", "float64", "int8", "uint8", "complex64", "complex128", "turboquant"}
	for _, typeName := range types {
		got := ResolveBackend(typeName, 100000)
		assert.Equal(t, mathutil.BackendStandard, got,
			"DispatchStandard: ResolveBackend(%q, 100000)", typeName)
	}
}

func TestResolveBackend_EmptyTypeName(t *testing.T) {
	origMode := dispatchMode
	dispatchMode = DispatchAuto
	defer func() { dispatchMode = origMode }()

	got := ResolveBackend("", 100000)
	assert.Equal(t, mathutil.BackendStandard, got, "empty type name should use standard")
}

// TestResolveBackend_AutoMode_RoutingRules verifies the routing logic structure.
// Without the emlgo build tag, all modes fall through to BackendStandard.
// With the emlgo build tag, auto mode routes complex/TQ>=50k to emlgo.
func TestResolveBackend_AutoMode_RoutingRules(t *testing.T) {
	origMode := dispatchMode
	dispatchMode = DispatchAuto
	defer func() { dispatchMode = origMode }()

	// Test that dispatchMode is correctly set
	assert.Equal(t, DispatchAuto, GetDispatchMode())

	// Verify standard mode returns standard for everything
	dispatchMode = DispatchStandard
	got := ResolveBackend("complex64", 100000)
	assert.Equal(t, mathutil.BackendStandard, got, "standard mode should always return standard")

	// Verify EML mode: without emlgo build tag, still returns standard (stub behavior)
	dispatchMode = DispatchEML
	got = ResolveBackend("complex64", 100000)
	// Without emlgo tag: always BackendStandard (stub)
	// With emlgo tag: BackendEML
	// Both are valid; the stub correctly falls through
	assert.True(t, got == mathutil.BackendStandard || got == mathutil.BackendEML,
		"eml mode returns standard (no emlgo tag) or emlgo (with tag)")
}

// TestResolveBackend_AutoMode_SizeThresholds verifies empirical size routing
// (docs/emlgo.md, roadmap.md §2).
func TestResolveBackend_AutoMode_SizeThresholds(t *testing.T) {
	origMode := dispatchMode
	defer func() { dispatchMode = origMode }()

	origThresholds := GetDispatchThresholds()
	defer SetDispatchThresholds(origThresholds)
	SetDispatchThresholds(DefaultThresholds())

	// Probe: forced DispatchEML returns EML only on emlgo builds (stub always standard).
	dispatchMode = DispatchEML
	emlgoTagged := ResolveBackend("float32", 1) == mathutil.BackendEML

	dispatchMode = DispatchAuto
	cases := []struct {
		name     string
		typeName string
		count    int
		wantEML  bool
	}{
		{"below_threshold_std", "complex128", MinEMLVectorCount - 1, false},
		{"at_threshold_complex128", "complex128", MinEMLVectorCount, true},
		{"complex64_mid_eml", "complex64", 100000, true},
		{"complex64_250k_eml", "complex64", 250000, true},
		{"complex64_500k_std", "complex64", 500001, false},
		{"complex128_500k_std", "complex128", 500000, false},
		{"float64_always_std", "float64", 500000, false},
		{"float32_always_std", "float32", 500000, false},
		{"turboquant_eml", "turboquant", 50000, true},
		{"int8_always_std", "int8", 500000, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ResolveBackend(c.typeName, c.count)
			if !emlgoTagged {
				assert.Equal(t, mathutil.BackendStandard, got, "stub build always standard")
				return
			}
			if c.wantEML {
				assert.Equal(t, mathutil.BackendEML, got, "%s n=%d", c.typeName, c.count)
			} else {
				assert.Equal(t, mathutil.BackendStandard, got, "%s n=%d", c.typeName, c.count)
			}
		})
	}
}

func TestHardwareAwareThresholdCalibration(t *testing.T) {
	// Tier 1: Advanced Server (AMX or AVX-512 VNNI with large L3)
	t.Run("Tier1_AMX_Server", func(t *testing.T) {
		profile := CPUHardwareProfile{
			HasAMX:        true,
			HasAVX512:     true,
			HasAVX512VNNI: true,
			L2CacheBytes:  2 * 1024 * 1024,
			L3CacheBytes:  64 * 1024 * 1024,
		}
		thresholds := CalibrateThresholds(profile)
		assert.Equal(t, 25000, thresholds.MinEMLVectorCount)
		assert.Equal(t, 25000, thresholds.TurboQuantEMLMinCount)
		assert.Equal(t, 400000, thresholds.Complex64EMLMaxCount)
		assert.Equal(t, 750000, thresholds.Complex128EMLMaxCount)
	})

	// Tier 2: Standard Server (AVX-512 or large L3)
	t.Run("Tier2_AVX512_Server", func(t *testing.T) {
		profile := CPUHardwareProfile{
			HasAMX:        false,
			HasAVX512:     true,
			HasAVX512VNNI: false,
			L2CacheBytes:  1024 * 1024,
			L3CacheBytes:  32 * 1024 * 1024,
		}
		thresholds := CalibrateThresholds(profile)
		assert.Equal(t, 35000, thresholds.MinEMLVectorCount)
		assert.Equal(t, 35000, thresholds.TurboQuantEMLMinCount)
		assert.Equal(t, 300000, thresholds.Complex64EMLMaxCount)
		assert.Equal(t, 600000, thresholds.Complex128EMLMaxCount)
	})

	// Tier 3: Standard Client (AVX2, 16MB L3)
	t.Run("Tier3_Standard_Client", func(t *testing.T) {
		profile := CPUHardwareProfile{
			HasAMX:        false,
			HasAVX512:     false,
			HasAVX512VNNI: false,
			L2CacheBytes:  512 * 1024,
			L3CacheBytes:  16 * 1024 * 1024,
		}
		thresholds := CalibrateThresholds(profile)
		assert.Equal(t, DefaultMinEMLVectorCount, thresholds.MinEMLVectorCount)
		assert.Equal(t, DefaultTurboQuantEMLMinCount, thresholds.TurboQuantEMLMinCount)
		assert.Equal(t, DefaultComplex64EMLMaxCount, thresholds.Complex64EMLMaxCount)
		assert.Equal(t, DefaultComplex128EMLMaxCount, thresholds.Complex128EMLMaxCount)
	})

	// Tier 4: Cache-Constrained (<8MB L3)
	t.Run("Tier4_Constrained_Cache", func(t *testing.T) {
		profile := CPUHardwareProfile{
			HasAMX:        false,
			HasAVX512:     false,
			HasAVX512VNNI: false,
			L2CacheBytes:  256 * 1024,
			L3CacheBytes:  4 * 1024 * 1024,
		}
		thresholds := CalibrateThresholds(profile)
		assert.Equal(t, 65536, thresholds.MinEMLVectorCount)
		assert.Equal(t, 65536, thresholds.TurboQuantEMLMinCount)
		assert.Equal(t, 150000, thresholds.Complex64EMLMaxCount)
		assert.Equal(t, 300000, thresholds.Complex128EMLMaxCount)
	})
}

func TestApplyDispatchConfig_EnvOverrides(t *testing.T) {
	origThresholds := GetDispatchThresholds()
	defer SetDispatchThresholds(origThresholds)

	os.Setenv("LONGBOW_MATH_DISPATCH_MIN_VECTORS", "12345")
	os.Setenv("LONGBOW_MATH_DISPATCH_COMPLEX64_MAX", "345678")
	os.Setenv("LONGBOW_MATH_DISPATCH_COMPLEX128_MAX", "890123")
	defer func() {
		os.Unsetenv("LONGBOW_MATH_DISPATCH_MIN_VECTORS")
		os.Unsetenv("LONGBOW_MATH_DISPATCH_COMPLEX64_MAX")
		os.Unsetenv("LONGBOW_MATH_DISPATCH_COMPLEX128_MAX")
		ApplyDispatchConfig()
	}()

	ApplyDispatchConfig()
	th := GetDispatchThresholds()
	assert.Equal(t, 12345, th.MinEMLVectorCount)
	assert.Equal(t, 12345, th.TurboQuantEMLMinCount)
	assert.Equal(t, 345678, th.Complex64EMLMaxCount)
	assert.Equal(t, 890123, th.Complex128EMLMaxCount)
}

func TestDetectHardwareProfile(t *testing.T) {
	profile := DetectHardwareProfile()
	// L2 and L3 should be non-negative
	assert.GreaterOrEqual(t, profile.L2CacheBytes, 0)
	assert.GreaterOrEqual(t, profile.L3CacheBytes, 0)
}
