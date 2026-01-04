package simd_test

import (
	"runtime"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/stretchr/testify/assert"
)

func TestSIMD_Detection(t *testing.T) {
	impl := simd.GetImplementation()
	t.Logf("Detected SIMD Implementation: %s", impl)

	features := simd.GetCPUFeatures()
	t.Logf("CPU Features: %+v", features)

	switch runtime.GOARCH {
	case "amd64":
		if features.HasAVX2 {
			assert.Equal(t, "avx2", impl, "Should use AVX2 on supported AMD64")
		} else {
			assert.Equal(t, "generic", impl, "Should fallback to generic if no AVX2")
		}
	case "arm64":
		if features.HasNEON {
			assert.Equal(t, "neon", impl, "Should use NEON on supported ARM64")
		} else {
			assert.Equal(t, "generic", impl, "Should fallback to generic if no NEON")
		}
	}
}
