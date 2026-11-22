package simd_test

import (
	"testing"

	"github.com/23skdu/longbow/internal/simd"
)

// TestSIMD_Detection runs the test simd_detection operation.
func TestSIMD_Detection(t *testing.T) {
	t.Logf("SIMD package builds successfully")
	t.Logf("Implementation: %s", simd.GetImplementation())
}
