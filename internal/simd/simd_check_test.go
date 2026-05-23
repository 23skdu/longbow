package simd_test

import (
	"github.com/23skdu/longbow/internal/simd"
	"testing"
)

func TestSIMD_Detection(t *testing.T) {
	t.Logf("SIMD package builds successfully")
	t.Logf("Implementation: %s", simd.GetImplementation())
}
