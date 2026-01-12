//go:build go1.18

package store

import (
	"testing"
)

func FuzzVectorDataType(f *testing.F) {
	// Add seed corpus for established types
	for i := 0; i < 20; i++ {
		f.Add(i)
	}

	f.Fuzz(func(t *testing.T, val int) {
		dt := VectorDataType(val)

		// Ensure String() doesn't panic
		_ = dt.String()

		// Ensure ElementSize() doesn't panic
		_ = dt.ElementSize()
	})
}
