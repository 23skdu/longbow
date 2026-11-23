//go:build go1.18

package hnsw

import (
	"testing"
)

// FuzzVectorDataType runs the fuzz vector data type operation.
func FuzzVectorDataType(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping fuzz test in short mode")
	}
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
