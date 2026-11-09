//go:build !gpu || !linux || !cgo

package cuda

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FuzzContractCUDAFallback asserts the cross-platform contract of the
// non-GPU build of the tensor contraction kernel: it must always report
// failure and must never touch its inputs, regardless of what data flows
// through the pipeline.
func FuzzContractCUDAFallback(f *testing.F) {
	// Seeds: valid shapes, empty shapes, unaligned sizes, single elements.
	f.Add([]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}, []byte{0, 0, 0, 0}, false, 2, 2, 1)
	f.Add([]byte{}, []byte{}, []byte{}, true, 0, 0, 0)
	f.Add([]byte{1}, []byte{1}, []byte{0}, false, 1, 1, 1)
	f.Add([]byte{0xFF}, []byte{0x7F}, []byte{0x01}, true, 1, 1, 1)
	f.Add(make([]byte, 17), make([]byte, 17), make([]byte, 17), false, 3, 3, 3)
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{8, 7, 6, 5, 4, 3, 2, 1}, []byte{0, 0, 0, 0, 0, 0, 0, 0}, false, 2, 2, 2)
	f.Add([]byte{0, 0, 0, 0}, []byte{0, 0, 0, 0}, []byte{0xFF, 0xFF, 0xFF, 0xFF}, true, 1, 1, 4)

	f.Fuzz(func(t *testing.T, a, b, out []byte, isFloat64 bool, m, n, k int) {
		// Bound work: these inputs describe tensors, not buffers to copy.
		if len(a) > 4096 || len(b) > 4096 || len(out) > 4096 {
			return
		}
		if m < 0 || n < 0 || k < 0 || m > 4096 || n > 4096 || k > 4096 {
			return
		}
		// Capture pristine copies to detect any mutation (nil stays nil,
		// empty stays empty so the comparison is exact).
		a0 := slices.Clone(a)
		b0 := slices.Clone(b)
		out0 := slices.Clone(out)

		// Run the fallback contract twice: availability must be a fixed,
		// deterministic "no" and neither call may mutate any argument.
		require.False(t, ContractCUDA(a, b, out, isFloat64, m, n, k),
			"fallback must never report GPU support in this build")
		require.False(t, ContractCUDA(a, b, out, isFloat64, m, n, k),
			"fallback result must be deterministic")

		assert.Equal(t, a0, a, "input A must not be mutated")
		assert.Equal(t, b0, b, "input B must not be mutated")
		assert.Equal(t, out0, out, "output buffer must not be mutated")
	})
}
