//go:build gpu && linux && cuda

package cuda

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdaptiveBatchBuffer_Add(t *testing.T) {
	dim := 128
	var flushedIDs []int64
	var flushedVecs []float32

	flushFn := func(ids []int64, vecs []float32) error {
		flushedIDs = append(flushedIDs, ids...)
		flushedVecs = append(flushedVecs, vecs...)
		return nil
	}

	buf := NewAdaptiveBatchBuffer(dim, flushFn)

	// Add less than a block size
	for i := 0; i < 100; i++ {
		vec := make([]float32, dim)
		buf.Add(int64(i), vec)
	}

	assert.Empty(t, flushedIDs)

	// Add until it flushes automatically
	for i := 100; i < CUDABlockSize+5; i++ {
		vec := make([]float32, dim)
		buf.Add(int64(i), vec)
	}

	assert.Equal(t, CUDABlockSize, len(flushedIDs))

	buf.Flush()
	assert.Equal(t, CUDABlockSize+5, len(flushedIDs))
}

func FuzzAdaptiveBatchBuffer(f *testing.F) {
	f.Add(100, 128)
	f.Add(300, 64)
	f.Fuzz(func(t *testing.T, numAdds int, dim int) {
		if dim <= 0 || dim > 2048 || numAdds < 0 || numAdds > 10000 {
			t.Skip()
		}

		flushCount := 0
		flushFn := func(ids []int64, vecs []float32) error {
			assert.Equal(t, len(ids)*dim, len(vecs))
			flushCount += len(ids)
			return nil
		}

		buf := NewAdaptiveBatchBuffer(dim, flushFn)

		for i := 0; i < numAdds; i++ {
			vec := make([]float32, dim)
			buf.Add(int64(i), vec)
		}

		buf.Flush()
		assert.Equal(t, numAdds, flushCount)
	})
}
