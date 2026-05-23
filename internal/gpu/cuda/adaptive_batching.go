//go:build gpu && linux && cuda

package cuda

import (
	"sync"
)

const (
	CUDAWarpSize  = 32
	CUDABlockSize = 256
)

// AdaptiveBatchBuffer coalesces vector ingestion to perfectly align with CUDA thread blocks.
type AdaptiveBatchBuffer struct {
	mu       sync.Mutex
	ids      []int64
	vectors  []float32
	dim      int
	flushFn  func([]int64, []float32) error
}

// NewAdaptiveBatchBuffer creates a new buffer that flushes to flushFn.
func NewAdaptiveBatchBuffer(dim int, flushFn func([]int64, []float32) error) *AdaptiveBatchBuffer {
	return &AdaptiveBatchBuffer{
		ids:     make([]int64, 0, CUDABlockSize),
		vectors: make([]float32, 0, CUDABlockSize*dim),
		dim:     dim,
		flushFn: flushFn,
	}
}

// Add adds a single vector to the buffer, flushing if block size is reached.
func (b *AdaptiveBatchBuffer) Add(id int64, vector []float32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.ids = append(b.ids, id)
	b.vectors = append(b.vectors, vector...)

	if len(b.ids) >= CUDABlockSize {
		return b.flush()
	}
	return nil
}

// AddBatch adds multiple vectors, flushing in CUDABlockSize chunks.
func (b *AdaptiveBatchBuffer) AddBatch(ids []int64, vectors []float32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range ids {
		b.ids = append(b.ids, ids[i])
		start := i * b.dim
		end := start + b.dim
		b.vectors = append(b.vectors, vectors[start:end]...)

		if len(b.ids) >= CUDABlockSize {
			if err := b.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// flush sends the current batch and resets the buffer. Not thread-safe.
func (b *AdaptiveBatchBuffer) flush() error {
	if len(b.ids) == 0 {
		return nil
	}
	err := b.flushFn(b.ids, b.vectors)
	b.ids = b.ids[:0]
	b.vectors = b.vectors[:0]
	return err
}

// Flush explicitly flushes any remaining buffered items.
func (b *AdaptiveBatchBuffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flush()
}
