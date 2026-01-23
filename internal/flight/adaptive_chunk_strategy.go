package flight

import (
	"sync/atomic"
)

// AdaptiveChunkStrategy manages dynamic chunk sizing for DoGet operations.
// It starts with small chunks for low latency and grows to larger chunks
// for high throughput.
type AdaptiveChunkStrategy struct {
	minSize      int
	maxSize      int
	growthFactor float64
	currentSize  atomic.Int32
}

// NewAdaptiveChunkStrategy creates a new adaptive chunking strategy.
// minSize: initial chunk size (e.g., 4096 rows)
// maxSize: maximum chunk size (e.g., 65536 rows)
// growthFactor: multiplier for exponential growth (e.g., 2.0 for doubling)
func NewAdaptiveChunkStrategy(minSize, maxSize int, growthFactor float64) *AdaptiveChunkStrategy {
	s := &AdaptiveChunkStrategy{
		minSize:      minSize,
		maxSize:      maxSize,
		growthFactor: growthFactor,
	}
	s.currentSize.Store(int32(minSize))
	return s
}

// NextChunkSize returns the next chunk size and advances the strategy.
func (s *AdaptiveChunkStrategy) NextChunkSize() int {
	current := int(s.currentSize.Load())

	// Calculate next size with exponential growth
	next := int(float64(current) * s.growthFactor)

	// Cap at max size
	if next > s.maxSize {
		next = s.maxSize
	}

	// Store next size for subsequent calls
	s.currentSize.Store(int32(next))

	// Return current size (before growth)
	return current
}

// CurrentSize returns the current chunk size without advancing.
func (s *AdaptiveChunkStrategy) CurrentSize() int {
	return int(s.currentSize.Load())
}

// Reset resets the strategy back to minimum chunk size.
func (s *AdaptiveChunkStrategy) Reset() {
	s.currentSize.Store(int32(s.minSize))
}

// LinearChunkStrategy implements linear growth instead of exponential.
type LinearChunkStrategy struct {
	minSize     int
	maxSize     int
	increment   int
	currentSize atomic.Int32
}

// NewLinearChunkStrategy creates a linear growth strategy.
func NewLinearChunkStrategy(minSize, maxSize, increment int) *LinearChunkStrategy {
	s := &LinearChunkStrategy{
		minSize:   minSize,
		maxSize:   maxSize,
		increment: increment,
	}
	s.currentSize.Store(int32(minSize))
	return s
}

// NextChunkSize returns the next chunk size with linear growth.
func (s *LinearChunkStrategy) NextChunkSize() int {
	current := int(s.currentSize.Load())

	// Calculate next size with linear growth
	next := current + s.increment

	// Cap at max size
	if next > s.maxSize {
		next = s.maxSize
	}

	// Store next size
	s.currentSize.Store(int32(next))

	// Return current size
	return current
}

// CurrentSize returns the current chunk size.
func (s *LinearChunkStrategy) CurrentSize() int {
	return int(s.currentSize.Load())
}

// Reset resets to minimum size.
func (s *LinearChunkStrategy) Reset() {
	s.currentSize.Store(int32(s.minSize))
}
