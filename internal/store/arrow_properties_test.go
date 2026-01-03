package store

import (
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// TestBitsetProperties validates Bitset behavior using property-based testing.
func TestBitsetProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	// Property: Setting a bit makes it set
	properties.Property("Set makes bit set", prop.ForAll(
		func(size int, idx uint32) bool {
			if size <= 0 || int(idx) >= size {
				return true // Skip invalid inputs
			}
			bs := NewArrowBitset(size)
			bs.Set(idx)
			return bs.IsSet(idx)
		},
		gen.IntRange(1, 10000),
		gen.UInt32Range(0, 9999),
	))

	// Property: Clearing resets all bits
	properties.Property("Clear resets all bits", prop.ForAll(
		func(size int, indices []uint32) bool {
			if size <= 0 {
				return true
			}
			bs := NewArrowBitset(size)

			// Set some bits
			for _, idx := range indices {
				if int(idx) < size {
					bs.Set(idx)
				}
			}

			// Clear
			bs.Clear()

			// All should be unset
			for i := 0; i < size; i++ {
				if bs.IsSet(uint32(i)) {
					return false
				}
			}
			return true
		},
		gen.IntRange(1, 1000),
		gen.SliceOf(gen.UInt32Range(0, 999)),
	))

	properties.TestingRun(t)
}

// TestFixedHeapProperties validates FixedHeap behavior using property-based testing.
func TestFixedHeapProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	// Property: Pop returns elements in ascending distance order
	properties.Property("Pop returns sorted order", prop.ForAll(
		func(distances []float32) bool {
			if len(distances) == 0 {
				return true
			}

			h := NewFixedHeap(len(distances))

			// Push all
			for i, dist := range distances {
				h.Push(Candidate{ID: uint32(i), Dist: dist})
			}

			// Pop all and verify sorted
			var prev float32 = -1e9
			for h.Len() > 0 {
				c, ok := h.Pop()
				if !ok {
					return false
				}
				if c.Dist < prev {
					return false // Not sorted
				}
				prev = c.Dist
			}
			return true
		},
		gen.SliceOfN(100, gen.Float32Range(0, 1000)),
	))

	// Property: Len matches number of pushes
	properties.Property("Len matches pushes", prop.ForAll(
		func(n int) bool {
			if n <= 0 || n > 1000 {
				return true
			}

			h := NewFixedHeap(n)
			for i := 0; i < n; i++ {
				h.Push(Candidate{ID: uint32(i), Dist: float32(i)})
			}
			return h.Len() == n
		},
		gen.IntRange(1, 1000),
	))

	properties.TestingRun(t)
}
