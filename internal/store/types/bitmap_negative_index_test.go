package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBitset_NegativeIndexIsIgnored pins the negative-index contract. The
// roaring-backed Bitset converts its int index to a roaring uint32, so an
// unguarded negative silently aliases bit 4294967295: Set(-1) would set a bit
// that reads back as an enormous index, and Contains(-1) would answer for it.
// ArrowBitset has always rejected negatives; Bitset must match it.
func TestBitset_NegativeIndexIsIgnored(t *testing.T) {
	t.Run("Set", func(t *testing.T) {
		b := NewBitset()
		defer b.Release()

		b.Set(1)
		b.Set(-1)
		b.Set(-2)
		b.Set(math.MinInt)

		assert.True(t, b.Contains(1), "a valid index must still be set")
		assert.Equal(t, uint64(1), b.Count(), "only the valid index may be set")
		assert.Equal(t, uint64(0), countAbove(b, 1<<31),
			"a negative index must not alias a bit near the top of the space")
	})

	t.Run("Clear", func(t *testing.T) {
		b := NewBitset()
		defer b.Release()

		b.Set(7)
		b.Clear(-1)
		b.Clear(math.MinInt)

		assert.True(t, b.Contains(7), "clearing a negative index must not disturb other bits")
		assert.Equal(t, uint64(1), b.Count())
	})

	t.Run("Contains", func(t *testing.T) {
		b := NewBitset()
		defer b.Release()

		// Populate the bit a negative index would alias, then confirm the
		// negative probe does not see it.
		b.bitmap.Add(1<<32 - 1)

		assert.False(t, b.Contains(-1), "a negative index is never contained")
		assert.True(t, b.Contains(int(1<<32-1)), "the real bit is still set")
	})

	t.Run("Slice", func(t *testing.T) {
		b := NewBitset()
		defer b.Release()

		for i := range 16 {
			b.Set(i)
		}

		negOffset := b.Slice(-1, 4)
		defer negOffset.Release()
		assert.Equal(t, uint64(0), negOffset.Count(), "a negative offset yields an empty slice")

		negLen := b.Slice(2, -3)
		defer negLen.Release()
		assert.Equal(t, uint64(0), negLen.Count(), "a negative length yields an empty slice")

		ok := b.Slice(4, 4)
		defer ok.Release()
		assert.Equal(t, uint64(4), ok.Count(), "a valid slice is unaffected")
		assert.True(t, ok.Contains(0), "the slice is shifted to start at zero")
		assert.True(t, ok.Contains(3))
		assert.False(t, ok.Contains(4))
	})
}

// TestBitset_NegativeIndexOnSharedBitmap checks the guard holds for a Bitset
// that wraps a caller's roaring.Bitmap, where Set must clone before it can
// mutate. The guard has to run before that work, not after.
func TestBitset_NegativeIndexOnSharedBitmap(t *testing.T) {
	owner := NewBitset()
	defer owner.Release()
	owner.Set(3)

	shared := NewBitsetFromRoaring(owner.AsRoaring())
	shared.Set(-1)

	assert.True(t, shared.Contains(3))
	assert.Equal(t, uint64(1), shared.Count())
	assert.Equal(t, uint64(1), owner.Count(), "the caller's bitmap must be untouched")
}

// countAbove counts set bits strictly greater than n.
func countAbove(b *Bitset, n uint32) uint64 {
	bm := b.AsRoaring()
	if bm == nil {
		return 0
	}
	it := bm.Iterator()
	it.AdvanceIfNeeded(n + 1)
	var c uint64
	for it.HasNext() {
		if it.Next() > n {
			c++
		}
	}
	return c
}
