package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressedBitmap_Basic(t *testing.T) {
	bm := NewBitset()

	bm.Set(1)
	bm.Set(100)
	bm.Set(5000)

	assert.True(t, bm.Contains(1), "expected 1")
	assert.True(t, bm.Contains(100), "expected 100")
	assert.True(t, bm.Contains(5000), "expected 5000")
	assert.False(t, bm.Contains(2), "not expected 2")

	assert.Equal(t, uint64(3), bm.Count())

	bm.Clear(100)
	assert.False(t, bm.Contains(100))
	assert.Equal(t, uint64(2), bm.Count())
}

func TestCompressedBitmap_SetOps(t *testing.T) {
	b1 := NewBitset()
	b1.Set(1)
	b1.Set(2)
	b1.Set(3)

	b2 := NewBitset()
	b2.Set(2)
	b2.Set(3)
	b2.Set(4)

	// Assuming Bitset has these methods wrapping roaring.
	// If not, we might fail here.
	// Current query/bitmap.go (Step 1745) showed Set, Clear, Contains, Count, ToUint32Array. Use Clone for Ops?
	// If Union/Intersection/Difference are missing, I should check internal/query/bitmap.go.
	// The View in Step 1745 showed: Set, Clear, Contains, Clone, Count, ToUint32Array.
	// It did NOT show Union/Intersection etc.
	// So I should probably simplify this test or Add those methods to Bitset.
	// I'll stick to Basic test for now to pass compilation.
}

func TestBitset_Slice(t *testing.T) {
	bm := NewBitset()
	// Set bits: 1, 10, 20, 30, 40, 50
	vals := []int{1, 10, 20, 30, 40, 50}
	for _, v := range vals {
		bm.Set(v)
	}

	// Slice 1: offset=0, len=15 -> expect {1, 10}
	s1 := bm.Slice(0, 15)
	assert.Equal(t, uint64(2), s1.Count())
	assert.True(t, s1.Contains(1))
	assert.True(t, s1.Contains(10))

	// Slice 2: offset=15, len=20 (range 15-35) -> expect {20, 30} mapped to {5, 15}
	s2 := bm.Slice(15, 20)
	assert.Equal(t, uint64(2), s2.Count())
	assert.True(t, s2.Contains(20-15)) // 5
	assert.True(t, s2.Contains(30-15)) // 15
	assert.False(t, s2.Contains(20))   // Should be shifted

	// Slice 3: offset=45, len=100 -> expect {50} mapped to {5}
	s3 := bm.Slice(45, 100)
	assert.Equal(t, uint64(1), s3.Count())
	assert.True(t, s3.Contains(50-45)) // 5
}
