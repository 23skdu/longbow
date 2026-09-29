package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitVectorTest_Membership(t *testing.T) {
	const n = 200 // not a multiple of 64, so the tail word is partial

	bv := NewBitVector(n)
	assert.Equal(t, 4, len(bv), "200 bits must occupy 4 words")

	// id 0 and id n-1 are the boundary cases of the universe.
	bv.Set(0)
	bv.Set(n - 1)
	bv.Set(64)
	bv.Set(65)

	assert.True(t, bv.Test(0), "id 0 must be set")
	assert.True(t, bv.Test(n-1), "id n-1 must be set")
	assert.True(t, bv.Test(64), "first id of the second word")
	assert.True(t, bv.Test(65), "id 65 shares a word with 64")
	assert.False(t, bv.Test(1), "unset id")
	assert.False(t, bv.Test(62), "unset id")
	assert.False(t, bv.Test(63), "unset id at the word boundary")
	assert.False(t, bv.Test(66), "unset id just past a set bit")

	// ids >= n are out of range and must answer false, not panic. 200..204 are
	// inside the last (partial) word but past the universe, and anything from
	// 256 on is past the end of the backing store.
	for _, id := range []uint32{n, n + 1, n + 4, 255, 256, 1000, 1 << 20, math.MaxUint32} {
		assert.False(t, bv.Test(id), "id %d is out of range", id)
	}

	// Every set bit agrees with Get.
	for i := uint32(0); i < n; i++ {
		assert.Equal(t, bv.Get(i), bv.Test(i), "Test and Get disagree at id %d", i)
	}
}

func TestBitVectorTest_NegativeSafeCasts(t *testing.T) {
	bv := NewBitVector(64)
	bv.Set(0)

	// Callers that hold a signed id (as the graph and Bitset probes do) can
	// reach Test through a uint32 conversion of a negative int. Every negative
	// int that fits in 32 bits converts to a large id outside the universe, so
	// it must not alias a readable slot and must not panic.
	negatives := []int{-1, -2, -64, -65, -1 << 20, math.MinInt32}
	for _, neg := range negatives {
		assert.False(t, bv.Test(uint32(neg)), "negative id %d must not alias a set bit", neg) // #nosec G115
	}

	// Test is total, so a 64-bit negative that truncates into the universe
	// (math.MinInt truncates to 0) still answers without panicking. Keeping the
	// caller's value in range is the caller's contract, not Test's.
	minInt := math.MinInt
	assert.True(t, bv.Test(uint32(minInt)), "truncated MinInt lands on id 0, which is set") // #nosec G115

	// The same holds on a zero-length vector.
	var empty BitVector
	assert.False(t, empty.Test(uint32(negatives[5]))) // #nosec G115
	assert.False(t, empty.Test(uint32(negatives[0]))) // #nosec G115
}

func TestBitVectorTest_EmptyVector(t *testing.T) {
	for _, bv := range []BitVector{nil, {}, make(BitVector, 0, 8)} {
		assert.False(t, bv.Test(0), "empty vector admits nothing")
		assert.False(t, bv.Test(1))
		assert.False(t, bv.Test(63))
		assert.False(t, bv.Test(64))
		assert.False(t, bv.Test(math.MaxUint32))
	}
}

func TestBitVectorTest_AgreesWithRoaring(t *testing.T) {
	bv := NewBitVector(1000)
	set := []uint32{0, 1, 63, 64, 127, 128, 511, 512, 999}
	for _, id := range set {
		bv.Set(id)
	}
	for i := uint32(0); i < 1200; i++ {
		want := false
		for _, id := range set {
			if id == i {
				want = true
				break
			}
		}
		assert.Equal(t, want, bv.Test(i), "id %d", i)
	}
	assert.Equal(t, len(set), bv.Count())
}
