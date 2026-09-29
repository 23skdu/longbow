package types

import (
	"github.com/23skdu/longbow/internal/simd"
)

// BitVector is a flat bitset for fast document filtering.
type BitVector []uint64

// NewBitVector creates a bit vector of the given size in bits.
func NewBitVector(size int) BitVector {
	return make(BitVector, (size+63)/64)
}

// Set sets the bit at index i.
func (bv BitVector) Set(i uint32) {
	bv[i/64] |= (1 << (i % 64))
}

// Test reports whether the bit at index i is set. It is the membership
// primitive for filter probing: a single indexed word load, one shift/mask and
// one compare, with an out-of-range index answering false instead of panicking,
// so callers can probe ids in traversal loops without a preceding bounds check.
func (bv BitVector) Test(i uint32) bool {
	w := i >> 6
	if w >= uint32(len(bv)) { // #nosec G115 -- len is a non-negative slice length
		return false
	}
	return bv[w]&(1<<(i&63)) != 0
}

// Get returns true if the bit at index i is set.
func (bv BitVector) Get(i uint32) bool {
	return bv.Test(i)
}

// And performs bitwise AND between two bit vectors.
func (bv BitVector) And(other BitVector) {
	simd.AndBitVectors(bv, other)
}

// Count returns the number of set bits.
func (bv BitVector) Count() int {
	return simd.CountBitVector(bv)
}

// ToRoaring converts the BitVector to a roaring.Bitmap.
// Useful for interoperability with existing filters.
/*
func (bv BitVector) ToRoaring() *roaring.Bitmap {
	bm := roaring.New()
	for i, v := range bv {
		if v == 0 {
			continue
		}
		for j := 0; j < 64; j++ {
			if (v & (1 << uint(j))) != 0 {
				bm.Add(uint32(i*64 + j))
			}
		}
	}
	return bm
}
*/
