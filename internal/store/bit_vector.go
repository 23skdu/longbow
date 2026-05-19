package store

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

// Get returns true if the bit at index i is set.
func (bv BitVector) Get(i uint32) bool {
	if int(i/64) >= len(bv) {
		return false
	}
	return (bv[i/64] & (1 << (i % 64))) != 0
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
