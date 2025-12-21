package store

import (
	"github.com/RoaringBitmap/roaring/v2"
)

// CompressedBitmap provides a memory-efficient bitset using Roaring Bitmaps.
// It is optimized for high-cardinality integer sets (e.g., VectorID lists, tombstones).
type CompressedBitmap struct {
	bitmap *roaring.Bitmap
}

// NewBitmap creates a new empty compressed bitmap.
func NewBitmap() *CompressedBitmap {
	return &CompressedBitmap{
		bitmap: roaring.New(),
	}
}

// Add sets the bit for the given VectorID.
func (b *CompressedBitmap) Add(id VectorID) {
	b.bitmap.Add(uint32(id))
}

// Remove clears the bit for the given VectorID.
func (b *CompressedBitmap) Remove(id VectorID) {
	b.bitmap.Remove(uint32(id))
}

// Contains checks if the bit for the given VectorID is set.
func (b *CompressedBitmap) Contains(id VectorID) bool {
	return b.bitmap.Contains(uint32(id))
}

// Cardinality returns the number of set bits.
func (b *CompressedBitmap) Cardinality() uint64 {
	return b.bitmap.GetCardinality()
}

// Intersection returns a new bitmap containing the intersection of b and other.
func (b *CompressedBitmap) Intersection(other *CompressedBitmap) *CompressedBitmap {
	return &CompressedBitmap{
		bitmap: roaring.And(b.bitmap, other.bitmap),
	}
}

// Union returns a new bitmap containing the union of b and other.
func (b *CompressedBitmap) Union(other *CompressedBitmap) *CompressedBitmap {
	return &CompressedBitmap{
		bitmap: roaring.Or(b.bitmap, other.bitmap),
	}
}

// Difference returns a new bitmap containing elements in b but not in other.
func (b *CompressedBitmap) Difference(other *CompressedBitmap) *CompressedBitmap {
	return &CompressedBitmap{
		bitmap: roaring.AndNot(b.bitmap, other.bitmap),
	}
}

// Iterator returns an iterator over the set bits.
func (b *CompressedBitmap) Iterator() roaring.IntIterable {
	return b.bitmap.Iterator()
}
