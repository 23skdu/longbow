package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressedBitmap_Basic(t *testing.T) {
	bm := NewBitmap()

	bm.Add(1)
	bm.Add(100)
	bm.Add(5000)

	assert.True(t, bm.Contains(1))
	assert.True(t, bm.Contains(100))
	assert.True(t, bm.Contains(5000))
	assert.False(t, bm.Contains(2))

	assert.Equal(t, uint64(3), bm.Cardinality())

	bm.Remove(100)
	assert.False(t, bm.Contains(100))
	assert.Equal(t, uint64(2), bm.Cardinality())
}

func TestCompressedBitmap_SetOps(t *testing.T) {
	b1 := NewBitmap()
	b1.Add(1)
	b1.Add(2)
	b1.Add(3)

	b2 := NewBitmap()
	b2.Add(2)
	b2.Add(3)
	b2.Add(4)

	// Union: {1, 2, 3, 4}
	union := b1.Union(b2)
	assert.Equal(t, uint64(4), union.Cardinality())
	assert.True(t, union.Contains(1))
	assert.True(t, union.Contains(4))

	// Intersection: {2, 3}
	inter := b1.Intersection(b2)
	assert.Equal(t, uint64(2), inter.Cardinality())
	assert.True(t, inter.Contains(2))
	assert.False(t, inter.Contains(1))

	// Difference: {1}
	diff := b1.Difference(b2)
	assert.Equal(t, uint64(1), diff.Cardinality())
	assert.True(t, diff.Contains(1))
	assert.False(t, diff.Contains(2))
}
