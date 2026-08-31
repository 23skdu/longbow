package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowBitset(t *testing.T) {
	b := NewArrowBitset(100)
	require.NotNil(t, b)
	assert.Equal(t, 0, b.Size())
	assert.GreaterOrEqual(t, b.Capacity(), 100)
	assert.Equal(t, 0, b.Count())

	b.Set(10)
	b.Set(65)
	b.Set(120) // forces growInternal
	assert.True(t, b.IsSet(10))
	assert.True(t, b.IsSet(65))
	assert.True(t, b.IsSet(120))
	assert.False(t, b.IsSet(0))
	assert.False(t, b.IsSet(121))
	assert.False(t, b.IsSet(-1))

	b.Set(-1) // Should ignore

	assert.Equal(t, 3, b.Count())
	assert.Equal(t, 120, b.Size())

	b.Grow(200)
	assert.GreaterOrEqual(t, b.Capacity(), 200)

	b.Clear()
	assert.False(t, b.IsSet(65))
	assert.Equal(t, 0, b.Count())

	b.ClearSIMD()
	assert.Equal(t, 0, b.Count())
	assert.False(t, b.IsSet(10))

	// edge cases
	b2 := NewArrowBitset(0) // should default to 64
	assert.GreaterOrEqual(t, b2.Capacity(), 64)
}

func TestBitVector(t *testing.T) {
	bv := NewBitVector(100)
	require.NotNil(t, bv)
	assert.Equal(t, 0, bv.Count())

	bv.Set(10)
	bv.Set(65)
	assert.True(t, bv.Get(10))
	assert.True(t, bv.Get(65))
	assert.False(t, bv.Get(0))
	assert.False(t, bv.Get(100))

	assert.Equal(t, 2, bv.Count())

	bv2 := NewBitVector(100)
	bv2.Set(10)
	bv2.Set(70)

	bv.And(bv2)
	assert.True(t, bv.Get(10))
	assert.False(t, bv.Get(65))
	assert.False(t, bv.Get(70))
	assert.Equal(t, 1, bv.Count())
}

func TestBitset_Roaring(t *testing.T) {
	b := NewBitset()
	require.NotNil(t, b)

	b.Set(10)
	b.Set(65)
	assert.True(t, b.Contains(10))
	assert.True(t, b.Contains(65))
	assert.False(t, b.Contains(0))
	assert.Equal(t, uint64(2), b.Count())

	arr := b.ToUint32Array()
	assert.Len(t, arr, 2)
	assert.Contains(t, arr, uint32(10))
	assert.Contains(t, arr, uint32(65))

	roar := b.AsRoaring()
	require.NotNil(t, roar)
	assert.True(t, roar.Contains(10))

	b2 := NewBitsetFromRoaring(roar)
	require.NotNil(t, b2)
	assert.True(t, b2.Contains(10))

	b.Clear(10)
	assert.False(t, b.Contains(10))
	assert.Equal(t, uint64(1), b.Count())

	b3 := b.Clone()
	require.NotNil(t, b3)
	assert.True(t, b3.Contains(65))

	b.And(b2.AsRoaring()) // b has 65, b2 has 10,65. result should be 65
	assert.True(t, b.Contains(65))
	assert.False(t, b.Contains(10))

	b.Release()
	b2.Release()
	b3.Release()
}

func TestBitset_Slice(t *testing.T) {
	b := NewBitset()
	b.Set(1)
	b.Set(5)
	b.Set(10)
	b.Set(15)

	slice1 := b.Slice(1, 10) // offset 1, length 10 => iterates 1, 5, 10. Sets 0, 4, 9
	assert.NotNil(t, slice1)
	arr := slice1.ToUint32Array()
	assert.Len(t, arr, 3)
	assert.Equal(t, uint32(0), arr[0])
	assert.Equal(t, uint32(4), arr[1])
	assert.Equal(t, uint32(9), arr[2])

	slice2 := b.Slice(0, 20) // should return 4 items
	assert.NotNil(t, slice2)
	assert.Equal(t, uint64(4), slice2.Count())
	
	slice1.Release()
	slice2.Release()
}

func TestAtomicBitset(t *testing.T) {
	ab := NewAtomicBitset()
	require.NotNil(t, ab)

	ab.Set(10)
	ab.Set(65)
	assert.True(t, ab.Contains(10))
	assert.True(t, ab.Contains(65))
	assert.False(t, ab.Contains(0))
	assert.Equal(t, uint64(2), ab.Count())

	arr := ab.ToUint32Array()
	assert.Len(t, arr, 2)
	assert.Contains(t, arr, uint32(10))
	assert.Contains(t, arr, uint32(65))

	ab.Clear(10)
	assert.False(t, ab.Contains(10))

	ab2 := ab.Clone()
	require.NotNil(t, ab2)
	assert.True(t, ab2.Contains(65))

	ab.Reset()
	assert.Equal(t, uint64(0), ab.Count())
	assert.False(t, ab.Contains(65))

	ab.Release()
	ab2.Release()
}

func TestBQEncoder(t *testing.T) {
	encoder := NewBQEncoder(128)
	require.NotNil(t, encoder)

	vec := make([]float32, 128)
	for i := range vec {
		if i%2 == 0 {
			vec[i] = 1.0
		} else {
			vec[i] = -1.0
		}
	}

	encoded := encoder.Encode(vec)
	assert.Len(t, encoded, 2) // 128 bits = 2 uint64

	dist := encoder.HammingDistance(encoded, encoded)
	assert.Equal(t, int(0), dist)
}
