package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitmapPool(t *testing.T) {
	// 1. Get a bitmap
	bm1 := GetBitmap()
	assert.NotNil(t, bm1)
	assert.Equal(t, uint64(0), bm1.GetCardinality(), "New bitmap should be empty")

	// 2. Modify it
	bm1.Add(1)
	bm1.Add(100)
	assert.Equal(t, uint64(2), bm1.GetCardinality())

	// 3. Put it back
	PutBitmap(bm1)

	// 4. Get another one (should be reused and cleared)
	bm2 := GetBitmap()
	assert.NotNil(t, bm2)
	assert.Equal(t, uint64(0), bm2.GetCardinality(), "Recycled bitmap should be cleared")

	// Note: We can't strictly guarantee we got the *same* object instance without pointer comparison,
	// but we can check behavior.
}
