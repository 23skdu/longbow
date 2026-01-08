package store

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBQEncoder_Encode(t *testing.T) {
	dims := 128
	enc := NewBQEncoder(dims)

	// Case 1: All Positive -> All 1s
	vec1 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = 1.0
	}
	code1 := enc.Encode(vec1)
	assert.Equal(t, 2, len(code1)) // 128 / 64 = 2
	assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), code1[0])
	assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), code1[1])

	// Case 2: All Negative -> All 0s
	vec2 := make([]float32, dims)
	for i := range vec2 {
		vec2[i] = -1.0
	}
	code2 := enc.Encode(vec2)
	assert.Equal(t, uint64(0), code2[0])
	assert.Equal(t, uint64(0), code2[1])

	// Case 3: Mixed
	vec3 := make([]float32, 64)
	vec3[0] = 1.0 // Bit 0 -> 1
	vec3[1] = -1.0
	vec3[63] = 1.0 // Bit 63 -> 1

	encSmall := NewBQEncoder(64)
	code3 := encSmall.Encode(vec3)
	assert.Equal(t, 1, len(code3))
	expected := uint64(1) | (1 << 63)
	assert.Equal(t, expected, code3[0])
}

func TestBQEncoder_HammingDistance(t *testing.T) {
	enc := NewBQEncoder(64)

	// dist(All0, All1) = 64
	a := []uint64{0}
	b := []uint64{0xFFFFFFFFFFFFFFFF}
	dist := enc.HammingDistance(a, b)
	assert.Equal(t, 64, dist)

	// dist(All0, 1) = 1
	c := []uint64{1}
	dist2 := enc.HammingDistance(a, c)
	assert.Equal(t, 1, dist2)
}

func TestBQEncoder_Integration(t *testing.T) {
	dims := 1024
	enc := NewBQEncoder(dims)

	// Generate two random vectors
	v1 := make([]float32, dims)
	v2 := make([]float32, dims)

	// Vectors pointing same direction (mostly)
	for i := 0; i < dims; i++ {
		val := rand.Float32() - 0.5
		v1[i] = val
		if rand.Float32() > 0.1 { // 90% chance same sign
			v2[i] = val
		} else {
			v2[i] = -val
		}
	}

	c1 := enc.Encode(v1)
	c2 := enc.Encode(v2)

	dist := enc.HammingDistance(c1, c2)
	// Expected distance approx 10% of 1024 = 102
	assert.True(t, dist > 50 && dist < 150, "Hamming distance %d should be around 102", dist)
}
