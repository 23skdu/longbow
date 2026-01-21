package simd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuantizeSQ8(t *testing.T) {
	src := []float32{0.0, 0.5, 1.0, -1.0, 2.0}
	dst := make([]byte, len(src))
	minVal := float32(0.0)
	maxVal := float32(1.0)

	QuantizeSQ8(src, dst, minVal, maxVal)

	assert.Equal(t, byte(0), dst[0])   // 0.0 -> 0
	assert.Equal(t, byte(127), dst[1]) // 0.5 -> ~127
	assert.Equal(t, byte(255), dst[2]) // 1.0 -> 255
	assert.Equal(t, byte(0), dst[3])   // -1.0 -> 0 (clamp)
	assert.Equal(t, byte(255), dst[4]) // 2.0 -> 255 (clamp)
}

func TestComputeBounds(t *testing.T) {
	tests := []struct {
		name    string
		vec     []float32
		wantMin float32
		wantMax float32
	}{
		{"Normal", []float32{1.0, 5.0, -2.0, 3.0}, -2.0, 5.0},
		{"Single", []float32{42.0}, 42.0, 42.0},
		{"Empty", []float32{}, 0.0, 0.0},
		{"Same", []float32{1.0, 1.0, 1.0}, 1.0, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minVal, maxVal := ComputeBounds(tt.vec)
			assert.Equal(t, tt.wantMin, minVal)
			assert.Equal(t, tt.wantMax, maxVal)
		})
	}
}
