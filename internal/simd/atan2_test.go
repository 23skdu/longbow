package simd

import (
	"math"
	"testing"
)

func TestAtan2Float32AVX2(t *testing.T) {
	y := []float32{0, 1, 1, -1, -1, 0.5, -0.5}
	x := []float32{1, 1, -1, 1, -1, 0.5, 0.5}
	dstAVX2 := make([]float32, len(y))
	dstGen := make([]float32, len(y))

	atan2AVX2(y, x, dstAVX2)
	atan2Float32Generic(y, x, dstGen)

	for i := range y {
		// Compare against the exact polynomial used in haversine
		x_val := x[i]
		y_val := y[i]
		var ratio float32
		if math.Abs(float64(x_val)) < math.Abs(float64(y_val)) {
			ratio = float32(math.Abs(float64(x_val)) / math.Abs(float64(y_val)))
		} else {
			ratio = float32(math.Abs(float64(y_val)) / math.Abs(float64(x_val)))
		}
		s := ratio * ratio
		p := ratio + ratio*s*(float32(-1.0/3.0)+s*(float32(1.0/5.0)+s*(float32(-1.0/7.0)+s*float32(1.0/9.0))))
		if math.Abs(float64(y_val)) > math.Abs(float64(x_val)) {
			p = math.Pi/2 - p
		}
		if x_val < 0 {
			p = math.Pi - p
		}
		if y_val < 0 {
			p = -p
		}

		diff := math.Abs(float64(dstAVX2[i] - p))
		if diff > 1e-4 {
			t.Errorf("Atan2 mismatch at %d: y=%v, x=%v, AVX2=%v, Poly=%v", i, y[i], x[i], dstAVX2[i], p)
		}
	}
}
