package simd

import (
	"math"
	"testing"
)

func TestSinFloat32AVX2(t *testing.T) {
	src := []float32{0, math.Pi / 6, math.Pi / 4, math.Pi / 3, math.Pi / 2, -math.Pi / 4, -math.Pi / 2, 0.1, 0.2, 0.3}
	dstAVX2 := make([]float32, len(src))
	dstGen := make([]float32, len(src))

	sinAVX2(src, dstAVX2)
	sinFloat32Generic(src, dstGen)

	for i := range src {
		diff := math.Abs(float64(dstAVX2[i] - dstGen[i]))
		if diff > 1e-3 {
			t.Errorf("Sin mismatch at %d: src=%v, AVX2=%v, Gen=%v", i, src[i], dstAVX2[i], dstGen[i])
		}
	}
}

func TestCosFloat32AVX2(t *testing.T) {
	src := []float32{0, math.Pi / 6, math.Pi / 4, math.Pi / 3, math.Pi / 2, -math.Pi / 4, -math.Pi / 2, 0.1, 0.2, 0.3}
	dstAVX2 := make([]float32, len(src))
	dstGen := make([]float32, len(src))

	cosAVX2(src, dstAVX2)
	cosFloat32Generic(src, dstGen)

	for i := range src {
		diff := math.Abs(float64(dstAVX2[i] - dstGen[i]))
		if diff > 1e-3 {
			t.Errorf("Cos mismatch at %d: src=%v, AVX2=%v, Gen=%v", i, src[i], dstAVX2[i], dstGen[i])
		}
	}
}

func TestSincosFloat32AVX2(t *testing.T) {
	src := []float32{0, math.Pi / 6, math.Pi / 4, math.Pi / 3, math.Pi / 2, -math.Pi / 4, -math.Pi / 2, 0.1, 0.2, 0.3}
	sinAVX2 := make([]float32, len(src))
	cosAVX2 := make([]float32, len(src))
	sinGen := make([]float32, len(src))
	cosGen := make([]float32, len(src))

	sincosAVX2(src, sinAVX2, cosAVX2)
	sincosFloat32Generic(src, sinGen, cosGen)

	for i := range src {
		diffSin := math.Abs(float64(sinAVX2[i] - sinGen[i]))
		diffCos := math.Abs(float64(cosAVX2[i] - cosGen[i]))
		if diffSin > 1e-3 || diffCos > 1e-3 {
			t.Errorf("Sincos mismatch at %d: src=%v, Sin(AVX2=%v, Gen=%v), Cos(AVX2=%v, Gen=%v)", i, src[i], sinAVX2[i], sinGen[i], cosAVX2[i], cosGen[i])
		}
	}
}
