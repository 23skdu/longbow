package simd

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// l2Squared384Float64 calculates squared Euclidean distance for 384-dim float64 vectors.
func l2Squared384Float64(a, b []float64) float32 {
	var sum float64
	for i := 0; i < 384; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3
	}
	return float32(sum)
}

// l2Squared768Float64 calculates squared Euclidean distance for 768-dim float64 vectors.
func l2Squared768Float64(a, b []float64) float32 {
	var sum float64
	for i := 0; i < 768; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3
	}
	return float32(sum)
}

// l2Squared384Int8 calculates squared Euclidean distance for 384-dim int8 vectors.
func l2Squared384Int8(a, b []int8) float32 {
	var sum int32
	for i := 0; i < 384; i += 8 {
		d0 := int32(a[i]) - int32(b[i])
		d1 := int32(a[i+1]) - int32(b[i+1])
		d2 := int32(a[i+2]) - int32(b[i+2])
		d3 := int32(a[i+3]) - int32(b[i+3])
		d4 := int32(a[i+4]) - int32(b[i+4])
		d5 := int32(a[i+5]) - int32(b[i+5])
		d6 := int32(a[i+6]) - int32(b[i+6])
		d7 := int32(a[i+7]) - int32(b[i+7])
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3 + d4*d4 + d5*d5 + d6*d6 + d7*d7
	}
	return float32(sum)
}

// l2Squared768Int8 calculates squared Euclidean distance for 768-dim int8 vectors.
func l2Squared768Int8(a, b []int8) float32 {
	var sum int32
	for i := 0; i < 768; i += 8 {
		d0 := int32(a[i]) - int32(b[i])
		d1 := int32(a[i+1]) - int32(b[i+1])
		d2 := int32(a[i+2]) - int32(b[i+2])
		d3 := int32(a[i+3]) - int32(b[i+3])
		d4 := int32(a[i+4]) - int32(b[i+4])
		d5 := int32(a[i+5]) - int32(b[i+5])
		d6 := int32(a[i+6]) - int32(b[i+6])
		d7 := int32(a[i+7]) - int32(b[i+7])
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3 + d4*d4 + d5*d5 + d6*d6 + d7*d7
	}
	return float32(sum)
}

// l2Squared384Float16 calculates squared Euclidean distance for 384-dim float16 vectors.
func l2Squared384Float16(a, b []float16.Num) float32 {
	var sum float32
	for i := 0; i < 384; i += 4 {
		d0 := a[i].Float32() - b[i].Float32()
		d1 := a[i+1].Float32() - b[i+1].Float32()
		d2 := a[i+2].Float32() - b[i+2].Float32()
		d3 := a[i+3].Float32() - b[i+3].Float32()
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3
	}
	return sum
}

// l2Squared768Float16 calculates squared Euclidean distance for 768-dim float16 vectors.
func l2Squared768Float16(a, b []float16.Num) float32 {
	var sum float32
	for i := 0; i < 768; i += 4 {
		d0 := a[i].Float32() - b[i].Float32()
		d1 := a[i+1].Float32() - b[i+1].Float32()
		d2 := a[i+2].Float32() - b[i+2].Float32()
		d3 := a[i+3].Float32() - b[i+3].Float32()
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3
	}
	return sum
}

// --- Wrapper Helpers for Registry ---

// Euclidean384Float64 calculates Euclidean distance for 384-dim float64 vectors.
func Euclidean384Float64(a, b []float64) (float32, error) {
	return float32(math.Sqrt(float64(l2Squared384Float64(a, b)))), nil
}

// Euclidean768Float64 calculates Euclidean distance for 768-dim float64 vectors.
func Euclidean768Float64(a, b []float64) (float32, error) {
	return float32(math.Sqrt(float64(l2Squared768Float64(a, b)))), nil
}

// Euclidean384Int8 calculates Euclidean distance for 384-dim int8 vectors.
func Euclidean384Int8(a, b []int8) (float32, error) {
	return float32(math.Sqrt(float64(l2Squared384Int8(a, b)))), nil
}

// Euclidean768Int8 calculates Euclidean distance for 768-dim int8 vectors.
func Euclidean768Int8(a, b []int8) (float32, error) {
	return float32(math.Sqrt(float64(l2Squared768Int8(a, b)))), nil
}

// Euclidean384Float16 calculates Euclidean distance for 384-dim float16 vectors.
func Euclidean384Float16(a, b []float16.Num) (float32, error) {
	return float32(math.Sqrt(float64(l2Squared384Float16(a, b)))), nil
}

// Euclidean768Float16 calculates Euclidean distance for 768-dim float16 vectors.
func Euclidean768Float16(a, b []float16.Num) (float32, error) {
	return float32(math.Sqrt(float64(l2Squared768Float16(a, b)))), nil
}

// --- Dot Product Unrolled ---

func dot384Float64(a, b []float64) float32 {
	var sum float64
	for i := 0; i < 384; i += 4 {
		sum += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
	}
	return float32(sum)
}

func dot768Float64(a, b []float64) float32 {
	var sum float64
	for i := 0; i < 768; i += 4 {
		sum += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
	}
	return float32(sum)
}

func dot384Int8(a, b []int8) float32 {
	var sum int32
	for i := 0; i < 384; i += 8 {
		sum += int32(a[i])*int32(b[i]) + int32(a[i+1])*int32(b[i+1]) + int32(a[i+2])*int32(b[i+2]) + int32(a[i+3])*int32(b[i+3]) +
			int32(a[i+4])*int32(b[i+4]) + int32(a[i+5])*int32(b[i+5]) + int32(a[i+6])*int32(b[i+6]) + int32(a[i+7])*int32(b[i+7])
	}
	return float32(sum)
}

func dot768Int8(a, b []int8) float32 {
	var sum int32
	for i := 0; i < 768; i += 8 {
		sum += int32(a[i])*int32(b[i]) + int32(a[i+1])*int32(b[i+1]) + int32(a[i+2])*int32(b[i+2]) + int32(a[i+3])*int32(b[i+3]) +
			int32(a[i+4])*int32(b[i+4]) + int32(a[i+5])*int32(b[i+5]) + int32(a[i+6])*int32(b[i+6]) + int32(a[i+7])*int32(b[i+7])
	}
	return float32(sum)
}

func dot384Float16(a, b []float16.Num) float32 {
	var sum float32
	for i := 0; i < 384; i += 4 {
		sum += a[i].Float32()*b[i].Float32() + a[i+1].Float32()*b[i+1].Float32() + a[i+2].Float32()*b[i+2].Float32() + a[i+3].Float32()*b[i+3].Float32()
	}
	return sum
}

func dot768Float16(a, b []float16.Num) float32 {
	var sum float32
	for i := 0; i < 768; i += 4 {
		sum += a[i].Float32()*b[i].Float32() + a[i+1].Float32()*b[i+1].Float32() + a[i+2].Float32()*b[i+2].Float32() + a[i+3].Float32()*b[i+3].Float32()
	}
	return sum
}

// --- Dot Product Wrappers ---

// Dot384Float64 calculates dot product for 384-dim float64 vectors.
func Dot384Float64(a, b []float64) (float32, error) {
	return dot384Float64(a, b), nil
}

// Dot768Float64 calculates dot product for 768-dim float64 vectors.
func Dot768Float64(a, b []float64) (float32, error) {
	return dot768Float64(a, b), nil
}

// Dot384Int8 calculates dot product for 384-dim int8 vectors.
func Dot384Int8(a, b []int8) (float32, error) {
	return dot384Int8(a, b), nil
}

// Dot768Int8 calculates dot product for 768-dim int8 vectors.
func Dot768Int8(a, b []int8) (float32, error) {
	return dot768Int8(a, b), nil
}

// Dot384Float16 calculates dot product for 384-dim float16 vectors.
func Dot384Float16(a, b []float16.Num) (float32, error) {
	return dot384Float16(a, b), nil
}

// Dot768Float16 calculates dot product for 768-dim float16 vectors.
func Dot768Float16(a, b []float16.Num) (float32, error) {
	return dot768Float16(a, b), nil
}
