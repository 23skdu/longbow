package simd

import (
	"math"
	"unsafe"
)

var dequantL2Float32Uint8Impl = dequantL2Float32Uint8Generic

// DequantizeL2Float32Uint8 computes the Euclidean (L2) distance between a float32 query
// and an SQ8-quantized uint8 vector: sqrt(sum((q[i] - (minV + float32(v[i])*scale))^2)).
func DequantizeL2Float32Uint8(q []float32, v []uint8, minV, scale float32) float32 {
	if len(q) == 0 || len(v) == 0 {
		return 0
	}
	n := len(q)
	if len(v) < n {
		n = len(v)
	}
	sum := dequantL2Float32Uint8Impl(q[:n], v[:n], minV, scale)
	return float32(math.Sqrt(float64(sum)))
}

// DequantizeL2Float32Int8 computes the Euclidean (L2) distance between a float32 query
// and an SQ8-quantized int8 vector.
func DequantizeL2Float32Int8(q []float32, v []int8, minV, scale float32) float32 {
	if len(q) == 0 || len(v) == 0 {
		return 0
	}
	v8 := *(*[]uint8)(unsafe.Pointer(&v)) // #nosec G103
	return DequantizeL2Float32Uint8(q, v8, minV, scale)
}

// L2Float32Uint8 computes the Euclidean distance between a float32 query and unquantized uint8 vector.
func L2Float32Uint8(q []float32, v []uint8) float32 {
	return DequantizeL2Float32Uint8(q, v, 0, 1.0)
}

// L2Float32Int8 computes the Euclidean distance between a float32 query and unquantized int8 vector.
func L2Float32Int8(q []float32, v []int8) float32 {
	return DequantizeL2Float32Int8(q, v, 0, 1.0)
}

// DequantizeL2Uint8Uint8 computes the Euclidean distance between two SQ8-quantized vectors:
// sqrt(sum((deqQ[i] - deqV[i])^2)) = scale * sqrt(sum((q8[i] - v8[i])^2)).
func DequantizeL2Uint8Uint8(q8, v8 []uint8, scale float32) float32 {
	if len(q8) == 0 || len(v8) == 0 {
		return 0
	}
	n := len(q8)
	if len(v8) < n {
		n = len(v8)
	}
	dist, err := EuclideanDistanceSQ8(q8[:n], v8[:n])
	if err == nil {
		return float32(dist) * scale
	}
	var sum float32
	for i := 0; i < n; i++ {
		diff := float32(q8[i]) - float32(v8[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum))) * scale
}

func dequantL2Float32Uint8Generic(q []float32, v []uint8, minV, scale float32) float32 {
	n := len(q)
	if len(v) < n {
		n = len(v)
	}
	var sum float32
	i := 0
	for ; i+3 < n; i += 4 {
		deq0 := minV + float32(v[i])*scale
		deq1 := minV + float32(v[i+1])*scale
		deq2 := minV + float32(v[i+2])*scale
		deq3 := minV + float32(v[i+3])*scale
		d0 := q[i] - deq0
		d1 := q[i+1] - deq1
		d2 := q[i+2] - deq2
		d3 := q[i+3] - deq3
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3
	}
	for ; i < n; i++ {
		deq := minV + float32(v[i])*scale
		d := q[i] - deq
		sum += d * d
	}
	return sum
}
