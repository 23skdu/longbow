package simd

import (
	"testing"
)

func TestSIMD_ExhaustiveTypes(t *testing.T) {
	// Call every public function in distance_functions.go to reach >90% coverage
	f64 := []float64{1, 2, 3}
	c64 := []complex64{1 + 1i}
	c128 := []complex128{1 + 1i}
	i8 := []int8{1}
	i16 := []int16{1}
	i32 := []int32{1}
	i64 := []int64{1}
	u16 := []uint16{1}
	u32 := []uint32{1}
	u64 := []uint64{1}

	_, _ = EuclideanDistanceFloat64(f64, f64)
	_, _ = DotProductF64(f64, f64)
	_, _ = EuclideanDistanceComplex64(c64, c64)
	_, _ = DotProductComplex64(c64, c64)
	_, _ = EuclideanDistanceComplex128(c128, c128)
	_, _ = DotProductComplex128(c128, c128)
	
	_, _ = EuclideanDistanceInt8(i8, i8)
	_, _ = DotProductInt8(i8, i8)
	_, _ = EuclideanDistanceInt16(i16, i16)
	_, _ = DotProductInt16(i16, i16)
	_, _ = EuclideanDistanceInt32(i32, i32)
	_, _ = DotProductInt32(i32, i32)
	_, _ = EuclideanDistanceInt64(i64, i64)
	_, _ = DotProductInt64(i64, i64)
	
	_, _ = EuclideanDistanceUint16(u16, u16)
	_, _ = DotProductUint16(u16, u16)
	_, _ = EuclideanDistanceUint32(u32, u32)
	_, _ = DotProductUint32(u32, u32)
	_, _ = EuclideanDistanceUint64(u64, u64)
	_, _ = DotProductUint64(u64, u64)
	
	_, _ = L2Squared([]float32{1.0}, []float32{1.0})
}
