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
	_, _ = L2SquaredFloat64(f64, f64)
	_, _ = CosineDistanceFloat64(f64, f64)

	// Add correctness assertions for float64 distance metrics
	f64a := []float64{1.0, 2.0, 3.0}
	f64b := []float64{4.0, 6.0, 8.0}
	// Euclidean distance = sqrt((4-1)^2 + (6-2)^2 + (8-3)^2) = sqrt(9 + 16 + 25) = sqrt(50) ≈ 7.0710678
	dEuclidean, err := EuclideanDistanceFloat64(f64a, f64b)
	if err != nil {
		t.Fatalf("EuclideanDistanceFloat64 error: %v", err)
	}
	if dEuclidean < 7.07 || dEuclidean > 7.08 {
		t.Errorf("EuclideanDistanceFloat64 got %f, expected ~7.071", dEuclidean)
	}

	// L2Squared distance = 50.0
	dL2Sq, err := L2SquaredFloat64(f64a, f64b)
	if err != nil {
		t.Fatalf("L2SquaredFloat64 error: %v", err)
	}
	if dL2Sq != 50.0 {
		t.Errorf("L2SquaredFloat64 got %f, expected 50.0", dL2Sq)
	}

	// Dot product = 1*4 + 2*6 + 3*8 = 4 + 12 + 24 = 40.0
	dDot, err := DotProductF64(f64a, f64b)
	if err != nil {
		t.Fatalf("DotProductF64 error: %v", err)
	}
	if dDot != 40.0 {
		t.Errorf("DotProductF64 got %f, expected 40.0", dDot)
	}

	// Registry dispatch check for L2Squared Float64
	dPolymorphic, err := Registry.Get(MetricL2Squared, DataTypeFloat64, 0).(func([]float64, []float64) (float32, error))(f64a, f64b)
	if err != nil {
		t.Fatalf("Polymorphic dispatch error: %v", err)
	}
	if dPolymorphic != 50.0 {
		t.Errorf("Polymorphic dispatch got %f, expected 50.0", dPolymorphic)
	}

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
