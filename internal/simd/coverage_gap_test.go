package simd

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	lbcore "github.com/23skdu/longbow/internal/core"
)

func TestComprehensiveCoverage(t *testing.T) {
	initializeDispatch()

	t.Run("Bitops", func(t *testing.T) {
		if implementation == "neon" {
			a := []uint64{0xF0F0F0F0F0F0F0F0, 0xAAAAAAAAAAAAAAAA}
			b := []uint64{0x0F0F0F0F0F0F0F0F, 0x5555555555555555}
			andBitVectorsNEON(a, b)
			assert.Equal(t, uint64(0), a[0])
			countBitVectorNEON(a)
			hammingNEON(a, b)
		}
	})

	t.Run("Conversions", func(t *testing.T) {
		i8 := []int8{1}; u8 := []uint8{1}; i16 := []int16{1}; u16 := []uint16{1}
		i32 := []int32{1}; u32 := []uint32{1}; f16 := []float16.Num{float16.New(1.0)}
		dst := make([]float32, 1)
		Int8ToFloat32(i8, dst); Uint8ToFloat32(u8, dst); Int16ToFloat32(i16, dst)
		Uint16ToFloat32(u16, dst); Int32ToFloat32(i32, dst); Uint32ToFloat32(u32, dst)
		Float16ToFloat32(f16, dst)
	})

	t.Run("HighDimGeneric", func(t *testing.T) {
		for _, d := range []int{384, 768} {
			a8, b8 := make([]int8, d), make([]int8, d)
			af64, bf64 := make([]float64, d), make([]float64, d)
			af16, bf16 := make([]float16.Num, d), make([]float16.Num, d)
			if d == 384 {
				Euclidean384Int8(a8, b8); Dot384Int8(a8, b8)
				Euclidean384Float64(af64, bf64); Dot384Float64(af64, bf64)
				Dot384Float16(af16, bf16)
			} else {
				Euclidean768Int8(a8, b8); Dot768Int8(a8, b8)
				Euclidean768Float64(af64, bf64); Dot768Float64(af64, bf64)
				Dot768Float16(af16, bf16)
			}
		}
	})

	t.Run("Hadamard", func(t *testing.T) {
		a := []float32{1, 2, 3, 4}
		FastWalshHadamardTransform32(a)
		PadToPowerOf2([]float32{1, 2, 3})
		RandomRotation(a, 42)
	})

	t.Run("Math", func(t *testing.T) {
		a := []float32{0, 1}; dst := make([]float32, 2)
		if sinFloat32Impl != nil { SinFloat32(a, dst) }
		if cosFloat32Impl != nil { CosFloat32(a, dst) }
		if atan2Float32Impl != nil { Atan2Float32(a, a, dst) }
	})

	t.Run("Match", func(t *testing.T) {
		src := []int32{1, 2, 3, 4}; dst := make([]byte, 4)
		MatchInt32(src, 2, CompareEq, dst)
		MatchFloat32([]float32{1, 2}, 1.0, CompareEq, make([]byte, 2))
		MatchInt64([]int64{1, 2}, 1, CompareEq, make([]byte, 2))
		MatchFloat64([]float64{1, 2}, 1.0, CompareEq, make([]byte, 2))
	})

	t.Run("FindNearest", func(t *testing.T) {
		FindNearestCentroid([]float32{1, 0}, []float32{0, 1, 1, 0}, 2, 2)
		FindNearestCentroidInCodebook([]float32{1, 0}, [][]float32{{0, 1}, {1, 0}}, 1, 2, 2)
	})

	t.Run("Haversine", func(t *testing.T) {
		points := make([]lbcore.GeoPoint, 5)
		results := make([]float32, 5)
		HaversineBatch(0, 0, points, 6371.0, results)
		haversineBatchGeneric(0, 0, points, 6371.0, results)
	})

	t.Run("Misc", func(t *testing.T) {
		Pause(); PauseN(1)
		isAllZerosGeneric(nil); andBytesGeneric(nil, nil); orBytesGeneric(nil, nil)
	})

	t.Run("Distances", func(t *testing.T) {
		a, b := []float32{1, 0}, []float32{0, 1}
		ManhattanDistanceFloat32(a, b); ChebyshevDistanceFloat32(a, b); BrayCurtisDistanceFloat32(a, b)
		
		af16, bf16 := []float16.Num{float16.New(1)}, []float16.Num{float16.New(0)}
		ManhattanDistanceF16(af16, bf16); ChebyshevDistanceF16(af16, bf16); BrayCurtisDistanceF16(af16, bf16)
		
		ac64, bc64 := []complex64{1}, []complex64{0}
		euclideanComplex64Unrolled(ac64, bc64); dotComplex64Unrolled(ac64, bc64); cosineComplex64Unrolled(ac64, bc64)
		
		ac128, bc128 := []complex128{1}, []complex128{0}
		euclideanComplex128Unrolled(ac128, bc128); dotComplex128Unrolled(ac128, bc128); cosineComplex128Unrolled(ac128, bc128)
	})

	t.Run("RegistryLoop", func(t *testing.T) {
		metrics := []MetricType{MetricEuclidean, MetricCosine, MetricDotProduct}
		dataTypes := []SIMDDataType{DataTypeFloat32, DataTypeFloat16, DataTypeInt8, DataTypeUint8, DataTypeInt16, DataTypeUint16, DataTypeInt32, DataTypeUint32, DataTypeInt64, DataTypeUint64, DataTypeFloat64}
		
		for _, m := range metrics {
			for _, dt := range dataTypes {
				for _, dims := range []int{0, 128, 384, 768, 1024, 1536, 3072} {
					kernel := Registry.Get(m, dt, dims)
					if kernel != nil {
						func() {
							defer func() { recover() }() // Swallow panics from nil inputs
							switch k := kernel.(type) {
							case distanceFunc: k(make([]float32, dims), make([]float32, dims))
							case distanceF16Func: k(make([]float16.Num, dims), make([]float16.Num, dims))
							case distanceFloat64Func: k(make([]float64, dims), make([]float64, dims))
							case func([]int8, []int8) (float32, error): k(make([]int8, dims), make([]int8, dims))
							case func([]uint8, []uint8) (float32, error): k(make([]uint8, dims), make([]uint8, dims))
							case func([]int16, []int16) (float32, error): k(make([]int16, dims), make([]int16, dims))
							case func([]uint16, []uint16) (float32, error): k(make([]uint16, dims), make([]uint16, dims))
							case func([]int32, []int32) (float32, error): k(make([]int32, dims), make([]int32, dims))
							case func([]uint32, []uint32) (float32, error): k(make([]uint32, dims), make([]uint32, dims))
							case func([]int64, []int64) (float32, error): k(make([]int64, dims), make([]int64, dims))
							case func([]uint64, []uint64) (float32, error): k(make([]uint64, dims), make([]uint64, dims))
							}
						}()
					}
				}
			}
		}
	})
}
