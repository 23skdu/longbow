package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// TestSIMDAllDTypesParity verifies Euclidean and Dot distance parity against
// exact float64 scalar references across all supported dtypes (roadmap B4).
func TestSIMDAllDTypesParity(t *testing.T) {
	dimsList := []int{128, 384, 768}
	rng := rand.New(rand.NewSource(20261118)) // #nosec G404 -- deterministic test

	for _, dims := range dimsList {
		t.Run(fmt.Sprintf("dim=%d", dims), func(t *testing.T) {
			aF32, bF32 := make([]float32, dims), make([]float32, dims)
			aF64, bF64 := make([]float64, dims), make([]float64, dims)
			aF16, bF16 := make([]float16.Num, dims), make([]float16.Num, dims)
			aI8, bI8 := make([]int8, dims), make([]int8, dims)
			aI16, bI16 := make([]int16, dims), make([]int16, dims)
			aU8, bU8 := make([]uint8, dims), make([]uint8, dims)
			aU16, bU16 := make([]uint16, dims), make([]uint16, dims)
			aU32, bU32 := make([]uint32, dims), make([]uint32, dims)

			var refL2F32, refDotF32 float64
			var refL2F64, refDotF64 float64
			var refL2F16, refDotF16 float64
			var refL2I8, refDotI8 float64
			var refL2I16, refDotI16 float64
			var refL2U8, refDotU8 float64
			var refL2U16, refDotU16 float64
			var refL2U32, refDotU32 float64

			for i := range dims {
				af, bf := rng.Float64()*2-1, rng.Float64()*2-1
				aF64[i], bF64[i] = af, bf
				refL2F64 += (af - bf) * (af - bf)
				refDotF64 += af * bf

				af32, bf32 := float32(af), float32(bf)
				aF32[i], bF32[i] = af32, bf32
				refL2F32 += float64(af32-bf32) * float64(af32-bf32)
				refDotF32 += float64(af32) * float64(bf32)

				aF16[i], bF16[i] = float16.New(af32), float16.New(bf32)
				af16, bf16 := float64(aF16[i].Float32()), float64(bF16[i].Float32())
				refL2F16 += (af16 - bf16) * (af16 - bf16)
				refDotF16 += af16 * bf16

				ai8, bi8 := int8(rng.Intn(200)-100), int8(rng.Intn(200)-100)
				aI8[i], bI8[i] = ai8, bi8
				refL2I8 += float64(int(ai8)-int(bi8)) * float64(int(ai8)-int(bi8))
				refDotI8 += float64(ai8) * float64(bi8)

				ai16, bi16 := int16(rng.Intn(2000)-1000), int16(rng.Intn(2000)-1000)
				aI16[i], bI16[i] = ai16, bi16
				refL2I16 += float64(int(ai16)-int(bi16)) * float64(int(ai16)-int(bi16))
				refDotI16 += float64(ai16) * float64(bi16)

				au8, bu8 := uint8(rng.Intn(256)), uint8(rng.Intn(256))
				aU8[i], bU8[i] = au8, bu8
				refL2U8 += float64(int(au8)-int(bu8)) * float64(int(au8)-int(bu8))
				refDotU8 += float64(au8) * float64(bu8)

				au16, bu16 := uint16(rng.Intn(2000)), uint16(rng.Intn(2000))
				aU16[i], bU16[i] = au16, bu16
				refL2U16 += float64(int(au16)-int(bu16)) * float64(int(au16)-int(bu16))
				refDotU16 += float64(au16) * float64(bu16)

				au32, bu32 := uint32(rng.Intn(2000)), uint32(rng.Intn(2000))
				aU32[i], bU32[i] = au32, bu32
				refL2U32 += float64(int64(au32)-int64(bu32)) * float64(int64(au32)-int64(bu32))
				refDotU32 += float64(au32) * float64(bu32)
			}

			checkClose := func(name string, got float32, want float64, relTol float64) {
				t.Helper()
				absDiff := math.Abs(float64(got) - want)
				denom := math.Max(1.0, math.Abs(want))
				if absDiff/denom > relTol {
					t.Errorf("%s: got %v, want %v (relErr=%v > %v)", name, got, want, absDiff/denom, relTol)
				}
			}

			g, err := EuclideanDistance(aF32, bF32)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("float32/L2", g, math.Sqrt(refL2F32), 1e-4)
			g, err = DotProduct(aF32, bF32)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("float32/Dot", g, refDotF32, 1e-4)

			g, err = EuclideanDistanceFloat64(aF64, bF64)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("float64/L2", g, math.Sqrt(refL2F64), 1e-5)
			g, err = DotProductF64(aF64, bF64)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("float64/Dot", g, refDotF64, 1e-5)

			g, err = EuclideanDistanceF16(aF16, bF16)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("float16/L2", g, math.Sqrt(refL2F16), 1e-3)
			g, err = DotProductF16(aF16, bF16)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("float16/Dot", g, refDotF16, 1e-3)

			g, err = EuclideanDistanceInt8(aI8, bI8)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("int8/L2", g, math.Sqrt(refL2I8), 1e-4)
			g, err = DotProductInt8(aI8, bI8)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("int8/Dot", g, refDotI8, 1e-4)

			g, err = EuclideanDistanceInt16(aI16, bI16)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("int16/L2", g, math.Sqrt(refL2I16), 1e-4)
			g, err = DotProductInt16(aI16, bI16)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("int16/Dot", g, refDotI16, 1e-4)

			g, err = EuclideanDistanceUint8(aU8, bU8)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("uint8/L2", g, math.Sqrt(refL2U8), 1e-4)
			g, err = DotProductUint8(aU8, bU8)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("uint8/Dot", g, refDotU8, 1e-4)

			g, err = EuclideanDistanceUint16(aU16, bU16)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("uint16/L2", g, math.Sqrt(refL2U16), 1e-4)
			g, err = DotProductUint16(aU16, bU16)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("uint16/Dot", g, refDotU16, 1e-4)

			g, err = EuclideanDistanceUint32(aU32, bU32)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("uint32/L2", g, math.Sqrt(refL2U32), 1e-4)
			g, err = DotProductUint32(aU32, bU32)
			if err != nil {
				t.Fatal(err)
			}
			checkClose("uint32/Dot", g, refDotU32, 1e-4)
		})
	}
}

// BenchmarkSIMDAllDTypesParity benchmarks Euclidean distance across all
// supported vector dtypes (roadmap B4).
func BenchmarkSIMDAllDTypesParity(b *testing.B) {
	const dims = 384
	rng := rand.New(rand.NewSource(42)) // #nosec G404 -- deterministic bench

	aF32, bF32 := make([]float32, dims), make([]float32, dims)
	aF64, bF64 := make([]float64, dims), make([]float64, dims)
	aF16, bF16 := make([]float16.Num, dims), make([]float16.Num, dims)
	aC64, bC64 := make([]complex64, dims/2), make([]complex64, dims/2)
	aC128, bC128 := make([]complex128, dims/2), make([]complex128, dims/2)
	aI8, bI8 := make([]int8, dims), make([]int8, dims)
	aI16, bI16 := make([]int16, dims), make([]int16, dims)
	aU8, bU8 := make([]uint8, dims), make([]uint8, dims)
	aU16, bU16 := make([]uint16, dims), make([]uint16, dims)
	aU32, bU32 := make([]uint32, dims), make([]uint32, dims)

	for i := range dims {
		af, bf := rng.Float64(), rng.Float64()
		aF64[i], bF64[i] = af, bf
		aF32[i], bF32[i] = float32(af), float32(bf)
		aF16[i], bF16[i] = float16.New(float32(af)), float16.New(float32(bf))
		aI8[i], bI8[i] = int8(rng.Intn(127)), int8(rng.Intn(127))
		aI16[i], bI16[i] = int16(rng.Intn(1000)), int16(rng.Intn(1000))
		aU8[i], bU8[i] = uint8(rng.Intn(255)), uint8(rng.Intn(255))
		aU16[i], bU16[i] = uint16(rng.Intn(1000)), uint16(rng.Intn(1000))
		aU32[i], bU32[i] = uint32(rng.Intn(1000)), uint32(rng.Intn(1000))
	}
	for i := range dims / 2 {
		aC64[i] = complex(aF32[2*i], aF32[2*i+1])
		bC64[i] = complex(bF32[2*i], bF32[2*i+1])
		aC128[i] = complex(aF64[2*i], aF64[2*i+1])
		bC128[i] = complex(bF64[2*i], bF64[2*i+1])
	}

	b.Run("float32", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistance(aF32, bF32)
		}
	})
	b.Run("float64", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistanceFloat64(aF64, bF64)
		}
	})
	b.Run("float16", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistanceF16(aF16, bF16)
		}
	})
	b.Run("complex64_batch1", func(b *testing.B) {
		vecs := [][]complex64{bC64}
		out := make([]float32, 1)
		scratch := make([][]float32, 1)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = EuclideanDistanceComplex64Batch(aC64, vecs, out, scratch)
		}
	})
	b.Run("complex128_batch1", func(b *testing.B) {
		vecs := [][]complex128{bC128}
		out := make([]float32, 1)
		scratch := make([][]float64, 1)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_ = EuclideanDistanceComplex128Batch(aC128, vecs, out, scratch)
		}
	})
	b.Run("int8", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistanceInt8(aI8, bI8)
		}
	})
	b.Run("int16", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistanceInt16(aI16, bI16)
		}
	})
	b.Run("uint8", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistanceUint8(aU8, bU8)
		}
	})
	b.Run("uint16", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistanceUint16(aU16, bU16)
		}
	})
	b.Run("uint32", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistanceUint32(aU32, bU32)
		}
	})
}
