//go:build darwin && arm64

package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

func TestARM64DarwinSIMDDispatch_AllDTypesAndDims(t *testing.T) {
	if impl := GetImplementation(); impl != "neon" && impl != "sve2" {
		t.Fatalf("expected ARM64 Darwin SIMD implementation neon or sve2, got %q", impl)
	}

	dimsList := []int{128, 384, 768}

	for _, dim := range dimsList {
		dim := dim
		t.Run(fmt.Sprintf("dim_%d", dim), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(2026 + dim))) // #nosec G404 -- deterministic

			t.Run("float32_general_and_fixed", func(t *testing.T) {
				a := make([]float32, dim)
				b := make([]float32, dim)
				var refL2Sq, refDot, refNormA, refNormB float64
				for i := 0; i < dim; i++ {
					a[i] = rng.Float32()*2 - 1
					b[i] = rng.Float32()*2 - 1
					da, db := float64(a[i]), float64(b[i])
					d := da - db
					refL2Sq += d * d
					refDot += da * db
					refNormA += da * da
					refNormB += db * db
				}
				refL2 := math.Sqrt(refL2Sq)
				refCos := 1.0 - refDot/(math.Sqrt(refNormA)*math.Sqrt(refNormB))

				gotL2, err := EuclideanDistance(a, b)
				if err != nil {
					t.Fatalf("EuclideanDistance: %v", err)
				}
				if relErr(float64(gotL2), refL2) > 1e-4 {
					t.Errorf("EuclideanDistance = %v, want %v", gotL2, refL2)
				}

				gotDot, err := DotProduct(a, b)
				if err != nil {
					t.Fatalf("DotProduct: %v", err)
				}
				if math.Abs(float64(gotDot)-refDot) > 1e-3 {
					t.Errorf("DotProduct = %v, want %v", gotDot, refDot)
				}

				gotCos, err := CosineDistance(a, b)
				if err != nil {
					t.Fatalf("CosineDistance: %v", err)
				}
				if math.Abs(float64(gotCos)-refCos) > 1e-4 {
					t.Errorf("CosineDistance = %v, want %v", gotCos, refCos)
				}

				gotSq, err := L2Squared(a, b)
				if err != nil {
					t.Fatalf("L2Squared: %v", err)
				}
				if relErr(float64(gotSq), refL2Sq) > 1e-4 {
					t.Errorf("L2Squared = %v, want %v", gotSq, refL2Sq)
				}

				// Specialized fixed-dimension dispatch table entries
				var fixedL2, fixedDot, fixedSq float32
				switch dim {
				case 128:
					fixedL2, err = currentDispatch.EuclideanDistance128(a, b)
					if err == nil {
						fixedDot, err = currentDispatch.DotProduct128(a, b)
					}
					if err == nil {
						fixedSq, err = currentDispatch.L2SquaredDistance128(a, b)
					}
				case 384:
					fixedL2, err = currentDispatch.EuclideanDistance384(a, b)
					if err == nil {
						fixedDot, err = currentDispatch.DotProduct384(a, b)
					}
					if err == nil {
						fixedSq, err = currentDispatch.L2SquaredDistance384(a, b)
					}
				case 768:
					fixedL2, err = currentDispatch.EuclideanDistance768(a, b)
					if err == nil {
						fixedDot, err = currentDispatch.DotProduct768(a, b)
					}
					if err == nil {
						fixedSq, err = currentDispatch.L2SquaredDistance768(a, b)
					}
				}
				if err != nil {
					t.Fatalf("fixed dim=%d dispatch error: %v", dim, err)
				}
				if relErr(float64(fixedL2), refL2) > 1e-4 {
					t.Errorf("EuclideanDistance%d = %v, want %v", dim, fixedL2, refL2)
				}
				if math.Abs(float64(fixedDot)-refDot) > 1e-3 {
					t.Errorf("DotProduct%d = %v, want %v", dim, fixedDot, refDot)
				}
				if relErr(float64(fixedSq), refL2Sq) > 1e-4 {
					t.Errorf("L2SquaredDistance%d = %v, want %v", dim, fixedSq, refL2Sq)
				}

				// Registry DispatchDistance
				dispL2, err := DispatchDistance(MetricEuclidean, a, b)
				if err != nil {
					t.Fatalf("DispatchDistance(Euclidean): %v", err)
				}
				if relErr(float64(dispL2), refL2) > 1e-4 {
					t.Errorf("DispatchDistance(Euclidean) = %v, want %v", dispL2, refL2)
				}
			})

			t.Run("float32_batch", func(t *testing.T) {
				const numVecs = 16
				q := make([]float32, dim)
				for i := range q {
					q[i] = rng.Float32()*2 - 1
				}
				vecs := make([][]float32, numVecs)
				for i := range vecs {
					vecs[i] = make([]float32, dim)
					for j := range vecs[i] {
						vecs[i][j] = rng.Float32()*2 - 1
					}
				}

				l2Res := make([]float32, numVecs)
				dotRes := make([]float32, numVecs)
				cosRes := make([]float32, numVecs)
				sqRes := make([]float32, numVecs)

				if err := EuclideanDistanceBatch(q, vecs, l2Res); err != nil {
					t.Fatalf("EuclideanDistanceBatch: %v", err)
				}
				if err := DotProductBatch(q, vecs, dotRes); err != nil {
					t.Fatalf("DotProductBatch: %v", err)
				}
				if err := CosineDistanceBatch(q, vecs, cosRes); err != nil {
					t.Fatalf("CosineDistanceBatch: %v", err)
				}
				if err := L2SquaredDistanceBatch(q, vecs, sqRes); err != nil {
					t.Fatalf("L2SquaredDistanceBatch: %v", err)
				}

				for i := 0; i < numVecs; i++ {
					wantL2, _ := EuclideanDistance(q, vecs[i])
					wantDot, _ := DotProduct(q, vecs[i])
					wantCos, _ := CosineDistance(q, vecs[i])
					wantSq, _ := L2Squared(q, vecs[i])
					if relErr(float64(l2Res[i]), float64(wantL2)) > 1e-4 {
						t.Errorf("batch L2[%d] = %v, want %v", i, l2Res[i], wantL2)
					}
					if math.Abs(float64(dotRes[i]-wantDot)) > 1e-3 {
						t.Errorf("batch Dot[%d] = %v, want %v", i, dotRes[i], wantDot)
					}
					if math.Abs(float64(cosRes[i]-wantCos)) > 1e-4 {
						t.Errorf("batch Cos[%d] = %v, want %v", i, cosRes[i], wantCos)
					}
					if relErr(float64(sqRes[i]), float64(wantSq)) > 1e-4 {
						t.Errorf("batch L2Sq[%d] = %v, want %v", i, sqRes[i], wantSq)
					}
				}
			})

			t.Run("float64", func(t *testing.T) {
				a := make([]float64, dim)
				b := make([]float64, dim)
				var refL2Sq, refDot, refNA, refNB float64
				for i := 0; i < dim; i++ {
					a[i] = rng.Float64()*2 - 1
					b[i] = rng.Float64()*2 - 1
					d := a[i] - b[i]
					refL2Sq += d * d
					refDot += a[i] * b[i]
					refNA += a[i] * a[i]
					refNB += b[i] * b[i]
				}
				gotL2, err := EuclideanDistanceFloat64(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceFloat64 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductF64(a, b)
				if err != nil || math.Abs(float64(gotDot)-refDot) > 1e-3 {
					t.Errorf("DotProductF64 = %v (err=%v), want %v", gotDot, err, refDot)
				}
				gotCos, err := CosineDistanceFloat64(a, b)
				refCos := 1.0 - refDot/(math.Sqrt(refNA)*math.Sqrt(refNB))
				if err != nil || math.Abs(float64(gotCos)-refCos) > 1e-4 {
					t.Errorf("CosineDistanceFloat64 = %v (err=%v), want %v", gotCos, err, refCos)
				}
			})

			t.Run("float16", func(t *testing.T) {
				a := make([]float16.Num, dim)
				b := make([]float16.Num, dim)
				var refL2Sq, refDot, refNA, refNB float64
				for i := 0; i < dim; i++ {
					a[i] = float16.New(rng.Float32()*2 - 1)
					b[i] = float16.New(rng.Float32()*2 - 1)
					fa, fb := float64(a[i].Float32()), float64(b[i].Float32())
					d := fa - fb
					refL2Sq += d * d
					refDot += fa * fb
					refNA += fa * fa
					refNB += fb * fb
				}
				gotL2, err := EuclideanDistanceF16(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-3 {
					t.Errorf("EuclideanDistanceF16 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductF16(a, b)
				if err != nil || math.Abs(float64(gotDot)-refDot) > 1e-2 {
					t.Errorf("DotProductF16 = %v (err=%v), want %v", gotDot, err, refDot)
				}
				gotCos, err := CosineDistanceF16(a, b)
				refCos := 1.0 - refDot/(math.Sqrt(refNA)*math.Sqrt(refNB))
				if err != nil || math.Abs(float64(gotCos)-refCos) > 1e-3 {
					t.Errorf("CosineDistanceF16 = %v (err=%v), want %v", gotCos, err, refCos)
				}
			})

			t.Run("complex64", func(t *testing.T) {
				a := make([]complex64, dim)
				b := make([]complex64, dim)
				var refL2Sq float64
				for i := 0; i < dim; i++ {
					a[i] = complex(rng.Float32()*2-1, rng.Float32()*2-1)
					b[i] = complex(rng.Float32()*2-1, rng.Float32()*2-1)
					dr := float64(real(a[i]) - real(b[i]))
					di := float64(imag(a[i]) - imag(b[i]))
					refL2Sq += dr*dr + di*di
				}
				gotL2, err := EuclideanDistanceComplex64(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-4 {
					t.Errorf("EuclideanDistanceComplex64 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
			})

			t.Run("complex128", func(t *testing.T) {
				a := make([]complex128, dim)
				b := make([]complex128, dim)
				var refL2Sq float64
				for i := 0; i < dim; i++ {
					a[i] = complex(rng.Float64()*2-1, rng.Float64()*2-1)
					b[i] = complex(rng.Float64()*2-1, rng.Float64()*2-1)
					dr := real(a[i]) - real(b[i])
					di := imag(a[i]) - imag(b[i])
					refL2Sq += dr*dr + di*di
				}
				gotL2, err := EuclideanDistanceComplex128(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceComplex128 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
			})

			t.Run("int8", func(t *testing.T) {
				a := make([]int8, dim)
				b := make([]int8, dim)
				var refL2Sq, refDot float64
				for i := 0; i < dim; i++ {
					a[i] = int8(rng.Intn(256) - 128)
					b[i] = int8(rng.Intn(256) - 128)
					d := float64(a[i]) - float64(b[i])
					refL2Sq += d * d
					refDot += float64(a[i]) * float64(b[i])
				}
				gotL2, err := EuclideanDistanceInt8(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceInt8 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductInt8(a, b)
				if err != nil || math.Abs(float64(gotDot)-refDot) > 1e-2 {
					t.Errorf("DotProductInt8 = %v (err=%v), want %v", gotDot, err, refDot)
				}
			})

			t.Run("uint8", func(t *testing.T) {
				a := make([]uint8, dim)
				b := make([]uint8, dim)
				var refL2Sq, refDot float64
				for i := 0; i < dim; i++ {
					a[i] = uint8(rng.Intn(256))
					b[i] = uint8(rng.Intn(256))
					d := float64(a[i]) - float64(b[i])
					refL2Sq += d * d
					refDot += float64(a[i]) * float64(b[i])
				}
				gotL2, err := EuclideanDistanceUint8(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceUint8 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductUint8(a, b)
				if err != nil || relErr(float64(gotDot), refDot) > 1e-5 {
					t.Errorf("DotProductUint8 = %v (err=%v), want %v", gotDot, err, refDot)
				}
			})

			t.Run("int16_full_range", func(t *testing.T) {
				a := make([]int16, dim)
				b := make([]int16, dim)
				var refL2Sq, refDot float64
				for i := 0; i < dim; i++ {
					a[i] = int16(rng.Intn(65536) - 32768)
					b[i] = int16(rng.Intn(65536) - 32768)
					d := float64(a[i]) - float64(b[i])
					refL2Sq += d * d
					refDot += float64(a[i]) * float64(b[i])
				}
				gotL2, err := EuclideanDistanceInt16(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceInt16 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductInt16(a, b)
				if err != nil || relErr(float64(gotDot), refDot) > 1e-5 {
					t.Errorf("DotProductInt16 = %v (err=%v), want %v", gotDot, err, refDot)
				}
			})

			t.Run("uint16_full_range", func(t *testing.T) {
				a := make([]uint16, dim)
				b := make([]uint16, dim)
				var refL2Sq, refDot float64
				for i := 0; i < dim; i++ {
					a[i] = uint16(rng.Intn(65536))
					b[i] = uint16(rng.Intn(65536))
					d := float64(a[i]) - float64(b[i])
					refL2Sq += d * d
					refDot += float64(a[i]) * float64(b[i])
				}
				gotL2, err := EuclideanDistanceUint16(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceUint16 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductUint16(a, b)
				if err != nil || relErr(float64(gotDot), refDot) > 1e-5 {
					t.Errorf("DotProductUint16 = %v (err=%v), want %v", gotDot, err, refDot)
				}
			})

			t.Run("int32", func(t *testing.T) {
				a := make([]int32, dim)
				b := make([]int32, dim)
				var refL2Sq, refDot float64
				for i := 0; i < dim; i++ {
					a[i] = int32(rng.Intn(20000) - 10000)
					b[i] = int32(rng.Intn(20000) - 10000)
					d := float64(a[i]) - float64(b[i])
					refL2Sq += d * d
					refDot += float64(a[i]) * float64(b[i])
				}
				gotL2, err := EuclideanDistanceInt32(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceInt32 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductInt32(a, b)
				if err != nil || relErr(float64(gotDot), refDot) > 1e-5 {
					t.Errorf("DotProductInt32 = %v (err=%v), want %v", gotDot, err, refDot)
				}
			})

			t.Run("uint32", func(t *testing.T) {
				a := make([]uint32, dim)
				b := make([]uint32, dim)
				var refL2Sq, refDot float64
				for i := 0; i < dim; i++ {
					a[i] = uint32(rng.Intn(20000))
					b[i] = uint32(rng.Intn(20000))
					d := float64(a[i]) - float64(b[i])
					refL2Sq += d * d
					refDot += float64(a[i]) * float64(b[i])
				}
				gotL2, err := EuclideanDistanceUint32(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceUint32 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductUint32(a, b)
				if err != nil || relErr(float64(gotDot), refDot) > 1e-5 {
					t.Errorf("DotProductUint32 = %v (err=%v), want %v", gotDot, err, refDot)
				}
			})

			t.Run("int64", func(t *testing.T) {
				a := make([]int64, dim)
				b := make([]int64, dim)
				var refL2Sq, refDot float64
				for i := 0; i < dim; i++ {
					a[i] = int64(rng.Intn(20000) - 10000)
					b[i] = int64(rng.Intn(20000) - 10000)
					d := float64(a[i]) - float64(b[i])
					refL2Sq += d * d
					refDot += float64(a[i]) * float64(b[i])
				}
				gotL2, err := EuclideanDistanceInt64(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceInt64 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductInt64(a, b)
				if err != nil || relErr(float64(gotDot), refDot) > 1e-5 {
					t.Errorf("DotProductInt64 = %v (err=%v), want %v", gotDot, err, refDot)
				}
			})

			t.Run("uint64", func(t *testing.T) {
				a := make([]uint64, dim)
				b := make([]uint64, dim)
				var refL2Sq, refDot float64
				for i := 0; i < dim; i++ {
					a[i] = uint64(rng.Intn(20000))
					b[i] = uint64(rng.Intn(20000))
					d := float64(a[i]) - float64(b[i])
					refL2Sq += d * d
					refDot += float64(a[i]) * float64(b[i])
				}
				gotL2, err := EuclideanDistanceUint64(a, b)
				if err != nil || relErr(float64(gotL2), math.Sqrt(refL2Sq)) > 1e-5 {
					t.Errorf("EuclideanDistanceUint64 = %v (err=%v), want %v", gotL2, err, math.Sqrt(refL2Sq))
				}
				gotDot, err := DotProductUint64(a, b)
				if err != nil || relErr(float64(gotDot), refDot) > 1e-5 {
					t.Errorf("DotProductUint64 = %v (err=%v), want %v", gotDot, err, refDot)
				}
			})
		})
	}
}
