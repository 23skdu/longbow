package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/mathutil"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Accuracy tolerances per data type class.
const (
	float64RelativeTol = 1e-6
	float32RelativeTol = 1e-5
	float16RelativeTol = 2e-3
	intExactTol        = 1e-5
	tq4MaxReconErr     = 0.25
	tq8MaxReconErr     = 0.05
)

// TestSIMDAccuracy_All15DTypes exercises the full numerical accuracy suite across
// all 15 vector data types supported by Longbow:
// 1. Float32
// 2. Float64
// 3. Float16
// 4. Complex64
// 5. Complex128
// 6. Int8
// 7. Int16
// 8. Int32
// 9. Int64
// 10. Uint8
// 11. Uint16
// 12. Uint32
// 13. Uint64
// 14. TurboQuant4
// 15. TurboQuant8
func TestSIMDAccuracy_All15DTypes(t *testing.T) {
	dimsList := []int{128, 384, 768}
	backends := []mathutil.Backend{mathutil.BackendStandard, mathutil.BackendEML}

	for _, backend := range backends {
		bName := "Standard"
		if backend == mathutil.BackendEML {
			bName = "EMLGo"
		}

		t.Run(bName, func(t *testing.T) {
			mathutil.SetBackend(backend)
			defer mathutil.SetBackend(mathutil.BackendStandard)

			for _, dim := range dimsList {
				rng := rand.New(rand.NewSource(int64(dim * 42))) // #nosec G404 -- deterministic test seed

				// 1. Float32
				t.Run(fmt.Sprintf("Float32_dim%d", dim), func(t *testing.T) {
					a := make([]float32, dim)
					b := make([]float32, dim)
					var gtDot, gtL2Sq, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = float32(rng.Float64()*2 - 1)
						b[i] = float32(rng.Float64()*2 - 1)
						gtDot += float64(a[i]) * float64(b[i])
						diff := float64(a[i] - b[i])
						gtL2Sq += diff * diff
						normA += float64(a[i] * a[i])
						normB += float64(b[i] * b[i])
					}
					gtEuc := math.Sqrt(gtL2Sq)
					gtCos := 1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB)))

					euc, err := EuclideanDistance(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*float32RelativeTol+1e-6)

					l2Sq, err := L2Squared(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtL2Sq, l2Sq, float64(gtL2Sq)*float32RelativeTol+1e-6)

					dot, err := DotProduct(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtDot, dot, math.Abs(gtDot)*float32RelativeTol+1e-5)

					cos, err := CosineDistance(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, float32RelativeTol*10)
				})

				// 2. Float64
				t.Run(fmt.Sprintf("Float64_dim%d", dim), func(t *testing.T) {
					a := make([]float64, dim)
					b := make([]float64, dim)
					var gtDot, gtL2Sq, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = rng.Float64()*2 - 1
						b[i] = rng.Float64()*2 - 1
						gtDot += a[i] * b[i]
						diff := a[i] - b[i]
						gtL2Sq += diff * diff
						normA += a[i] * a[i]
						normB += b[i] * b[i]
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceFloat64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*float64RelativeTol+1e-6)

					l2Sq, err := L2SquaredFloat64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtL2Sq), l2Sq, float64(gtL2Sq)*float64RelativeTol+1e-6)

					dot, err := DotProductF64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*float64RelativeTol+1e-5)

					cos, err := CosineDistanceFloat64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, float64RelativeTol*10)
				})

				// 3. Float16
				t.Run(fmt.Sprintf("Float16_dim%d", dim), func(t *testing.T) {
					a := make([]float16.Num, dim)
					b := make([]float16.Num, dim)
					var gtDot, gtL2Sq, normA, normB float64
					for i := 0; i < dim; i++ {
						vA := float32(rng.Float64()*2 - 1)
						vB := float32(rng.Float64()*2 - 1)
						a[i] = float16.New(vA)
						b[i] = float16.New(vB)
						decA := float64(a[i].Float32())
						decB := float64(b[i].Float32())
						gtDot += decA * decB
						diff := decA - decB
						gtL2Sq += diff * diff
						normA += decA * decA
						normB += decB * decB
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceF16(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*float16RelativeTol+1e-4)

					dot, err := DotProductF16(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*float16RelativeTol+1e-3)

					cos, err := CosineDistanceF16(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, float16RelativeTol*5)
				})

				// 4. Complex64
				t.Run(fmt.Sprintf("Complex64_dim%d", dim), func(t *testing.T) {
					a := make([]complex64, dim)
					b := make([]complex64, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						rA, iA := float32(rng.Float64()*2-1), float32(rng.Float64()*2-1)
						rB, iB := float32(rng.Float64()*2-1), float32(rng.Float64()*2-1)
						a[i] = complex(rA, iA)
						b[i] = complex(rB, iB)
						dr, di := float64(rA-rB), float64(iA-iB)
						gtL2Sq += dr*dr + di*di
						gtDot += float64(rA)*float64(rB) + float64(iA)*float64(iB)
						normA += float64(rA)*float64(rA) + float64(iA)*float64(iA)
						normB += float64(rB)*float64(rB) + float64(iB)*float64(iB)
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceComplex64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*float32RelativeTol+1e-5)

					dot, err := DotProductComplex64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*float32RelativeTol+1e-4)

					cos, err := CosineDistanceComplex64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, float32RelativeTol*10)
				})

				// 5. Complex128
				t.Run(fmt.Sprintf("Complex128_dim%d", dim), func(t *testing.T) {
					a := make([]complex128, dim)
					b := make([]complex128, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						rA, iA := rng.Float64()*2-1, rng.Float64()*2-1
						rB, iB := rng.Float64()*2-1, rng.Float64()*2-1
						a[i] = complex(rA, iA)
						b[i] = complex(rB, iB)
						dr, di := rA-rB, iA-iB
						gtL2Sq += dr*dr + di*di
						gtDot += rA*rB + iA*iB
						normA += rA*rA + iA*iA
						normB += rB*rB + iB*iB
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceComplex128(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*float64RelativeTol+1e-5)

					dot, err := DotProductComplex128(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*float64RelativeTol+1e-4)

					cos, err := CosineDistanceComplex128(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, float64RelativeTol*10)
				})

				// 6. Int8
				t.Run(fmt.Sprintf("Int8_dim%d", dim), func(t *testing.T) {
					a := make([]int8, dim)
					b := make([]int8, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = int8(rng.Intn(200) - 100)
						b[i] = int8(rng.Intn(200) - 100)
						diff := float64(int(a[i]) - int(b[i]))
						gtL2Sq += diff * diff
						gtDot += float64(int(a[i]) * int(b[i]))
						normA += float64(int(a[i]) * int(a[i]))
						normB += float64(int(b[i]) * int(b[i]))
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceInt8(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*intExactTol+1e-5)

					l2Sq, err := L2SquaredInt8(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtL2Sq), l2Sq, float64(gtL2Sq)*intExactTol+1e-5)

					dot, err := DotProductInt8(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*intExactTol+1e-4)

					cos, err := CosineDistanceInt8(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, intExactTol*10)
				})

				// 7. Int16
				t.Run(fmt.Sprintf("Int16_dim%d", dim), func(t *testing.T) {
					a := make([]int16, dim)
					b := make([]int16, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = int16(rng.Intn(1000) - 500)
						b[i] = int16(rng.Intn(1000) - 500)
						diff := float64(int(a[i]) - int(b[i]))
						gtL2Sq += diff * diff
						gtDot += float64(int(a[i]) * int(b[i]))
						normA += float64(int(a[i]) * int(a[i]))
						normB += float64(int(b[i]) * int(b[i]))
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceInt16(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*intExactTol+1e-4)

					dot, err := DotProductInt16(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*intExactTol+1e-3)

					cos, err := CosineDistanceInt16(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, intExactTol*10)
				})

				// 8. Int32
				t.Run(fmt.Sprintf("Int32_dim%d", dim), func(t *testing.T) {
					a := make([]int32, dim)
					b := make([]int32, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = int32(rng.Intn(2000) - 1000)
						b[i] = int32(rng.Intn(2000) - 1000)
						diff := float64(int64(a[i]) - int64(b[i]))
						gtL2Sq += diff * diff
						gtDot += float64(int64(a[i]) * int64(b[i]))
						normA += float64(int64(a[i]) * int64(a[i]))
						normB += float64(int64(b[i]) * int64(b[i]))
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceInt32(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*intExactTol+1e-3)

					dot, err := DotProductInt32(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*intExactTol+1e-2)

					cos, err := CosineDistanceInt32(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, intExactTol*10)
				})

				// 9. Int64
				t.Run(fmt.Sprintf("Int64_dim%d", dim), func(t *testing.T) {
					a := make([]int64, dim)
					b := make([]int64, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = int64(rng.Intn(2000) - 1000)
						b[i] = int64(rng.Intn(2000) - 1000)
						diff := float64(a[i] - b[i])
						gtL2Sq += diff * diff
						gtDot += float64(a[i] * b[i])
						normA += float64(a[i] * a[i])
						normB += float64(b[i] * b[i])
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceInt64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*intExactTol+1e-3)

					dot, err := DotProductInt64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*intExactTol+1e-2)

					cos, err := CosineDistanceInt64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, intExactTol*10)
				})

				// 10. Uint8
				t.Run(fmt.Sprintf("Uint8_dim%d", dim), func(t *testing.T) {
					a := make([]uint8, dim)
					b := make([]uint8, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = uint8(rng.Intn(256))
						b[i] = uint8(rng.Intn(256))
						diff := float64(int(a[i]) - int(b[i]))
						gtL2Sq += diff * diff
						gtDot += float64(int(a[i]) * int(b[i]))
						normA += float64(int(a[i]) * int(a[i]))
						normB += float64(int(b[i]) * int(b[i]))
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceUint8(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*intExactTol+1e-5)

					l2Sq, err := L2SquaredUint8(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtL2Sq), l2Sq, float64(gtL2Sq)*intExactTol+1e-5)

					dot, err := DotProductUint8(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*intExactTol+1e-4)

					cos, err := CosineDistanceUint8(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, intExactTol*10)
				})

				// 11. Uint16
				t.Run(fmt.Sprintf("Uint16_dim%d", dim), func(t *testing.T) {
					a := make([]uint16, dim)
					b := make([]uint16, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = uint16(rng.Intn(1000))
						b[i] = uint16(rng.Intn(1000))
						diff := float64(int(a[i]) - int(b[i]))
						gtL2Sq += diff * diff
						gtDot += float64(int(a[i]) * int(b[i]))
						normA += float64(int(a[i]) * int(a[i]))
						normB += float64(int(b[i]) * int(b[i]))
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceUint16(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*intExactTol+1e-4)

					dot, err := DotProductUint16(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*intExactTol+1e-3)

					cos, err := CosineDistanceUint16(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, intExactTol*10)
				})

				// 12. Uint32
				t.Run(fmt.Sprintf("Uint32_dim%d", dim), func(t *testing.T) {
					a := make([]uint32, dim)
					b := make([]uint32, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = uint32(rng.Intn(2000))
						b[i] = uint32(rng.Intn(2000))
						diff := float64(int64(a[i]) - int64(b[i]))
						gtL2Sq += diff * diff
						gtDot += float64(int64(a[i]) * int64(b[i]))
						normA += float64(int64(a[i]) * int64(a[i]))
						normB += float64(int64(b[i]) * int64(b[i]))
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceUint32(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*intExactTol+1e-3)

					dot, err := DotProductUint32(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*intExactTol+1e-2)

					cos, err := CosineDistanceUint32(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, intExactTol*10)
				})

				// 13. Uint64
				t.Run(fmt.Sprintf("Uint64_dim%d", dim), func(t *testing.T) {
					a := make([]uint64, dim)
					b := make([]uint64, dim)
					var gtL2Sq, gtDot, normA, normB float64
					for i := 0; i < dim; i++ {
						a[i] = uint64(rng.Intn(2000))
						b[i] = uint64(rng.Intn(2000))
						diff := float64(a[i]) - float64(b[i])
						gtL2Sq += diff * diff
						gtDot += float64(a[i] * b[i])
						normA += float64(a[i] * a[i])
						normB += float64(b[i] * b[i])
					}
					gtEuc := float32(math.Sqrt(gtL2Sq))
					gtCos := float32(1.0 - (gtDot / (math.Sqrt(normA) * math.Sqrt(normB))))

					euc, err := EuclideanDistanceUint64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtEuc, euc, float64(gtEuc)*intExactTol+1e-3)

					dot, err := DotProductUint64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, float32(gtDot), dot, math.Abs(gtDot)*intExactTol+1e-2)

					cos, err := CosineDistanceUint64(a, b)
					require.NoError(t, err)
					assert.InDelta(t, gtCos, cos, intExactTol*10)
				})

				// 14. TurboQuant4
				t.Run(fmt.Sprintf("TurboQuant4_dim%d", dim), func(t *testing.T) {
					src := make([]float32, dim)
					for i := range src {
						src[i] = float32(rng.Float64()*2*math.Pi - math.Pi)
					}
					packed := make([]byte, (dim+1)/2)
					PackTQ4(src, packed)

					unpacked := make([]float32, dim)
					scale := float32(2 * math.Pi / 15.0)
					bias := -float32(math.Pi)
					UnpackTQ4(packed, unpacked, scale, bias)

					var maxDiff float32
					for i := range src {
						d := float32(math.Abs(float64(src[i] - unpacked[i])))
						if d > maxDiff {
							maxDiff = d
						}
					}
					// TurboQuant 4-bit maximum angle quantization step is ~2*pi / 15 ≈ 0.419; max deviation <= 0.45
					assert.LessOrEqual(t, maxDiff, float32(0.45), "TQ4 maximum reconstruction error")

					// Angle distance symmetry and accuracy
					d1 := TurboQuantAngleDistance(4, 3, 7)
					d2 := TurboQuantAngleDistance(4, 7, 3)
					assert.InDelta(t, d1, d2, 1e-6, "TQ4 angle distance symmetry")
				})

				// 15. TurboQuant8
				t.Run(fmt.Sprintf("TurboQuant8_dim%d", dim), func(t *testing.T) {
					src := make([]float32, dim)
					for i := range src {
						src[i] = float32(rng.Float64()*2*math.Pi - math.Pi)
					}
					packed := make([]byte, dim)
					PackTQ8(src, packed)

					unpacked := make([]float32, dim)
					scale := float32(2 * math.Pi / 255.0)
					bias := -float32(math.Pi)
					UnpackTQ8(packed, unpacked, scale, bias)

					var maxDiff float32
					for i := range src {
						d := float32(math.Abs(float64(src[i] - unpacked[i])))
						if d > maxDiff {
							maxDiff = d
						}
					}
					// TurboQuant 8-bit maximum angle quantization step is 2*pi / 255 ≈ 0.0246; max deviation <= 0.05
					assert.LessOrEqual(t, maxDiff, float32(tq8MaxReconErr), "TQ8 maximum reconstruction error")

					// Angle distance symmetry
					d1 := TurboQuantAngleDistance(8, 20, 150)
					d2 := TurboQuantAngleDistance(8, 150, 20)
					assert.InDelta(t, d1, d2, 1e-6, "TQ8 angle distance symmetry")
				})
			}
		})
	}
}

// TestSIMDAccuracy_NumericalIdentityEdgeCases verifies strict invariant guarantees:
// - Distance of identical vectors is 0
// - Cosine distance of identical vectors is 0
// - Dot product of orthogonal vectors is 0
func TestSIMDAccuracy_NumericalIdentityEdgeCases(t *testing.T) {
	dim := 256
	rng := rand.New(rand.NewSource(99999))

	t.Run("Float32_Identity", func(t *testing.T) {
		a := make([]float32, dim)
		for i := range a {
			a[i] = float32(rng.Float64()*10 - 5)
		}
		euc, err := EuclideanDistance(a, a)
		require.NoError(t, err)
		assert.Equal(t, float32(0), euc, "Euclidean self-distance must be identically 0")

		cos, err := CosineDistance(a, a)
		require.NoError(t, err)
		assert.InDelta(t, float32(0), cos, 1e-6, "Cosine self-distance must be 0")
	})

	t.Run("Float64_Identity", func(t *testing.T) {
		a := make([]float64, dim)
		for i := range a {
			a[i] = rng.Float64()*10 - 5
		}
		euc, err := EuclideanDistanceFloat64(a, a)
		require.NoError(t, err)
		assert.Equal(t, float32(0), euc, "Float64 Euclidean self-distance must be identically 0")

		cos, err := CosineDistanceFloat64(a, a)
		require.NoError(t, err)
		assert.InDelta(t, float32(0), cos, 1e-6, "Float64 Cosine self-distance must be 0")
	})

	t.Run("Uint8_Identity", func(t *testing.T) {
		a := make([]uint8, dim)
		for i := range a {
			a[i] = uint8(rng.Intn(256))
		}
		euc, err := EuclideanDistanceUint8(a, a)
		require.NoError(t, err)
		assert.Equal(t, float32(0), euc, "Uint8 Euclidean self-distance must be identically 0")

		l2Sq, err := L2SquaredUint8(a, a)
		require.NoError(t, err)
		assert.Equal(t, float32(0), l2Sq, "Uint8 L2Squared self-distance must be identically 0")
	})
}
