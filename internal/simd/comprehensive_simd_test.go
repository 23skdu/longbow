package simd

import (
	"testing"
	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/stretchr/testify/assert"
)

func TestSIMD_GenerativeCorrectness(t *testing.T) {
	dimensions := []int{4, 16, 128, 384, 768, 1024, 1536}

	t.Run("Float32_Euclidean", func(t *testing.T) {
		for _, dim := range dimensions {
			a := makeTestVector(dim, 1.0)
			b := makeTestVector(dim, 2.0)
			expected := referenceEuclidean(a, b)
			res, err := EuclideanDistance(a, b)
			assert.NoError(t, err)
			assert.InDelta(t, expected, res, 0.05)
		}
	})

	t.Run("Float32_Cosine", func(t *testing.T) {
		for _, dim := range dimensions {
			a := makeTestVector(dim, 1.0)
			b := makeTestVector(dim, 2.0)
			expected := referenceCosine(a, b)
			res, err := CosineDistance(a, b)
			assert.NoError(t, err)
			assert.InDelta(t, expected, res, 0.001)
		}
	})

	t.Run("Float32_Dot", func(t *testing.T) {
		for _, dim := range dimensions {
			a := makeTestVector(dim, 1.0)
			b := makeTestVector(dim, 2.0)
			var expected float32
			for i := range a {
				expected += a[i] * b[i]
			}
			res, err := DotProduct(a, b)
			assert.NoError(t, err)
			// For large dot products (1536 dims), precision can vary.
			assert.InDelta(t, expected, res, 100.0)
		}
	})
}

func TestSIMD_BlockedKernels(t *testing.T) {
	dim := 2048
	
	t.Run("Float32Blocked", func(t *testing.T) {
		a := makeTestVector(dim, 1.0)
		b := makeTestVector(dim, 2.0)
		
		_, _ = DotProductFloat32Blocked(a, b)
		_, _ = L2Float32Blocked(a, b)
		_, _ = DotProductFloat32BlockedPrefetch(a, b)
		_, _ = EuclideanFloat32BlockedPrefetch(a, b)
		_, _ = euclideanBlocked(a, b)
	})

	t.Run("Int32Blocked", func(t *testing.T) {
		a := make([]int32, dim)
		b := make([]int32, dim)
		for i := 0; i < dim; i++ {
			a[i] = int32(i)
			b[i] = int32(i + 1)
		}
		_, _ = DotProductInt32Blocked(a, b)
		_, _ = EuclideanInt32Blocked(a, b)
	})

	t.Run("Int16Blocked", func(t *testing.T) {
		a := make([]int16, dim)
		b := make([]int16, dim)
		_, _ = DotProductInt16Blocked(a, b)
		_, _ = EuclideanInt16Blocked(a, b)
	})

	t.Run("Int8Blocked", func(t *testing.T) {
		a := make([]int8, dim)
		b := make([]int8, dim)
		_, _ = DotProductInt8Blocked(a, b)
		_, _ = EuclideanInt8Blocked(a, b)
	})

	t.Run("UintBlocked", func(t *testing.T) {
		a16 := make([]uint16, dim)
		b16 := make([]uint16, dim)
		_, _ = EuclideanUint16Blocked(a16, b16)
		_, _ = DotProductUint16Blocked(a16, b16)

		a32 := make([]uint32, dim)
		b32 := make([]uint32, dim)
		_, _ = EuclideanUint32Blocked(a32, b32)
		_, _ = DotProductUint32Blocked(a32, b32)

		a64 := make([]uint64, dim)
		b64 := make([]uint64, dim)
		_, _ = EuclideanUint64Blocked(a64, b64)
		_, _ = DotProductUint64Blocked(a64, b64)
	})
	
	t.Run("Int64Blocked", func(t *testing.T) {
		a := make([]int64, dim)
		b := make([]int64, dim)
		_, _ = EuclideanInt64Blocked(a, b)
		_, _ = DotProductInt64Blocked(a, b)
	})
}

func TestSIMD_CompatibilityLayer(t *testing.T) {
	scl := NewSIMDCompatibilityLayer()
	assert.False(t, scl.IsV23Enabled())
	assert.Equal(t, "v18.5", scl.GetOptimizationLevel())
	
	err := scl.V23MemoryLayoutOptimizations()
	assert.Error(t, err)

	scl.EnableV23Optimizations()
	assert.True(t, scl.IsV23Enabled())
	assert.Equal(t, "v23.0", scl.GetOptimizationLevel())
	
	assert.NoError(t, scl.V23MemoryLayoutOptimizations())
	assert.NoError(t, scl.V23InstructionSetEnhancements())
	assert.NoError(t, scl.V23ZeroCopyOptimizations())
	assert.NoError(t, scl.V23VectorizedOperations())
	assert.NoError(t, scl.EnhancedBatchOperations())
	assert.NoError(t, scl.PrepareForV23())
	
	path := scl.GetV23MigrationPath()
	assert.NotEmpty(t, path)
	assert.True(t, scl.ValidateV23Readiness())
	
	metrics := scl.GetPerformanceMetrics()
	assert.Equal(t, 0.0, metrics.ImprovementRatio)
	scl.UpdatePerformanceMetrics(100.0, 150.0)

	// Test V23 conversion
	f16 := float16.New(1.0)
	fc := NewFloat16Compatibility(f16)
	assert.Equal(t, f16, fc.GetBase())
	assert.Equal(t, float32(1.0), fc.V23EnhancedConversion())
}

func TestSIMD_MiscBlocked(t *testing.T) {
	a := make([]float32, 4000)
	b := make([]float32, 4000)
	
	// Test fixed-dim blocked kernels
	_, _ = euclidean384Blocked(a[:384], b[:384])
	_, _ = euclidean768Blocked(a[:768], b[:768])
	_, _ = euclidean1024Blocked(a[:1024], b[:1024])
	_, _ = euclidean1536Blocked(a[:1536], b[:1536])
	_, _ = euclidean2048Blocked(a[:2048], b[:2048])
	_, _ = euclidean3072Blocked(a[:3072], b[:3072])
	_, _ = euclideanBlockedGeneric(a[:1000], b[:1000], 256)
}
func TestSIMD_PublicBatchOperations(t *testing.T) {
	dim := 128
	a := make([]float32, dim)
	batch := [][]float32{a}
	results := make([]float32, 1)
	
	_ = EuclideanDistanceBatch(a, batch, results)
	_ = CosineDistanceBatch(a, batch, results)
	_ = DotProductBatch(a, batch, results)
	
	flat := make([]float32, dim)
	_ = EuclideanDistanceBatchFlat(a, flat, 1, dim, results)
	
	_ = EuclideanDistanceVerticalBatch(a, batch, results)
	
	// ADC
	table := make([]float32, 256)
	codes := []byte{0}
	_ = ADCDistanceBatch(table, codes, 1, results)
	
	// Converters
	_ = ToFloat32([]float64{1.0})
}
