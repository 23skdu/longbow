package simd

import (
	"testing"
	"unsafe"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func TestExtraCoverage(t *testing.T) {
	initializeDispatch()

	t.Run("Context", func(t *testing.T) {
		ctx := &Context{}
		ctx.RecordCall()
		gctx := &GlobalSimdContext{}
		gctx.RecordCall()
	})

	t.Run("V23Compatibility", func(t *testing.T) {
		scl := NewCompatibilityLayer()
		scl.EnableV23Optimizations()
		_ = scl.V23MemoryLayoutOptimizations()
		_ = scl.V23InstructionSetEnhancements()
		_ = scl.V23ZeroCopyOptimizations()
		_ = scl.V23VectorizedOperations()
		_ = scl.EnhancedBatchOperations()
		_ = scl.PrepareForV23()
		_ = scl.GetV23MigrationPath()
		scl.ValidateV23Readiness()
		
		fc := NewFloat16Compatibility(float16.New(1.0))
		fc.GetBase()
		fc.V23EnhancedConversion()
		
		scl.GetPerformanceMetrics()
		scl.UpdatePerformanceMetrics(1.0, 2.0)
	})

	t.Run("BitopsPublic", func(t *testing.T) {
		a := []uint64{1}; b := []uint64{2}
		HammingDistance(a, b)
		AndBitVectors(a, b)
		CountBitVector(a)
		Popcount(123)
	})

	t.Run("BitopsGeneric", func(t *testing.T) {
		a := []uint64{1}; b := []uint64{2}
		HammingDistanceGeneric(a, b)
		AndBitVectorsGeneric(a, b)
		CountBitVectorGeneric(a)
		notBytesGeneric([]byte{0, 1})
	})

	t.Run("SparseScore", func(t *testing.T) {
		BM25ScoreBatch(nil, nil, 0, 0, 0, 0)
		bm25ScoreBatchGeneric(nil, nil, 0, 0, 0, 0)
		if implementation == "neon" {
			bm25ScoreBatchArch(nil, nil, 0, 0, 0, 0)
		}
	})

	t.Run("FMA", func(t *testing.T) {
		a, b := []float32{1}, []float32{2}
		_, _ = DotProductFMA(a, b)
		_, _ = EuclideanDistanceFMA(a, b)
		_, _ = CosineDistanceFMA(a, b)
	})

	t.Run("BlockedProcessing", func(t *testing.T) {
		a := make([]float32, 2048); b := make([]float32, 2048)
		_, _ = DotProductFloat32Blocked(a, b)
		_, _ = L2Float32Blocked(a, b)
		_ = EuclideanDistanceTiledBatch(a, [][]float32{b}, make([]float32, 1))
		_ = DotProductTiledBatch(a, [][]float32{b}, make([]float32, 1))
		_, _ = DotProductFloat32BlockedPrefetch(a, b)
		_, _ = EuclideanFloat32BlockedPrefetch(a, b)
		
		af64 := make([]float64, 2048); bf64 := make([]float64, 2048)
		_, _ = DotProductFloat64Blocked(af64, bf64)
		_, _ = EuclideanFloat64Blocked(af64, bf64)
		
		ai32 := make([]int32, 2048); bi32 := make([]int32, 2048)
		_, _ = DotProductInt32Blocked(ai32, bi32)
		_, _ = EuclideanInt32Blocked(ai32, bi32)

		ai8 := make([]int8, 2048); bi8 := make([]int8, 2048)
		_, _ = DotProductInt8Blocked(ai8, bi8)
		_, _ = EuclideanInt8Blocked(ai8, bi8)
	})

	t.Run("Memcpy", func(t *testing.T) {
		src := []byte{1, 2, 3, 4}
		dst := make([]byte, 4)
		memcpyGeneric(unsafe.Pointer(&dst[0]), unsafe.Pointer(&src[0]), 4)
		MemcpyNTA(unsafe.Pointer(&dst[0]), unsafe.Pointer(&src[0]), 4)
		if implementation == "neon" {
			memcpyNEON(unsafe.Pointer(&dst[0]), unsafe.Pointer(&src[0]), 4)
		}
	})

	t.Run("Scatter", func(t *testing.T) {
		dst := make([]float32, 10)
		targets := []uint32{1}
		weights := []float32{1.0}
		AccumulateWeightedScatter(dst, targets, weights, 1.0)
		accumulateWeightedScatterGeneric(dst, targets, weights, 1.0)
		if implementation == "neon" {
			accumulateWeightedScatterNEON(dst, targets, weights, 1.0)
		}
		AccumulateWeightedScatterFloat32(dst, targets, weights, 1.0)
	})

	t.Run("MatchNeon", func(t *testing.T) {
		if implementation == "neon" {
			matchInt64Neon(nil, 0, 0, nil)
			matchInt32Neon(nil, 0, 0, nil)
			matchFloat32Neon(nil, 0, 0, nil)
			matchFloat64Neon(nil, 0, 0, nil)
		}
	})

	t.Run("Jit", func(t *testing.T) {
		if jitRT != nil {
			_ = jitRT.EuclideanBatchInto(nil, nil, nil)
		}
	})
	t.Run("BatchBaselines", func(t *testing.T) {
		q := []float16.Num{float16.New(1.0)}
		vs := [][]float16.Num{{float16.New(2.0)}, nil}
		res := make([]float32, 2)
		_ = euclideanF16BatchGeneric(q, vs, res)
	})

	t.Run("BatchFlatSpecialDims", func(t *testing.T) {
		q128 := make([]float32, 128); f128 := make([]float32, 256)
		_ = EuclideanDistanceBatchFlat(q128, f128, 2, 128, make([]float32, 2))
		
		q384 := make([]float32, 384); f384 := make([]float32, 768)
		_ = EuclideanDistanceBatchFlat(q384, f384, 2, 384, make([]float32, 2))
	})
	
	t.Run("BatchSpecialDims", func(t *testing.T) {
		q128 := make([]float32, 128); vs128 := [][]float32{make([]float32, 128), nil}
		_ = EuclideanDistanceBatch(q128, vs128, make([]float32, 2))
		
		q384 := make([]float32, 384); vs384 := [][]float32{make([]float32, 384), nil}
		_ = EuclideanDistanceBatch(q384, vs384, make([]float32, 2))
	})

	t.Run("VerticalBatch", func(t *testing.T) {
		q := make([]float32, 128); vs := [][]float32{make([]float32, 128)}
		_ = EuclideanDistanceVerticalBatch(q, vs, make([]float32, 1))
		
		q2 := make([]float32, 100); vs2 := [][]float32{make([]float32, 100)}
		_ = EuclideanDistanceVerticalBatch(q2, vs2, make([]float32, 1))
	})

	t.Run("OptimizedComplex", func(t *testing.T) {
		a := []complex64{1}; b := []complex64{2}
		_, _ = euclideanComplex64Optimized(a, b)
	})
}
