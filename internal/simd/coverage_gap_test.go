package simd

import (
	"testing"
	"unsafe"
	"math"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

func TestUnpackTQFull(t *testing.T) {
	scale := float32(0.5)
	bias := float32(1.0)

	t.Run("TQ2", func(t *testing.T) {
		src := []byte{0x1B, 0xE4, 0x00, 0xFF}
		dst := make([]float32, 16)
		UnpackTQ2(src, dst, scale, bias)
		UnpackTQ2Generic(src, dst, scale, bias)
		UnpackTQ2Generic(src, dst[:3], scale, bias)
	})

	t.Run("TQ4", func(t *testing.T) {
		src := []byte{0x12, 0x34, 0x56, 0x78, 0x90}
		dst := make([]float32, 10)
		UnpackTQ4(src, dst, scale, bias)
		UnpackTQ4Generic(src, dst, scale, bias)
		UnpackTQ4Generic(src, dst[:3], scale, bias)
	})

	t.Run("TQ8", func(t *testing.T) {
		src := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
		dst := make([]float32, 9)
		UnpackTQ8(src, dst, scale, bias)
		UnpackTQ8Generic(src, dst, scale, bias)
		UnpackTQ8Generic(src, dst[:3], scale, bias)
	})
	
	l2SquaredTQCorrectionGeneric(make([]float32, 8), make([]float32, 8), make([]byte, 1), 0.5, 8)
}

func TestTurboQuantDistanceFull(t *testing.T) {
	query := make([]float32, 8)
	tqData := make([]byte, 4 + 7 + 1)
	radius := float32(1.0)
	rb := math.Float32bits(radius)
	tqData[0] = byte(rb); tqData[1] = byte(rb >> 8); tqData[2] = byte(rb >> 16); tqData[3] = byte(rb >> 24)
	
	TurboQuantDistanceNEON(query, tqData, 8, 8, 8)
	TurboQuantDistanceNEON(query, tqData, 8, 8, 4)
	TurboQuantDistanceNEON(query, tqData, 8, 8, 2)
	TurboQuantDistanceNEON(query, tqData, 8, 8, 1)
	
	TurboQuantDistanceGeneric(query, tqData, 8, 8, 8)
	TurboQuantDistanceAVX512(query, tqData, 8, 8, 8)
	TurboQuantDistanceAVX2(query, tqData, 8, 8, 8)
	GetTurboQuantDistanceFunc()
}

func TestBlockedImplementationsFull(t *testing.T) {
	dim := 2048
	t.Run("Float32", func(t *testing.T) {
		a := make([]float32, dim); b := make([]float32, dim)
		_, _ = DotProductFloat32Blocked(a, b)
		_, _ = L2Float32Blocked(a, b)
		_, _ = DotProductFloat32BlockedPrefetch(a, b)
		_, _ = EuclideanFloat32BlockedPrefetch(a, b)
		
		res := make([]float32, 1)
		_ = EuclideanDistanceTiledBatch(a, [][]float32{b}, res)
		_ = DotProductTiledBatch(a, [][]float32{b}, res)
		
		_, _ = euclidean1024Blocked(a[:1024], b[:1024])
		_, _ = euclidean3072Blocked(make([]float32, 3072), make([]float32, 3072))
		_, _ = euclideanBlocked(a, b)
	})

	t.Run("Int32", func(t *testing.T) {
		a := make([]int32, dim); b := make([]int32, dim)
		_, _ = DotProductInt32Blocked(a, b)
		_, _ = EuclideanInt32Blocked(a, b)
	})

	t.Run("Int16", func(t *testing.T) {
		a := make([]int16, dim); b := make([]int16, dim)
		_, _ = DotProductInt16Blocked(a, b)
		_, _ = EuclideanInt16Blocked(a, b)
	})

	t.Run("Int8", func(t *testing.T) {
		a := make([]int8, dim); b := make([]int8, dim)
		_, _ = DotProductInt8Blocked(a, b)
		_, _ = EuclideanInt8Blocked(a, b)
	})

	t.Run("Uint16", func(t *testing.T) {
		a := make([]uint16, dim); b := make([]uint16, dim)
		_, _ = DotProductUint16Blocked(a, b)
		_, _ = EuclideanUint16Blocked(a, b)
	})

	t.Run("Uint32", func(t *testing.T) {
		a := make([]uint32, dim); b := make([]uint32, dim)
		_, _ = DotProductUint32Blocked(a, b)
		_, _ = EuclideanUint32Blocked(a, b)
	})

	t.Run("Int64", func(t *testing.T) {
		a := make([]int64, dim); b := make([]int64, dim)
		_, _ = DotProductInt64Blocked(a, b)
		_, _ = EuclideanInt64Blocked(a, b)
	})
	
	t.Run("Uint64", func(t *testing.T) {
		a := make([]uint64, dim); b := make([]uint64, dim)
		_, _ = DotProductUint64Blocked(a, b)
		_, _ = EuclideanUint64Blocked(a, b)
	})
}

func TestNeonDirectCallsFull(t *testing.T) {
	a := make([]float32, 128); b := make([]float32, 128)
	_, _ = euclideanNEON(a, b)
	_, _ = dotNEON(a, b)
	_, _ = cosineNEON(a, b)
	_, _ = l2SquaredNEON(a, b)
	
	_, _ = l2Squared128NEON(a, b)
	_, _ = l2Squared384NEON(make([]float32, 384), make([]float32, 384))
	_, _ = l2Squared768NEON(make([]float32, 768), make([]float32, 768))
	_, _ = l2Squared1024NEON(make([]float32, 1024), make([]float32, 1024))
	_, _ = l2Squared3072NEON(make([]float32, 3072), make([]float32, 3072))
	
	_, _ = euclidean128NEON(a, b)
	_, _ = euclidean384NEON(make([]float32, 384), make([]float32, 384))
	_, _ = euclidean768NEON(make([]float32, 768), make([]float32, 768))
	_, _ = euclidean1024NEON(make([]float32, 1024), make([]float32, 1024))
	_, _ = euclidean1536NEON(make([]float32, 1536), make([]float32, 1536))
	_, _ = euclidean3072NEON(make([]float32, 3072), make([]float32, 3072))
	
	_, _ = dot128NEON(a, b)
	_, _ = dot384NEON(make([]float32, 384), make([]float32, 384))
	_, _ = dot768NEON(make([]float32, 768), make([]float32, 768))
	_, _ = dot1024NEON(make([]float32, 1024), make([]float32, 1024))
	_, _ = dot1536NEON(make([]float32, 1536), make([]float32, 1536))
	_, _ = dot3072NEON(make([]float32, 3072), make([]float32, 3072))
	
	af16 := make([]float16.Num, 10); bf16 := make([]float16.Num, 10)
	_, _ = euclideanF16NEON(af16, bf16)
	_, _ = dotF16NEON(af16, bf16)
	_, _ = cosineF16NEON(af16, bf16)
	
	ab := make([]byte, 16); bb := make([]byte, 16)
	_, _ = dotInt4Neon(ab, bb)
	_, _ = dotInt2Neon(ab, bb)
	
	_ = matchInt64Neon(make([]int64, 1), 0, 0, make([]byte, 1))
	_ = matchInt32Neon(make([]int32, 1), 0, 0, make([]byte, 1))
	_ = matchFloat32Neon(make([]float32, 1), 0, 0, make([]byte, 1))
	_ = matchFloat64Neon(make([]float64, 1), 0, 0, make([]byte, 1))
	
	_ = euclideanBatchNEON(a, [][]float32{b}, make([]float32, 1))
	_ = dotBatchNEON(a, [][]float32{b}, make([]float32, 1))
	_ = cosineBatchNEON(a, [][]float32{b}, make([]float32, 1))
	
	_ = FastWalshHadamardTransform32NEON(make([]float32, 32))
	_ = RandomRotationNEON(a, 123)
	
	int8ToFloat32NEON(make([]int8, 8), make([]float32, 8))
	uint8ToFloat32NEON(make([]uint8, 8), make([]float32, 8))
	int16ToFloat32NEON(make([]int16, 8), make([]float32, 8))
	uint16ToFloat32NEON(make([]uint16, 8), make([]float32, 8))
	int32ToFloat32NEON(make([]int32, 8), make([]float32, 8))
	uint32ToFloat32NEON(make([]uint32, 8), make([]float32, 8))
	float16ToFloat32NEON(make([]float16.Num, 8), make([]float32, 8))
	
	sigmoidNEON(a[:1], b[:1])
	expNEON(a[:1], b[:1])
	logNEON(a[:1], b[:1])
	softmaxNEON(a[:1], b[:1])
	
	memcpyNEON(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), 4)
	
	_, _ = euclideanFloat64NEON(make([]float64, 8), make([]float64, 8))
	_, _ = dotFloat64NEON(make([]float64, 8), make([]float64, 8))
	
	_ = sumNEON(a)
	_ = maxNEON(a)
	_ = minNEON(a)
	_ = argMaxNEON(a)
	_ = argMinNEON(a)
	matMulNEON(a[:1], b[:1], 1, 1, 1, make([]float32, 1))
	
	_, _ = manhattanNEON(a[:1], b[:1])
	_, _ = chebyshevNEON(a[:1], b[:1])
	_, _ = brayCurtisNEON(a[:1], b[:1])
	
	accumulateWeightedScatterNEON(make([]float32, 10), []uint32{1}, []float32{1.0}, 1.0)
}

func TestDistanceFunctionsFull(t *testing.T) {
	dims := []int{128, 384, 768, 1024, 1536, 3072, 4000}
	for _, d := range dims {
		a := make([]float32, d); b := make([]float32, d)
		_, _ = EuclideanDistance(a, b)
		_, _ = DotProduct(a, b)
		_, _ = CosineDistance(a, b)
		_, _ = L2Squared(a, b)
		
		a64 := make([]float64, d); b64 := make([]float64, d)
		_, _ = EuclideanDistanceFloat64(a64, b64)
		_, _ = DotProductF64(a64, b64)
		_, _ = CosineDistanceFloat64(a64, b64)
		
		ai8 := make([]int8, d); bi8 := make([]int8, d)
		_, _ = EuclideanDistanceInt8(ai8, bi8)
		_, _ = DotProductInt8(ai8, bi8)
		_, _ = CosineDistanceInt8(ai8, bi8)
		
		au8 := make([]uint8, d); bu8 := make([]uint8, d)
		_, _ = EuclideanDistanceUint8(au8, bu8)
		_, _ = DotProductUint8(au8, bu8)
		_, _ = CosineDistanceUint8(au8, bu8)
		
		ai16 := make([]int16, d); bi16 := make([]int16, d)
		_, _ = EuclideanDistanceInt16(ai16, bi16)
		_, _ = DotProductInt16(ai16, bi16)
		_, _ = CosineDistanceInt16(ai16, bi16)
		
		ai32 := make([]int32, d); bi32 := make([]int32, d)
		_, _ = EuclideanDistanceInt32(ai32, bi32)
		_, _ = DotProductInt32(ai32, bi32)
		_, _ = CosineDistanceInt32(ai32, bi32)
		
		af16 := make([]float16.Num, d); bf16 := make([]float16.Num, d)
		_, _ = EuclideanDistanceF16(af16, bf16)
		_, _ = DotProductF16(af16, bf16)
		_, _ = CosineDistanceF16(af16, bf16)
		
		au64 := make([]uint64, d); bu64 := make([]uint64, d)
		_, _ = EuclideanDistanceUint64(au64, bu64)
		_, _ = DotProductUint64(au64, bu64)
		_, _ = CosineDistanceUint64(au64, bu64)
	}
	
	// Error paths
	_, _ = EuclideanDistance(make([]float32, 1), make([]float32, 2))
	_, _ = DotProduct(make([]float32, 1), make([]float32, 2))
	_, _ = CosineDistance(make([]float32, 1), make([]float32, 2))
	_, _ = L2Squared(make([]float32, 1), make([]float32, 2))
	
	c64 := []complex64{1, 2}; d64 := []complex64{3, 4}
	_, _ = EuclideanDistanceComplex64(c64, d64)
	_, _ = DotProductComplex64(c64, d64)
	_, _ = CosineDistanceComplex64(c64, d64)
	_, _ = EuclideanDistanceComplex64(c64, nil)
	
	c128 := []complex128{1, 2}; d128 := []complex128{3, 4}
	_, _ = EuclideanDistanceComplex128(c128, d128)
	_, _ = DotProductComplex128(c128, d128)
	_, _ = CosineDistanceComplex128(c128, d128)
	_, _ = EuclideanDistanceComplex128(c128, nil)
}

func TestHighDimGenericFull(t *testing.T) {
	a64 := make([]float64, 768); b64 := make([]float64, 768)
	l2Squared384Float64(a64[:384], b64[:384])
	l2Squared768Float64(a64, b64)
	dot384Float64(a64[:384], b64[:384])
	dot768Float64(a64, b64)
	Dot384Float64(a64[:384], b64[:384])
	Dot768Float64(a64, b64)
	Euclidean384Float64(a64[:384], b64[:384])
	Euclidean768Float64(a64, b64)

	ai8 := make([]int8, 768); bi8 := make([]int8, 768)
	l2Squared384Int8(ai8[:384], bi8[:384])
	l2Squared768Int8(ai8, bi8)
	dot384Int8(ai8[:384], bi8[:384])
	dot768Int8(ai8, bi8)
	Dot384Int8(ai8[:384], bi8[:384])
	Dot768Int8(ai8, bi8)
	Euclidean384Int8(ai8[:384], bi8[:384])
	Euclidean768Int8(ai8, bi8)
	
	af16 := make([]float16.Num, 768); bf16 := make([]float16.Num, 768)
	l2Squared384Float16(af16[:384], bf16[:384])
	l2Squared768Float16(af16, bf16)
	dot384Float16(af16[:384], bf16[:384])
	dot768Float16(af16, bf16)
	Dot384Float16(af16[:384], bf16[:384])
	Dot768Float16(af16, bf16)
	Euclidean384Float16(af16[:384], bf16[:384])
	Euclidean768Float16(af16, bf16)
}

func TestV23CompatibilityFull(t *testing.T) {
	scl := NewCompatibilityLayer()
	scl.EnableV23Optimizations()
	_ = scl.V23MemoryLayoutOptimizations()
	_ = scl.V23InstructionSetEnhancements()
	_ = scl.V23ZeroCopyOptimizations()
	_ = scl.V23VectorizedOperations()
	_ = scl.EnhancedBatchOperations()
	
	scl.IsV23Enabled()
	scl.GetOptimizationLevel()
	scl.UpdatePerformanceMetrics(1.0, 1.0)
	scl.GetPerformanceMetrics()
	scl.PrepareForV23()
	scl.GetV23MigrationPath()
	scl.ValidateV23Readiness()
	
	fc := NewFloat16Compatibility(float16.New(0))
	fc.GetBase()
	fc.V23EnhancedConversion()
}

func TestRegistryFull(t *testing.T) {
	GetKernel[float32](MetricEuclidean, 0)
	GetKernel[float32](MetricEuclidean, 128)
	GetKernel[float32](MetricCosine, 128)
	GetKernel[float32](MetricDotProduct, 128)
	GetKernel[float32](MetricL2Squared, 128)
	
	GetKernel[float16.Num](MetricEuclidean, 0)
	GetKernel[float64](MetricEuclidean, 0)
	GetKernel[complex64](MetricEuclidean, 0)
	GetKernel[complex128](MetricEuclidean, 0)
	GetKernel[int8](MetricEuclidean, 0)
	GetKernel[uint8](MetricEuclidean, 0)
	GetKernel[int16](MetricEuclidean, 0)
	GetKernel[uint16](MetricEuclidean, 0)
	GetKernel[int32](MetricEuclidean, 0)
	GetKernel[uint32](MetricEuclidean, 0)
	GetKernel[int64](MetricEuclidean, 0)
	GetKernel[uint64](MetricEuclidean, 0)
	
	_ = MetricEuclidean.String()
	_ = MetricCosine.String()
	_ = MetricDotProduct.String()
	_ = MetricL2Squared.String()
	_ = MetricManhattan.String()
	_ = MetricChebyshev.String()
	_ = MetricBrayCurtis.String()
	_ = MetricType(255).String()
}

func TestPrefetchFull(t *testing.T) {
	a := make([]float32, 128)
	Prefetch(unsafe.Pointer(&a[0]))
	prefetchGeneric(unsafe.Pointer(&a[0]))
	prefetchNTA(0)
}

func TestJitRuntimeFull(t *testing.T) {
	if jitRT != nil {
		q := make([]float32, 128)
		vs := [][]float32{make([]float32, 128)}
		res := make([]float32, 1)
		_ = jitRT.EuclideanBatchInto(q, vs, res)
		_ = jitRT.EuclideanBatchInto(nil, nil, nil)
	}
}

func TestStubsX86DirectCallsFull(t *testing.T) {
	a := make([]float32, 128); b := make([]float32, 128)
	_, _ = euclideanAVX2(a, b)
	_, _ = euclideanAVX512(a, b)
	_, _ = cosineAVX2(a, b)
	_, _ = cosineAVX512(a, b)
	_, _ = dotAVX2(a, b)
	_, _ = dotAVX512(a, b)
	
	_, _ = euclidean128AVX2(a, b)
	_, _ = euclidean384AVX2(a, b)
	_, _ = euclidean768AVX2(a, b)
	_, _ = euclidean1024AVX2(a, b)
	_, _ = euclidean1536AVX2(a, b)
	_, _ = euclidean3072AVX2(a, b)
	
	_, _ = euclidean128AVX512(a, b)
	_, _ = euclidean384AVX512(a, b)
	_, _ = euclidean768AVX512(a, b)
	_, _ = euclidean1024AVX512(a, b)
	_, _ = euclidean3072AVX512(a, b)
	
	_, _ = dot128AVX2(a, b)
	_, _ = dot384AVX2(a, b)
	_, _ = dot768AVX2(a, b)
	_, _ = dot1024AVX2(a, b)
	_, _ = dot1536AVX2(a, b)
	_, _ = dot3072AVX2(a, b)
	
	_, _ = dot128AVX512(a, b)
	_, _ = dot384AVX512(a, b)
	_, _ = dot768AVX512(a, b)
	_, _ = dot1024AVX512(a, b)
	_, _ = dot1536AVX512(a, b)
	_, _ = dot3072AVX512(a, b)
	
	_, _ = l2Squared128AVX2(a, b)
	_, _ = l2Squared384AVX2(a, b)
	_, _ = l2Squared768AVX2(a, b)
	_, _ = l2Squared1024AVX2(a, b)
	_, _ = l2Squared3072AVX2(a, b)
	
	_, _ = l2Squared128AVX512(a, b)
	_, _ = l2Squared384AVX512(a, b)
	_, _ = l2Squared768AVX512(a, b)
	_, _ = l2Squared1024AVX512(a, b)
	_, _ = l2Squared3072AVX512(a, b)
	
	_ = euclideanBatchAVX2(a, [][]float32{b}, make([]float32, 1))
	_ = cosineBatchAVX2(a, [][]float32{b}, make([]float32, 1))
	_ = dotBatchAVX2(a, [][]float32{b}, make([]float32, 1))
	
	_, _ = l2SquaredAVX2(a, b)
	_, _ = l2SquaredAVX512(a, b)
	
	_ = matchInt64AVX2(make([]int64, 1), 0, 0, make([]byte, 1))
	_ = matchInt32AVX2(make([]int32, 1), 0, 0, make([]byte, 1))
	_ = matchFloat32AVX2(make([]float32, 1), 0, 0, make([]byte, 1))
	_ = matchFloat64AVX2(make([]float64, 1), 0, 0, make([]byte, 1))
	
	_ = adcBatchAVX2(nil, nil, 0, nil)
	_ = adcBatchAVX512(nil, nil, 0, nil)
	_ = adcBatchVNNI(nil, nil, 0, nil)
	
	int8ToFloat32AVX2(make([]int8, 8), make([]float32, 8))
	uint8ToFloat32AVX2(make([]uint8, 8), make([]float32, 8))
	int16ToFloat32AVX2(make([]int16, 8), make([]float32, 8))
	uint16ToFloat32AVX2(make([]uint16, 8), make([]float32, 8))
	int32ToFloat32AVX2(make([]int32, 8), make([]float32, 8))
	uint32ToFloat32AVX2(make([]uint32, 8), make([]float32, 8))
	float16ToFloat32AVX2(make([]float16.Num, 8), make([]float32, 8))
	
	sigmoidAVX2(nil, nil)
	softmaxAVX2(nil, nil)
	expAVX2(nil, nil)
	logAVX2(nil, nil)
	
	sumAVX2(nil)
	maxAVX2(nil)
	minAVX2(nil)
	
	matMulAVX2(nil, nil, 0, 0, 0, nil)
	argMaxAVX2(nil)
	argMinAVX2(nil)
	
	sinAVX2(nil, nil)
	cosAVX2(nil, nil)
	atan2AVX2(nil, nil, nil)
	
	haversineBatchAVX2(0, 0, nil, 0, nil)
	
	_, _ = dotFloat64AVX2(make([]float64, 8), make([]float64, 8))
	_, _ = euclideanFloat64AVX2(make([]float64, 8), make([]float64, 8))
	_, _ = dotInt4AVX2(make([]byte, 8), make([]byte, 8))
	_, _ = dotInt2AVX2(make([]byte, 8), make([]byte, 8))
	
	andBytesAVX2(make([]byte, 8), make([]byte, 8))
	orBytesAVX2(make([]byte, 8), make([]byte, 8))
	isAllZerosAVX2(make([]byte, 8))
	
	_ = euclideanSQ8BatchAVX512(nil, nil, nil)
	_ = euclideanF16BatchAVX512(nil, nil, nil)
	
	_, _ = euclideanFloat64AVX512(make([]float64, 8), make([]float64, 8))
	_, _ = dotFloat64AVX512(make([]float64, 8), make([]float64, 8))
	_, _ = euclideanInt8AVX512(make([]int8, 8), make([]int8, 8))
	
	_ = euclideanBatchAVX512(nil, nil, nil)
	_ = dotBatchAVX512(nil, nil, nil)
	_ = cosineBatchAVX512(nil, nil, nil)
	
	_ = euclideanVerticalBatchAVX2(nil, nil, nil)
	_ = euclideanVerticalBatchAVX512(nil, nil, nil)
	
	_, _ = euclideanInt8AVX2(nil, nil)
	_, _ = euclideanInt16AVX2(nil, nil)
	_, _ = euclideanUint16AVX2(nil, nil)
	_, _ = dotInt16AVX2(nil, nil)
	_, _ = dotUint16AVX2(nil, nil)
	
	_ = euclideanSQ8BatchAVX2(nil, nil, nil)
	_ = euclideanF16BatchAVX2(nil, nil, nil)
	
	_, _ = euclidean16AVX512Wrapper(nil, nil)
	_, _ = cosine16AVX512Wrapper(nil, nil)
	
	_, _ = dotInt4AVX512(nil, nil)
	_, _ = dotInt2AVX512(nil, nil)
	
	matMulAVX2Go(nil, nil, 0, 0, 0, nil)
	_, _ = ManhattanDistanceFloat32AVX2(nil, nil)
	_, _ = ChebyshevDistanceFloat32AVX2(nil, nil)
	_, _ = BrayCurtisDistanceFloat32AVX2(nil, nil)
}

func TestHadamardFull(t *testing.T) {
	a := []float32{1, 2, 3, 4}
	_ = FastWalshHadamardTransform32(a)
	_ = RandomRotation(a, 123)
	_ = PadToPowerOf2(a)
	_ = PadToPowerOf2(make([]float32, 3))
}

func TestSqFull(t *testing.T) {
	a := []byte{0x12}; b := []byte{0x34}
	_, _ = dotInt4Generic(a, b)
	_, _ = dotInt2Generic(a, b)
	
	EuclideanDistanceSQ8([]byte{1}, []byte{2})
	QuantizeSQ8([]float32{1.0}, make([]byte, 1), 0, 255)
}

func TestSimdExtra(t *testing.T) {
	Int8ToFloat32(nil, nil)
	Uint8ToFloat32(nil, nil)
	Int16ToFloat32(nil, nil)
	Uint16ToFloat32(nil, nil)
	Int32ToFloat32(nil, nil)
	Uint32ToFloat32(nil, nil)
	Float16ToFloat32(nil, nil)
	
	SinFloat32(nil, nil)
	CosFloat32(nil, nil)
	Atan2Float32(nil, nil, nil)
	
	Pause()
	PauseN(1)
	
	euclideanBatchFlatAVX2(nil, nil, 0, 0, nil)
	euclideanBatchFlatAVX512(nil, nil, 0, 0, nil)
	
	FindNearestCentroidInCodebook(make([]float32, 4), [][]float32{make([]float32, 4)}, 1, 1, 4)
	FindNearestCentroid(make([]float32, 4), make([]float32, 4), 4, 1)
	
	l2SquaredInt8Unrolled4x(make([]int8, 4), make([]int8, 4))
	l2SquaredUint8Unrolled4x(make([]uint8, 4), make([]uint8, 4))
	
	ManhattanDistanceF16(make([]float16.Num, 4), make([]float16.Num, 4))
	ChebyshevDistanceF16(make([]float16.Num, 4), make([]float16.Num, 4))
	BrayCurtisDistanceF16(make([]float16.Num, 4), make([]float16.Num, 4))
	
	notBytesGeneric([]byte{0})
	andBytesGeneric([]byte{0}, []byte{0})
	orBytesGeneric([]byte{0}, []byte{0})
	
	AccumulateWeightedScatterFloat32(make([]float32, 10), []uint32{0}, []float32{1.0}, 1.0)
	EuclideanDistanceSQ8Batch(make([]byte, 4), [][]byte{make([]byte, 4)}, make([]float32, 1))
	EuclideanDistanceF16Batch(make([]float16.Num, 4), [][]float16.Num{make([]float16.Num, 4)}, make([]float32, 1))
}
