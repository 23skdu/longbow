package simd

import (
	"github.com/23skdu/longbow/internal/simd/amx"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// MetricDispatch holds function pointers for vector distance and similarity metrics.
type MetricDispatch struct {
	// Core distance functions
	EuclideanDistance distanceFunc
	CosineDistance    distanceFunc
	DotProduct        distanceFunc

	// F16 distance functions
	EuclideanDistanceF16 distanceF16Func
	CosineDistanceF16    distanceF16Func
	DotProductF16        distanceF16Func

	// Specialized functions for fixed dimensions
	EuclideanDistance128  distanceFunc
	EuclideanDistance384  distanceFunc
	EuclideanDistance768  distanceFunc
	EuclideanDistance1024 distanceFunc
	EuclideanDistance1536 distanceFunc
	EuclideanDistance3072 distanceFunc

	DotProduct128  distanceFunc
	DotProduct384  distanceFunc
	DotProduct768  distanceFunc
	DotProduct1024 distanceFunc
	DotProduct1536 distanceFunc
	DotProduct3072 distanceFunc

	// L2Squared specialized (no sqrt) for fixed dimensions
	L2SquaredDistance128  distanceFunc
	L2SquaredDistance384  distanceFunc
	L2SquaredDistance768  distanceFunc
	L2SquaredDistance1024 distanceFunc
	L2SquaredDistance3072 distanceFunc

	// More distances
	ManhattanDistance  distanceFunc
	ChebyshevDistance  distanceFunc
	BrayCurtisDistance distanceFunc
	L2SquaredDistance  distanceFunc
}

// BatchDispatch holds batch and high-throughput vector processing routines.
type BatchDispatch struct {
	EuclideanDistanceBatch     distanceBatchFunc
	CosineDistanceBatch        distanceBatchFunc
	DotProductBatch            distanceBatchFunc
	EuclideanDistanceBatchFlat distanceBatchFlatFunc
	L2SquaredDistanceBatch     distanceBatchFunc
	TurboQuantDistanceBatch    distanceTQBatchFunc

	// Batch processing (GraphRAG expansion)
	AccumulateWeightedScatter func(dst []float32, targets []uint32, weights []float32, factor float32)

	// Sparse Search
	BM25ScoreBatch func(tfs []int, docLengths []int, avgDL, idf, k1, b float32) []float32

	// Geospatial
	HaversineBatch haversineBatchFunc
}

// ConversionDispatch holds numerical type casting and normalization kernels.
type ConversionDispatch struct {
	Int8ToFloat32    func(src []int8, dst []float32)
	Uint8ToFloat32   func(src []uint8, dst []float32)
	Int16ToFloat32   func(src []int16, dst []float32)
	Uint16ToFloat32  func(src []uint16, dst []float32)
	Int32ToFloat32   func(src []int32, dst []float32)
	Uint32ToFloat32  func(src []uint32, dst []float32)
	Float16ToFloat32 func(src []float16.Num, dst []float32)
}

// ActivationDispatch holds non-linear activation kernels.
type ActivationDispatch struct {
	Sigmoid func(src, dst []float32)
	Softmax func(src, dst []float32)
	Exp     func(src, dst []float32)
	Log     func(src, dst []float32)
}

// MathDispatch holds reductions, linear algebra, and transcendental functions.
type MathDispatch struct {
	Sum    func(src []float32) float32
	Max    func(src []float32) float32
	Min    func(src []float32) float32
	MatMul func(a, b []float32, m, n, k int, dst []float32)
	Sin    func(src, dst []float32)
	Cos    func(src, dst []float32)
	Sincos func(src, sinDst, cosDst []float32)
	Sqrt   func(src, dst []float32)
	Atan2  func(y, x, dst []float32)
	ArgMax func(src []float32) int
	ArgMin func(src []float32) int
}

// QuantDispatch holds TurboQuant bit-packing and unpacking kernels.
type QuantDispatch struct {
	UnpackTQ2 func(src []byte, dst []float32, scale, bias float32)
	UnpackTQ4 func(src []byte, dst []float32, scale, bias float32)
	UnpackTQ8 func(src []byte, dst []float32, scale, bias float32)
	PackTQ2   func(src []float32, dst []byte)
	PackTQ4   func(src []float32, dst []byte)
	PackTQ8   func(src []float32, dst []byte)
}

// newGenericDispatch constructs the baseline generic SIMD table.
func newGenericDispatch() *ImplementationDispatch {
	return &ImplementationDispatch{
		MetricDispatch: MetricDispatch{
			EuclideanDistance:     euclideanGeneric,
			CosineDistance:        cosineGeneric,
			DotProduct:            dotGeneric,
			EuclideanDistanceF16:  euclideanF16Unrolled4x,
			CosineDistanceF16:     cosineF16Unrolled4x,
			DotProductF16:         dotF16Unrolled4x,
			EuclideanDistance128:  euclideanNEON,
			EuclideanDistance384:  euclidean384Unrolled4x,
			EuclideanDistance768:  euclidean768Unrolled4x,
			EuclideanDistance1024: euclidean1024Blocked,
			EuclideanDistance1536: euclidean1536Unrolled4x,
			EuclideanDistance3072: euclidean3072Blocked,
			DotProduct128:         dot128Unrolled4x,
			DotProduct384:         dotUnrolled4x,
			DotProduct768:         dotUnrolled4x,
			DotProduct1024:        dotUnrolled4x,
			DotProduct1536:        dotUnrolled4x,
			DotProduct3072:        DotProductFloat32Blocked,
			ManhattanDistance:     ManhattanDistanceFloat32,
			ChebyshevDistance:     ChebyshevDistanceFloat32,
			BrayCurtisDistance:    BrayCurtisDistanceFloat32,
			L2SquaredDistance:     L2SquaredFloat32,
		},
		BatchDispatch: BatchDispatch{
			EuclideanDistanceBatch:     euclideanBatchGeneric,
			L2SquaredDistanceBatch:     l2SquaredBatchGeneric,
			CosineDistanceBatch:        cosineBatchGeneric,
			DotProductBatch:            dotBatchGeneric,
			EuclideanDistanceBatchFlat: euclideanBatchFlatGeneric,
			TurboQuantDistanceBatch:    turboQuantDistanceBatchGeneric,
			AccumulateWeightedScatter:  accumulateWeightedScatterGeneric,
			BM25ScoreBatch:             bm25ScoreBatchGeneric,
			HaversineBatch:             haversineBatchGeneric,
		},
		ConversionDispatch: ConversionDispatch{
			Int8ToFloat32:    int8ToFloat32Generic,
			Uint8ToFloat32:   uint8ToFloat32Generic,
			Int16ToFloat32:   int16ToFloat32Generic,
			Uint16ToFloat32:  uint16ToFloat32Generic,
			Int32ToFloat32:   int32ToFloat32Generic,
			Uint32ToFloat32:  uint32ToFloat32Generic,
			Float16ToFloat32: float16ToFloat32Generic,
		},
		ActivationDispatch: ActivationDispatch{
			Sigmoid: sigmoidGeneric,
			Softmax: softmaxGeneric,
			Exp:     expGeneric,
			Log:     logGeneric,
		},
		MathDispatch: MathDispatch{
			Sum:    sumGeneric,
			Max:    maxGeneric,
			Min:    minGeneric,
			MatMul: matMulGeneric,
			Sin:    sinFloat32Generic,
			Cos:    cosFloat32Generic,
			Sincos: sincosFloat32Generic,
			Sqrt:   sqrtFloat32Generic,
			Atan2:  atan2Float32Generic,
			ArgMax: argMaxGeneric,
			ArgMin: argMinGeneric,
		},
		QuantDispatch: QuantDispatch{
			UnpackTQ2: UnpackTQ2Generic,
			UnpackTQ4: UnpackTQ4Generic,
			UnpackTQ8: UnpackTQ8Generic,
			PackTQ2:   PackTQ2Generic,
			PackTQ4:   PackTQ4Generic,
			PackTQ8:   PackTQ8Generic,
		},
	}
}

// layerAVX2 overlays x86 AVX2 vector optimizations onto the generic base.
func layerAVX2(base *ImplementationDispatch) *ImplementationDispatch {
	d := *base
	d.EuclideanDistance = euclideanAVX2
	d.CosineDistance = cosineAVX2
	d.DotProduct = dotAVX2
	d.EuclideanDistanceBatch = euclideanBatchAVX2
	d.L2SquaredDistanceBatch = l2SquaredBatchAVX2
	d.CosineDistanceBatch = cosineBatchAVX2
	d.DotProductBatch = dotBatchAVX2
	d.L2SquaredDistance = l2SquaredAVX2
	d.L2SquaredDistance128 = l2Squared128AVX2
	d.L2SquaredDistance384 = l2Squared384AVX2
	d.L2SquaredDistance768 = l2Squared768AVX2
	d.L2SquaredDistance1024 = l2Squared1024AVX2
	d.L2SquaredDistance3072 = l2Squared3072AVX2
	d.EuclideanDistanceBatchFlat = euclideanBatchFlatAVX2
	d.TurboQuantDistanceBatch = turboQuantDistanceBatchAVX2
	d.EuclideanDistanceF16 = euclideanF16AVX2
	d.CosineDistanceF16 = cosineF16AVX2
	d.DotProductF16 = dotF16AVX2

	d.EuclideanDistance128 = euclidean128AVX2
	d.EuclideanDistance384 = euclidean384AVX2
	d.EuclideanDistance768 = euclidean768AVX2
	d.EuclideanDistance1024 = euclidean1024AVX2
	d.EuclideanDistance1536 = euclidean1536AVX2
	d.EuclideanDistance3072 = euclidean3072AVX2

	d.DotProduct128 = dot128AVX2
	d.DotProduct384 = dot384AVX2
	d.DotProduct768 = dot768AVX2
	d.DotProduct1024 = dot1024AVX2
	d.DotProduct1536 = dot1536AVX2
	d.DotProduct3072 = dot3072AVX2

	d.Int8ToFloat32 = int8ToFloat32AVX2
	d.Uint8ToFloat32 = uint8ToFloat32AVX2
	d.Int16ToFloat32 = int16ToFloat32AVX2
	d.Uint16ToFloat32 = uint16ToFloat32AVX2
	d.Int32ToFloat32 = int32ToFloat32AVX2
	d.Uint32ToFloat32 = uint32ToFloat32AVX2
	d.Float16ToFloat32 = float16ToFloat32AVX2

	d.Sigmoid = sigmoidAVX2
	d.Softmax = softmaxAVX2
	d.Exp = expAVX2
	d.Log = logAVX2
	d.UnpackTQ8 = UnpackTQ8AVX2
	d.PackTQ8 = PackTQ8AVX2

	d.Sum = sumAVX2
	d.Max = maxAVX2
	d.Min = minAVX2
	d.MatMul = matMulAVX2
	d.Sin = sinAVX2
	d.Cos = cosAVX2
	d.Sincos = sincosAVX2
	d.Sqrt = sqrtAVX2
	d.Atan2 = atan2AVX2
	d.ArgMax = argMaxAVX2
	d.ArgMin = argMinAVX2
	d.BrayCurtisDistance = brayCurtisAVX2
	d.HaversineBatch = haversineBatchAVX2
	return &d
}

// layerAVX512 overlays x86 AVX-512 vector optimizations onto the AVX2 layer.
func layerAVX512(avx2 *ImplementationDispatch) *ImplementationDispatch {
	d := *avx2
	d.EuclideanDistance = euclideanAVX512
	d.CosineDistance = cosineAVX512
	d.DotProduct = dotAVX512
	d.EuclideanDistanceBatch = euclideanBatchAVX512
	d.L2SquaredDistanceBatch = l2SquaredBatchAVX512
	d.CosineDistanceBatch = cosineBatchAVX512
	d.DotProductBatch = dotBatchAVX512
	d.EuclideanDistanceBatchFlat = euclideanBatchFlatAVX512
	d.TurboQuantDistanceBatch = turboQuantDistanceBatchAVX512
	d.EuclideanDistanceF16 = euclideanF16AVX512
	d.CosineDistanceF16 = cosineF16AVX512
	d.DotProductF16 = dotF16AVX512
	d.L2SquaredDistance = l2SquaredAVX512
	d.L2SquaredDistance128 = l2Squared128AVX512
	d.L2SquaredDistance384 = l2Squared384AVX512
	d.L2SquaredDistance768 = l2Squared768AVX512
	d.L2SquaredDistance1024 = l2Squared1024AVX512
	d.L2SquaredDistance3072 = l2Squared3072AVX512

	d.EuclideanDistance128 = euclidean128AVX512
	d.EuclideanDistance384 = euclidean384AVX512
	d.EuclideanDistance768 = euclidean768AVX512
	d.EuclideanDistance1024 = euclidean1024AVX512
	d.EuclideanDistance1536 = euclidean1536AVX512
	d.EuclideanDistance3072 = euclidean3072AVX512

	d.DotProduct128 = dot128AVX512
	d.DotProduct384 = dot384AVX512
	d.DotProduct768 = dot768AVX512
	d.DotProduct1024 = dot1024AVX512
	d.DotProduct1536 = dot1536AVX512
	d.DotProduct3072 = dot3072AVX512

	d.Int8ToFloat32 = int8ToFloat32AVX512
	d.Uint8ToFloat32 = uint8ToFloat32AVX512
	d.Int16ToFloat32 = int16ToFloat32AVX512
	d.Uint16ToFloat32 = uint16ToFloat32AVX512
	d.Int32ToFloat32 = int32ToFloat32AVX512
	d.Uint32ToFloat32 = uint32ToFloat32AVX512
	d.Float16ToFloat32 = float16ToFloat32AVX512

	d.Sigmoid = sigmoidAVX512
	d.Softmax = softmaxAVX512
	d.Exp = expAVX512
	d.Log = logAVX512
	d.UnpackTQ8 = UnpackTQ8AVX512
	d.PackTQ2 = PackTQ2AVX512
	d.PackTQ4 = PackTQ4AVX512
	d.PackTQ8 = PackTQ8AVX512

	d.MatMul = matMulGeneric
	d.BrayCurtisDistance = BrayCurtisDistanceFloat32
	d.Sum = sumGeneric
	d.Max = maxGeneric
	d.Min = minGeneric
	d.Sin = sinFloat32Generic
	d.Cos = cosFloat32Generic
	d.Sincos = sincosFloat32Generic
	d.Sqrt = sqrtFloat32Generic
	d.Atan2 = atan2Float32Generic
	d.ArgMax = argMaxGeneric
	d.ArgMin = argMinGeneric

	return &d
}

// layerEmerald overlays Intel AMX Emerald Rapids matrix tiles onto AVX-512.
func layerEmerald(avx512 *ImplementationDispatch) *ImplementationDispatch {
	d := *avx512
	d.EuclideanDistance = euclideanAMX
	d.DotProduct = dotAMX
	d.EuclideanDistanceBatch = euclideanBatchAMX
	d.DotProductBatch = dotBatchAMX
	d.L2SquaredDistance = l2SquaredAMX
	d.MatMul = matMulAMX
	d.EuclideanDistanceF16 = euclideanF16AVX512FP16
	d.DotProductF16 = dotF16AVX512FP16
	return &d
}

// layerGranite overlays Intel Granite Rapids native F16 AMX matrix tiles onto Emerald.
func layerGranite(emerald *ImplementationDispatch) *ImplementationDispatch {
	d := *emerald
	d.EuclideanDistanceF16 = euclideanF16AMX
	d.DotProductF16 = dotF16AMX
	return &d
}

// layerNEON overlays ARM NEON vector instructions onto generic base.
func layerNEON(base *ImplementationDispatch) *ImplementationDispatch {
	d := *base
	d.EuclideanDistance = euclideanNEON
	d.CosineDistance = cosineNEON
	d.DotProduct = func(a, b []float32) (float32, error) {
		if len(a) >= 1024 {
			return amx.DotAMX(a, b)
		}
		return dotNEON(a, b)
	}
	d.EuclideanDistanceBatch = euclideanBatchNEON
	d.L2SquaredDistanceBatch = l2SquaredBatchNEON
	d.CosineDistanceBatch = cosineBatchNEON
	d.DotProductBatch = dotBatchNEON
	d.TurboQuantDistanceBatch = turboQuantDistanceBatchNEON
	d.EuclideanDistanceF16 = euclideanF16NEON
	d.CosineDistanceF16 = cosineF16NEON
	d.DotProductF16 = dotF16NEON
	d.L2SquaredDistance = func(a, b []float32) (float32, error) {
		if len(a) >= 1024 {
			return amx.L2AMX(a, b)
		}
		return l2SquaredNEON(a, b)
	}
	d.L2SquaredDistance128 = l2Squared128NEON
	d.L2SquaredDistance384 = l2Squared384NEON
	d.L2SquaredDistance768 = l2Squared768NEON
	d.L2SquaredDistance1024 = l2Squared1024NEON
	d.L2SquaredDistance3072 = l2Squared3072NEON

	d.EuclideanDistance128 = euclidean128NEON
	d.EuclideanDistance384 = euclidean384NEON
	d.EuclideanDistance768 = euclidean768NEON
	d.EuclideanDistance1024 = euclidean1024NEON
	d.EuclideanDistance1536 = euclidean1536NEON
	d.EuclideanDistance3072 = euclidean3072NEON

	d.DotProduct128 = dot128NEON
	d.DotProduct384 = dot384NEON
	d.DotProduct768 = dot768NEON
	d.DotProduct1024 = dot1024NEON
	d.DotProduct1536 = dot1536NEON
	d.DotProduct3072 = dot3072NEON

	d.Int8ToFloat32 = int8ToFloat32NEON
	d.Uint8ToFloat32 = uint8ToFloat32NEON
	d.Int16ToFloat32 = int16ToFloat32NEON
	d.Uint16ToFloat32 = uint16ToFloat32NEON
	d.Int32ToFloat32 = int32ToFloat32NEON
	d.Uint32ToFloat32 = uint32ToFloat32NEON
	d.Float16ToFloat32 = float16ToFloat32NEON

	d.Sigmoid = sigmoidNEON
	d.Softmax = softmaxNEON
	d.Exp = expNEON
	d.Log = logNEON

	d.Sum = sumNEON
	d.Max = maxNEON
	d.Min = minNEON
	d.MatMul = matMulNEON
	d.ArgMax = argMaxNEON
	d.ArgMin = argMinNEON
	d.ManhattanDistance = manhattanNEON
	d.ChebyshevDistance = chebyshevNEON
	d.BrayCurtisDistance = brayCurtisNEON
	d.AccumulateWeightedScatter = accumulateWeightedScatterNEON
	d.UnpackTQ2 = UnpackTQ2NEON
	d.UnpackTQ4 = UnpackTQ4NEON
	d.UnpackTQ8 = UnpackTQ8NEON
	d.PackTQ2 = PackTQ2NEON
	d.PackTQ4 = PackTQ4NEON
	d.PackTQ8 = PackTQ8NEON
	return &d
}

// layerSVE2 overlays ARM SVE2 predicate-driven vector instructions onto NEON base.
func layerSVE2(base *ImplementationDispatch) *ImplementationDispatch {
	d := *base
	d.EuclideanDistance = euclideanSVE2
	d.CosineDistance = cosineSVE2
	d.DotProduct = func(a, b []float32) (float32, error) {
		if len(a) != len(b) {
			return 0, ErrDimensionMismatch
		}
		return dotSVE2(a, b), nil
	}
	d.L2SquaredDistance = func(a, b []float32) (float32, error) {
		if len(a) != len(b) {
			return 0, ErrDimensionMismatch
		}
		return l2SquaredSVE2(a, b), nil
	}
	return &d
}

// initDispatchTable builds all architecture dispatch tables using layered inheritance.
func initDispatchTable() {
	initTableOnce.Do(func() {
		generic := newGenericDispatch()
		avx2 := layerAVX2(generic)
		avx512 := layerAVX512(avx2)
		emerald := layerEmerald(avx512)
		granite := layerGranite(emerald)
		neon := layerNEON(generic)
		sve2 := layerSVE2(neon)

		dispatchTable["generic"] = generic
		dispatchTable["avx2"] = avx2
		dispatchTable["avx512"] = avx512
		dispatchTable["emerald"] = emerald
		dispatchTable["granite"] = granite
		dispatchTable["neon"] = neon
		dispatchTable["sve2"] = sve2
	})
}
