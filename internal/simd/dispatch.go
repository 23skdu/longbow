package simd

import (
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"unsafe"
	"github.com/23skdu/longbow/internal/simd/amx"
)

// ImplementationDispatch holds all SIMD function pointers for a specific implementation
type ImplementationDispatch struct {
	// Core distance functions
	EuclideanDistance distanceFunc
	CosineDistance    distanceFunc
	DotProduct        distanceFunc

	// F16 distance functions
	EuclideanDistanceF16 distanceF16Func
	CosineDistanceF16    distanceF16Func
	DotProductF16        distanceF16Func

	// Batch functions
	EuclideanDistanceBatch     distanceBatchFunc
	CosineDistanceBatch        distanceBatchFunc
	DotProductBatch            distanceBatchFunc
	EuclideanDistanceBatchFlat distanceBatchFlatFunc

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

	// Type conversion
	Int8ToFloat32   func(src []int8, dst []float32)
	Uint8ToFloat32  func(src []uint8, dst []float32)
	Int16ToFloat32  func(src []int16, dst []float32)
	Uint16ToFloat32 func(src []uint16, dst []float32)
	Int32ToFloat32  func(src []int32, dst []float32)
	Uint32ToFloat32 func(src []uint32, dst []float32)
	Float16ToFloat32 func(src []float16.Num, dst []float32)

	// Activations
	Sigmoid func(src, dst []float32)
	Softmax func(src, dst []float32)
	Exp     func(src, dst []float32)
	Log     func(src, dst []float32)

	// Reductions
	Sum func(src []float32) float32
	Max func(src []float32) float32
	Min func(src []float32) float32

	// Matrix operations
	MatMul func(a, b []float32, m, n, k int, dst []float32)

	// Transcendental
	Sin   func(src, dst []float32)
	Cos   func(src, dst []float32)
	Atan2 func(y, x, dst []float32)

	// More reductions
	ArgMax func(src []float32) int
	ArgMin func(src []float32) int

	// More distances
	ManhattanDistance  distanceFunc
	ChebyshevDistance  distanceFunc
	BrayCurtisDistance distanceFunc
	L2SquaredDistance  distanceFunc
 
	// Batch processing (GraphRAG expansion)
	AccumulateWeightedScatter func(dst []float32, targets []uint32, weights []float32, factor float32)
 
	// Sparse Search
	BM25ScoreBatch func(tfs []int, docLengths []int, avgDL, idf, k1, b float32) []float32

	// Geospatial
	HaversineBatch haversineBatchFunc
	// TurboQuant
	UnpackTQ2 func(src []byte, dst []float32, scale, bias float32)
	UnpackTQ4 func(src []byte, dst []float32, scale, bias float32)
	UnpackTQ8 func(src []byte, dst []float32, scale, bias float32)
	PackTQ2   func(src []float32, dst []byte)
	PackTQ4   func(src []float32, dst []byte)
	PackTQ8   func(src []float32, dst []byte)
}

// Global dispatch table - one per implementation
var (
	dispatchTable = make(map[string]*ImplementationDispatch)
	initTableOnce sync.Once
)

func init() {
	initDispatchTable()
}

func initDispatchTable() {
	initTableOnce.Do(func() {
		dispatchTable["avx512"] = &ImplementationDispatch{
			EuclideanDistance:          euclideanAVX512,
			CosineDistance:             cosineAVX512,
			DotProduct:                 dotAVX512,
			EuclideanDistanceBatch:     euclideanBatchGeneric, // Fallback for now
			CosineDistanceBatch:        cosineBatchGeneric,
			DotProductBatch:            dotBatchGeneric,
			EuclideanDistanceBatchFlat: euclideanBatchFlatAVX512,
			EuclideanDistanceF16:       euclideanF16AVX512,
			CosineDistanceF16:          cosineF16AVX512,
			DotProductF16:               dotF16AVX512,
			L2SquaredDistance:           l2SquaredAVX512,
			L2SquaredDistance128:  l2Squared128AVX512,
			L2SquaredDistance384:  l2Squared384AVX512,
			L2SquaredDistance768:  l2Squared768AVX512,
			L2SquaredDistance1024: l2Squared1024AVX512,
			L2SquaredDistance3072: l2Squared3072AVX512,

			EuclideanDistance128:  euclidean128AVX512,
			EuclideanDistance384:  euclidean384AVX512,
			EuclideanDistance768:  euclidean768AVX512,
			EuclideanDistance1024: euclidean1024AVX512,
			EuclideanDistance1536: euclidean1536AVX512,
			EuclideanDistance3072: euclidean3072AVX512,

			DotProduct128:  dot128AVX512,
			DotProduct384:  dot384AVX512,
			DotProduct768:  dot768AVX512,
			DotProduct1024: dot1024AVX512,
			DotProduct1536: dot1536AVX512,
			DotProduct3072: dot3072AVX512,

			Int8ToFloat32:   int8ToFloat32AVX512,
			Uint8ToFloat32:  uint8ToFloat32AVX512,
			Int16ToFloat32:  int16ToFloat32AVX512,
			Uint16ToFloat32: uint16ToFloat32AVX512,
			Int32ToFloat32:  int32ToFloat32AVX512,
			Uint32ToFloat32: uint32ToFloat32AVX512,
			Float16ToFloat32: float16ToFloat32AVX512,

			Sigmoid: sigmoidAVX512,
			Softmax: softmaxAVX512,
			Exp:     expAVX512,
			Log:     logAVX512,
			UnpackTQ2: UnpackTQ2AVX512,
			UnpackTQ4: UnpackTQ4AVX512,
			UnpackTQ8: UnpackTQ8AVX512,
			PackTQ2:   PackTQ2AVX512,
			PackTQ4:   PackTQ4AVX512,
			PackTQ8:   PackTQ8AVX512,

			Sum: sumGeneric,
			Max: maxGeneric,
			Min: minGeneric,
			MatMul: matMulGeneric,
			Sin: sinFloat32Generic,
			Cos: cosFloat32Generic,
			Atan2: atan2Float32Generic,
			ArgMax: argMaxGeneric,
			ArgMin: argMinGeneric,
			ManhattanDistance: ManhattanDistanceFloat32,
			ChebyshevDistance: ChebyshevDistanceFloat32,
			BrayCurtisDistance: BrayCurtisDistanceFloat32,
			AccumulateWeightedScatter: accumulateWeightedScatterGeneric,
			BM25ScoreBatch: bm25ScoreBatchGeneric,
			HaversineBatch: haversineBatchGeneric,
		}

		dispatchTable["avx2"] = &ImplementationDispatch{
			EuclideanDistance:          euclideanAVX2,
			CosineDistance:             cosineAVX2,
			DotProduct:                 dotAVX2,
			EuclideanDistanceBatch:     euclideanBatchAVX2,
			CosineDistanceBatch:        cosineBatchAVX2,
			DotProductBatch:            dotBatchAVX2,
			L2SquaredDistance:           l2SquaredAVX2,
			L2SquaredDistance128:  l2Squared128AVX2,
			L2SquaredDistance384:  l2Squared384AVX2,
			L2SquaredDistance768:  l2Squared768AVX2,
			L2SquaredDistance1024: l2Squared1024AVX2,
			L2SquaredDistance3072: l2Squared3072AVX2,
			EuclideanDistanceBatchFlat: euclideanBatchFlatAVX2,
			EuclideanDistanceF16:       euclideanF16Unrolled4x,
			CosineDistanceF16:          cosineF16Unrolled4x,
			DotProductF16:               dotF16Unrolled4x,

			EuclideanDistance128:       euclidean128AVX2,
			EuclideanDistance384:       euclidean384AVX2,
			EuclideanDistance768:       euclidean768AVX2,
			EuclideanDistance1024:      euclidean1024AVX2,
			EuclideanDistance1536:      euclidean1536AVX2,
			EuclideanDistance3072:      euclidean3072AVX2,

			DotProduct128:  dot128AVX2,
			DotProduct384:  dot384AVX2,
			DotProduct768:  dot768AVX2,
			DotProduct1024: dot1024AVX2,
			DotProduct1536: dot1536AVX2,
			DotProduct3072: dot3072AVX2,

			Int8ToFloat32:   int8ToFloat32AVX2,
			Uint8ToFloat32:  uint8ToFloat32AVX2,
			Int16ToFloat32:  int16ToFloat32AVX2,
			Uint16ToFloat32: uint16ToFloat32AVX2,
			Int32ToFloat32:  int32ToFloat32AVX2,
			Uint32ToFloat32: uint32ToFloat32AVX2,
			Float16ToFloat32: float16ToFloat32AVX2,

			Sigmoid: sigmoidAVX2,
			Softmax: softmaxAVX2,
			Exp:     expAVX2,
			Log:     logAVX2,
			UnpackTQ2: UnpackTQ2AVX2,
			UnpackTQ4: UnpackTQ4AVX2,
			UnpackTQ8: UnpackTQ8AVX2,
			PackTQ2:   PackTQ2AVX2,
			PackTQ4:   PackTQ4AVX2,
			PackTQ8:   PackTQ8AVX2,

			Sum: sumAVX2,
			Max: maxAVX2,
			Min: minAVX2,
			MatMul: matMulAVX2,
			Sin: sinAVX2,
			Cos: cosAVX2,
			Atan2: atan2AVX2,
			ArgMax: argMaxAVX2,
			ArgMin: argMinAVX2,
			ManhattanDistance: ManhattanDistanceFloat32,
			ChebyshevDistance: ChebyshevDistanceFloat32,
			BrayCurtisDistance: brayCurtisAVX2,
			AccumulateWeightedScatter: accumulateWeightedScatterGeneric,
			BM25ScoreBatch: bm25ScoreBatchGeneric,
			HaversineBatch: haversineBatchGeneric,
		}

		dispatchTable["neon"] = &ImplementationDispatch{
			EuclideanDistance:          euclideanNEON,
			CosineDistance:             cosineNEON,
			DotProduct: func(a, b []float32) (float32, error) {
				if len(a) >= 1024 {
					return amx.DotAMX(a, b)
				}
				return dotNEON(a, b)
			},
			EuclideanDistanceBatch:     euclideanBatchNEON,
			CosineDistanceBatch:        cosineBatchNEON,
			DotProductBatch:            dotBatchNEON,
			EuclideanDistanceBatchFlat: euclideanBatchFlatGeneric,
			EuclideanDistanceF16:       euclideanF16NEON,
			CosineDistanceF16:          cosineF16NEON,
			DotProductF16:               dotF16NEON,
			L2SquaredDistance: func(a, b []float32) (float32, error) {
				if len(a) >= 1024 {
					return amx.L2AMX(a, b)
				}
				return l2SquaredNEON(a, b)
			},
			L2SquaredDistance128:  l2Squared128NEON,
			L2SquaredDistance384:  l2Squared384NEON,
			L2SquaredDistance768:  l2Squared768NEON,
			L2SquaredDistance1024: l2Squared1024NEON,
			L2SquaredDistance3072: l2Squared3072NEON,

			EuclideanDistance128:  euclidean128NEON,
			EuclideanDistance384:  euclidean384NEON,
			EuclideanDistance768:  euclidean768NEON,
			EuclideanDistance1024: euclidean1024NEON,
			EuclideanDistance1536: euclidean1536NEON,
			EuclideanDistance3072: euclidean3072NEON,

			DotProduct128:  dot128NEON,
			DotProduct384:  dot384NEON,
			DotProduct768:  dot768NEON,
			DotProduct1024: dot1024NEON,
			DotProduct1536: dot1536NEON,
			DotProduct3072: dot3072NEON,

			Int8ToFloat32:   int8ToFloat32NEON,
			Uint8ToFloat32:  uint8ToFloat32NEON,
			Int16ToFloat32:  int16ToFloat32NEON,
			Uint16ToFloat32: uint16ToFloat32NEON,
			Int32ToFloat32:  int32ToFloat32NEON,
			Uint32ToFloat32: uint32ToFloat32NEON,
			Float16ToFloat32: float16ToFloat32NEON,

			Sigmoid: sigmoidNEON,
			Softmax: softmaxNEON,
			Exp:     expNEON,
			Log:     logNEON,

			Sum: sumNEON,
			Max: maxNEON,
			Min: minNEON,
			MatMul: matMulNEON,
			Sin: sinFloat32Generic,
			Cos: cosFloat32Generic,
			Atan2: atan2Float32Generic,
			ArgMax: argMaxNEON,
			ArgMin: argMinNEON,
			ManhattanDistance: manhattanNEON,
			ChebyshevDistance: chebyshevNEON,
			BrayCurtisDistance: brayCurtisNEON,
			AccumulateWeightedScatter: accumulateWeightedScatterNEON,
			BM25ScoreBatch: bm25ScoreBatchGeneric,
			HaversineBatch: haversineBatchGeneric,
			UnpackTQ2:      UnpackTQ2NEON,
			UnpackTQ4:      UnpackTQ4NEON,
			UnpackTQ8:      UnpackTQ8NEON,
			PackTQ2:        PackTQ2NEON,
			PackTQ4:        PackTQ4NEON,
			PackTQ8:        PackTQ8NEON,
		}

		dispatchTable["generic"] = &ImplementationDispatch{
			EuclideanDistance:          euclideanGeneric,
			CosineDistance:             cosineGeneric,
			DotProduct:                 dotGeneric,
			EuclideanDistanceBatch:     euclideanBatchGeneric,
			CosineDistanceBatch:        cosineBatchGeneric,
			DotProductBatch:            dotBatchGeneric,
			EuclideanDistanceBatchFlat: euclideanBatchFlatGeneric,
			EuclideanDistanceF16:       euclideanF16Unrolled4x,
			CosineDistanceF16:          cosineF16Unrolled4x,
			DotProductF16:               dotF16Unrolled4x,

			EuclideanDistance128:  euclideanNEON,
			EuclideanDistance384:  euclidean384Unrolled4x,
			EuclideanDistance768:  euclidean768Unrolled4x,
			EuclideanDistance1024: euclidean1024Blocked,
			EuclideanDistance1536: euclidean1536Unrolled4x,
			EuclideanDistance3072: euclidean3072Blocked,

			DotProduct128:  dot128Unrolled4x,
			DotProduct384:  dotUnrolled4x,
			DotProduct768:  dotUnrolled4x,
			DotProduct1024: dotUnrolled4x,
			DotProduct1536: dotUnrolled4x,
			DotProduct3072: DotProductFloat32Blocked,

			Int8ToFloat32:   int8ToFloat32Generic,
			Uint8ToFloat32:  uint8ToFloat32Generic,
			Int16ToFloat32:  int16ToFloat32Generic,
			Uint16ToFloat32: uint16ToFloat32Generic,
			Int32ToFloat32:  int32ToFloat32Generic,
			Uint32ToFloat32: uint32ToFloat32Generic,
			Float16ToFloat32: float16ToFloat32Generic,

			Sigmoid: sigmoidGeneric,
			Softmax: softmaxGeneric,
			Exp:     expGeneric,
			Log:     logGeneric,

			Sum: sumGeneric,
			Max: maxGeneric,
			Min: minGeneric,
			MatMul: matMulGeneric,
			Sin: sinFloat32Generic,
			Cos: cosFloat32Generic,
			Atan2: atan2Float32Generic,
			ArgMax: argMaxGeneric,
			ArgMin: argMinGeneric,
			ManhattanDistance: ManhattanDistanceFloat32,
			ChebyshevDistance: ChebyshevDistanceFloat32,
			BrayCurtisDistance: BrayCurtisDistanceFloat32,
			AccumulateWeightedScatter: accumulateWeightedScatterGeneric,
			BM25ScoreBatch: bm25ScoreBatchGeneric,
			HaversineBatch: haversineBatchGeneric,
			UnpackTQ2:      UnpackTQ2Generic,
			UnpackTQ4:      UnpackTQ4Generic,
			UnpackTQ8:      UnpackTQ8Generic,
			PackTQ2:        PackTQ2Generic,
			PackTQ4:        PackTQ4Generic,
			PackTQ8:        PackTQ8Generic,
		}
	})
}


// Current dispatch - single pointer lookup instead of many
var currentDispatch *ImplementationDispatch

// initializeDispatch sets function pointers based on detected CPU features.
// This is called once at startup, removing branch overhead from hot paths.
func initializeDispatch() {
	initDispatchTable()
	dispatch, exists := dispatchTable[implementation]
	if !exists {
		// Fallback to generic if implementation not found
		dispatch = dispatchTable["generic"]
	}
	currentDispatch = dispatch
	switch implementation {
	case "avx512":
		euclideanDistanceImpl = dispatch.EuclideanDistance
		euclideanDistance384Impl = dispatch.EuclideanDistance384
		euclideanDistance768Impl = dispatch.EuclideanDistance768
		euclideanDistance1024Impl = dispatch.EuclideanDistance1024
		euclideanDistance1536Impl = dispatch.EuclideanDistance1536
		euclideanDistance3072Impl = dispatch.EuclideanDistance3072
		euclideanDistance128Impl = dispatch.EuclideanDistance128
		metrics.SimdDispatchCount.WithLabelValues("avx512").Inc()
		metrics.SimdStaticDispatchType.Set(3)
		cosineDistanceImpl = dispatch.CosineDistance
		dotProductImpl = dispatch.DotProduct
		dotProduct384Impl = dispatch.DotProduct384
		dotProduct768Impl = dispatch.DotProduct768
		dotProduct1024Impl = dispatch.DotProduct1024
		dotProduct1536Impl = dispatch.DotProduct1536
		dotProduct3072Impl = dispatch.DotProduct3072
		dotProduct128Impl = dispatch.DotProduct128
		euclideanDistanceBatchImpl = dispatch.EuclideanDistanceBatch
		cosineDistanceBatchImpl = dispatch.CosineDistanceBatch
		dotProductBatchImpl = dispatch.DotProductBatch
		l2SquaredImpl = dispatch.L2SquaredDistance
		l2Squared128Impl = dispatch.L2SquaredDistance128
		l2Squared384Impl = dispatch.L2SquaredDistance384
		l2Squared768Impl = dispatch.L2SquaredDistance768
		l2Squared1024Impl = dispatch.L2SquaredDistance1024
		l2Squared3072Impl = dispatch.L2SquaredDistance3072
		prefetchImpl = func(p unsafe.Pointer) { prefetchNTA(uintptr(p)) }
		memcpyNTAImpl = memcpyGeneric // Use generic for now on x86, we will add NTA later
		matchInt64Impl = matchInt64AVX512
		matchInt32Impl = matchInt32AVX512
		matchFloat32Impl = matchFloat32AVX512
		matchFloat64Impl = matchFloat64AVX512
		adcDistanceBatchImpl = adcBatchAVX512
		euclideanDistanceVerticalBatchImpl = euclideanBatchGeneric
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchAVX512
		euclideanDistanceF16BatchImpl = euclideanF16BatchAVX512
		andBytesImpl = andBytesAVX512
		orBytesImpl = orBytesAVX512
		notBytesImpl = notBytesGeneric
		isAllZerosImpl = isAllZerosAVX512
		euclideanDistanceF16Impl = euclideanF16AVX512
		cosineDistanceF16Impl = cosineF16AVX512
		dotProductF16Impl = dotF16AVX512
		euclideanDistanceFloat64Impl = euclideanFloat64AVX512
		dotProductFloat64Impl = dotFloat64AVX512
		l2SquaredFloat64Impl = l2SquaredFloat64AVX512
		cosineDistanceFloat64Impl = cosineFloat64Unrolled4x
		euclideanDistanceInt8Impl = euclideanInt8AVX512
		dotProductInt8Impl = dotInt8Unrolled4x
		dotProductUint8Impl = dotUint8Unrolled4x
		euclideanDistanceUint8Impl = euclideanUint8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16AVX512
		euclideanDistanceUint16Impl = euclideanUint16AVX512
		dotProductInt16Impl = dotInt16AVX512
		dotProductUint16Impl = dotUint16AVX512
		dotProductInt4Impl = dotInt4AVX512
		dotProductInt2Impl = dotInt2AVX512

		int8ToFloat32Impl = dispatch.Int8ToFloat32
		uint8ToFloat32Impl = dispatch.Uint8ToFloat32
		int16ToFloat32Impl = dispatch.Int16ToFloat32
		uint16ToFloat32Impl = dispatch.Uint16ToFloat32
		int32ToFloat32Impl = dispatch.Int32ToFloat32
		uint32ToFloat32Impl = dispatch.Uint32ToFloat32
		float16ToFloat32Impl = dispatch.Float16ToFloat32

		sigmoidFloat32Impl = dispatch.Sigmoid
		softmaxFloat32Impl = dispatch.Softmax
		expFloat32Impl = dispatch.Exp
		logFloat32Impl = dispatch.Log

		sumFloat32Impl = dispatch.Sum
		maxFloat32Impl = dispatch.Max
		minFloat32Impl = dispatch.Min
		matMulFloat32Impl = dispatch.MatMul
		sinFloat32Impl = dispatch.Sin
		cosFloat32Impl = dispatch.Cos
		atan2Float32Impl = dispatch.Atan2
		argMaxFloat32Impl = dispatch.ArgMax
		argMinFloat32Impl = dispatch.ArgMin
		manhattanDistanceImpl = dispatch.ManhattanDistance
		chebyshevDistanceImpl = dispatch.ChebyshevDistance
		brayCurtisDistanceImpl = dispatch.BrayCurtisDistance
		accumulateWeightedScatterFloat32Impl = dispatch.AccumulateWeightedScatter
		haversineBatchImpl = dispatch.HaversineBatch
		unpackTQ2Impl = dispatch.UnpackTQ2
		unpackTQ4Impl = dispatch.UnpackTQ4
		unpackTQ8Impl = dispatch.UnpackTQ8
		packTQ2Impl = dispatch.PackTQ2
		packTQ4Impl = dispatch.PackTQ4
		packTQ8Impl = dispatch.PackTQ8
	case "avx2":
		euclideanDistanceImpl = dispatch.EuclideanDistance
		euclideanDistance384Impl = dispatch.EuclideanDistance384
		euclideanDistance768Impl = dispatch.EuclideanDistance768
		euclideanDistance1024Impl = dispatch.EuclideanDistance1024
		euclideanDistance1536Impl = dispatch.EuclideanDistance1536
		euclideanDistance3072Impl = dispatch.EuclideanDistance3072
		euclideanDistance128Impl = dispatch.EuclideanDistance128
		metrics.SimdDispatchCount.WithLabelValues("avx2").Inc()
		metrics.SimdStaticDispatchType.Set(2)
		cosineDistanceImpl = dispatch.CosineDistance
		dotProductImpl = dispatch.DotProduct
		dotProduct384Impl = dispatch.DotProduct384
		dotProduct768Impl = dispatch.DotProduct768
		dotProduct1024Impl = dispatch.DotProduct1024
		dotProduct1536Impl = dispatch.DotProduct1536
		dotProduct3072Impl = dispatch.DotProduct3072
		dotProduct128Impl = dispatch.DotProduct128
		euclideanDistanceBatchImpl = euclideanBatchGeneric // AVX2 vertical batch kernel is a stub; use verified generic

		cosineDistanceBatchImpl = cosineBatchGeneric // AVX2 batch kernel is a stub; use verified generic
		dotProductBatchImpl = dotBatchGeneric       // AVX2 batch kernel is a stub; use verified generic
		l2SquaredImpl = l2SquaredAVX2 // uses AVX2 kernel (no sqrt)
		l2Squared128Impl = dispatch.L2SquaredDistance128
		l2Squared384Impl = dispatch.L2SquaredDistance384
		l2Squared768Impl = dispatch.L2SquaredDistance768
		l2Squared1024Impl = dispatch.L2SquaredDistance1024
		l2Squared3072Impl = dispatch.L2SquaredDistance3072
		prefetchImpl = func(p unsafe.Pointer) { prefetchNTA(uintptr(p)) }
		memcpyNTAImpl = memcpyGeneric
		matchInt64Impl = matchInt64Generic
		matchInt32Impl = matchInt32Generic
		matchFloat32Impl = matchFloat32Generic
		matchFloat64Impl = matchFloat64Generic
		adcDistanceBatchImpl = adcBatchGeneric
		euclideanDistanceVerticalBatchImpl = euclideanBatchGeneric // AVX2 vertical batch kernel is a stub
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric
		euclideanDistanceF16BatchImpl = euclideanF16BatchGeneric
		andBytesImpl = andBytesAVX2
		orBytesImpl = orBytesAVX2
		notBytesImpl = notBytesGeneric
		isAllZerosImpl = isAllZerosAVX2
		euclideanDistanceF16Impl = euclideanF16Unrolled4x
		cosineDistanceF16Impl = cosineF16Unrolled4x
		dotProductF16Impl = dotF16Unrolled4x
		euclideanDistanceFloat64Impl = euclideanFloat64AVX2
		dotProductFloat64Impl = dotFloat64AVX2
		l2SquaredFloat64Impl = l2SquaredFloat64AVX2
		cosineDistanceFloat64Impl = cosineFloat64Unrolled4x
		euclideanDistanceInt8Impl = euclideanInt8Unrolled4x
		dotProductInt8Impl = dotInt8Unrolled4x
		dotProductUint8Impl = dotUint8Unrolled4x
		euclideanDistanceUint8Impl = euclideanUint8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16Unrolled4x
		euclideanDistanceUint16Impl = euclideanUint16Unrolled4x
		dotProductInt16Impl = dotInt16Unrolled4x
		dotProductUint16Impl = dotUint16Unrolled4x
		dotProductInt4Impl = dotInt4Generic
		dotProductInt2Impl = dotInt2Generic

		int8ToFloat32Impl = dispatch.Int8ToFloat32
		uint8ToFloat32Impl = dispatch.Uint8ToFloat32
		int16ToFloat32Impl = dispatch.Int16ToFloat32
		uint16ToFloat32Impl = dispatch.Uint16ToFloat32
		int32ToFloat32Impl = dispatch.Int32ToFloat32
		uint32ToFloat32Impl = dispatch.Uint32ToFloat32
		float16ToFloat32Impl = dispatch.Float16ToFloat32

		sigmoidFloat32Impl = dispatch.Sigmoid
		softmaxFloat32Impl = dispatch.Softmax
		expFloat32Impl = dispatch.Exp
		logFloat32Impl = dispatch.Log

		sumFloat32Impl = dispatch.Sum
		maxFloat32Impl = dispatch.Max
		minFloat32Impl = dispatch.Min
		matMulFloat32Impl = dispatch.MatMul
		sinFloat32Impl = dispatch.Sin
		cosFloat32Impl = dispatch.Cos
		atan2Float32Impl = dispatch.Atan2
		argMaxFloat32Impl = dispatch.ArgMax
		argMinFloat32Impl = dispatch.ArgMin
		manhattanDistanceImpl = dispatch.ManhattanDistance
		chebyshevDistanceImpl = dispatch.ChebyshevDistance
		brayCurtisDistanceImpl = dispatch.BrayCurtisDistance
		accumulateWeightedScatterFloat32Impl = dispatch.AccumulateWeightedScatter
		haversineBatchImpl = dispatch.HaversineBatch
		unpackTQ2Impl = dispatch.UnpackTQ2
		unpackTQ4Impl = dispatch.UnpackTQ4
		unpackTQ8Impl = dispatch.UnpackTQ8
		packTQ2Impl = dispatch.PackTQ2
		packTQ4Impl = dispatch.PackTQ4
		packTQ8Impl = dispatch.PackTQ8
	case "neon":
		euclideanDistanceImpl = dispatch.EuclideanDistance
		euclideanDistance384Impl = dispatch.EuclideanDistance384
		euclideanDistance768Impl = dispatch.EuclideanDistance768
		euclideanDistance1024Impl = dispatch.EuclideanDistance1024
		euclideanDistance1536Impl = dispatch.EuclideanDistance1536
		euclideanDistance3072Impl = dispatch.EuclideanDistance3072
		euclideanDistance128Impl = dispatch.EuclideanDistance128
		metrics.SimdDispatchCount.WithLabelValues("neon").Inc()
		metrics.SimdStaticDispatchType.Set(1)
		cosineDistanceImpl = dispatch.CosineDistance
		dotProductImpl = dispatch.DotProduct
		dotProduct384Impl = dispatch.DotProduct384
		dotProduct768Impl = dispatch.DotProduct768
		dotProduct1024Impl = dispatch.DotProduct1024
		dotProduct1536Impl = dispatch.DotProduct1536
		dotProduct3072Impl = dispatch.DotProduct3072
		dotProduct128Impl = dispatch.DotProduct128
		euclideanDistanceBatchImpl = euclideanBatchNEON
		cosineDistanceBatchImpl = cosineBatchNEON
		dotProductBatchImpl = dotBatchNEON
		l2SquaredImpl = l2SquaredNEON
		l2Squared128Impl = dispatch.L2SquaredDistance128
		l2Squared384Impl = dispatch.L2SquaredDistance384
		l2Squared768Impl = dispatch.L2SquaredDistance768
		l2Squared1024Impl = dispatch.L2SquaredDistance1024
		l2Squared3072Impl = dispatch.L2SquaredDistance3072
		prefetchImpl = prefetchGeneric
		matchInt64Impl = matchInt64Generic
		matchInt32Impl = matchInt32Generic
		matchFloat32Impl = matchFloat32Generic
		matchFloat64Impl = matchFloat64Generic
		adcDistanceBatchImpl = adcBatchGeneric
		euclideanDistanceVerticalBatchImpl = euclideanBatchGeneric // Fallback - vertical batch has separate issues
		cosineDistanceBatchImpl = cosineBatchNEON // Temp fallback
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric
		euclideanDistanceF16BatchImpl = euclideanF16BatchGeneric
		andBytesImpl = andBytesGeneric
		orBytesImpl = orBytesGeneric
		notBytesImpl = notBytesGeneric
		isAllZerosImpl = isAllZerosGeneric
		// F16 Kernels
		euclideanDistanceF16Impl = dispatch.EuclideanDistanceF16
		cosineDistanceF16Impl = dispatch.CosineDistanceF16
		dotProductF16Impl = dispatch.DotProductF16
		euclideanDistanceComplex64Impl = euclideanComplex64Optimized
		euclideanDistanceComplex128Impl = euclideanComplex128Unrolled
		euclideanDistanceFloat64Impl = euclideanFloat64NEON
		dotProductFloat64Impl = dotFloat64Unrolled4x
		l2SquaredFloat64Impl = l2SquaredFloat64Unrolled4x
		cosineDistanceFloat64Impl = cosineFloat64Unrolled4x
		euclideanDistanceInt8Impl = euclideanInt8Unrolled4x
		euclideanDistanceUint8Impl = euclideanUint8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16Unrolled4x
		euclideanDistanceUint16Impl = euclideanUint16Unrolled4x
		dotProductInt8Impl = dotInt8Unrolled4x
		dotProductUint8Impl = dotUint8Unrolled4x
		dotProductInt16Impl = dotInt16Unrolled4x
		dotProductUint16Impl = dotUint16Unrolled4x
		dotProductInt4Impl = dotInt4Neon
		dotProductInt2Impl = dotInt2Neon
		memcpyNTAImpl = memcpyNEON
		
		int8ToFloat32Impl = dispatch.Int8ToFloat32
		uint8ToFloat32Impl = dispatch.Uint8ToFloat32
		int16ToFloat32Impl = dispatch.Int16ToFloat32
		uint16ToFloat32Impl = dispatch.Uint16ToFloat32
		int32ToFloat32Impl = dispatch.Int32ToFloat32
		uint32ToFloat32Impl = dispatch.Uint32ToFloat32
		float16ToFloat32Impl = dispatch.Float16ToFloat32

		sigmoidFloat32Impl = dispatch.Sigmoid
		softmaxFloat32Impl = dispatch.Softmax
		expFloat32Impl = dispatch.Exp
		logFloat32Impl = dispatch.Log

		sumFloat32Impl = dispatch.Sum
		maxFloat32Impl = dispatch.Max
		minFloat32Impl = dispatch.Min
		matMulFloat32Impl = dispatch.MatMul
		sinFloat32Impl = dispatch.Sin
		cosFloat32Impl = dispatch.Cos
		atan2Float32Impl = dispatch.Atan2
		argMaxFloat32Impl = dispatch.ArgMax
		argMinFloat32Impl = dispatch.ArgMin
		manhattanDistanceImpl = dispatch.ManhattanDistance
		chebyshevDistanceImpl = dispatch.ChebyshevDistance
		brayCurtisDistanceImpl = dispatch.BrayCurtisDistance
		accumulateWeightedScatterFloat32Impl = dispatch.AccumulateWeightedScatter
		haversineBatchImpl = dispatch.HaversineBatch
		unpackTQ2Impl = dispatch.UnpackTQ2
		unpackTQ4Impl = dispatch.UnpackTQ4
		unpackTQ8Impl = dispatch.UnpackTQ8
		packTQ2Impl = dispatch.PackTQ2
		packTQ4Impl = dispatch.PackTQ4
		packTQ8Impl = dispatch.PackTQ8
	case "generic":
		euclideanDistanceImpl = dispatch.EuclideanDistance
		euclideanDistance128Impl = dispatch.EuclideanDistance128
		euclideanDistance384Impl = dispatch.EuclideanDistance384
		euclideanDistance768Impl = dispatch.EuclideanDistance768
		euclideanDistance1024Impl = dispatch.EuclideanDistance1024
		euclideanDistance1536Impl = dispatch.EuclideanDistance1536
		euclideanDistance3072Impl = dispatch.EuclideanDistance3072
		metrics.SimdDispatchCount.WithLabelValues("generic").Inc()
		metrics.SimdStaticDispatchType.Set(0)
		cosineDistanceImpl = dispatch.CosineDistance
		dotProductImpl = dispatch.DotProduct
		dotProduct128Impl = dispatch.DotProduct128
		dotProduct384Impl = dispatch.DotProduct384
		dotProduct768Impl = dispatch.DotProduct768
		dotProduct1024Impl = dispatch.DotProduct1024
		dotProduct1536Impl = dispatch.DotProduct1536
		dotProduct3072Impl = dispatch.DotProduct3072
		euclideanDistanceBatchImpl = euclideanBatchUnrolled4x
		cosineDistanceBatchImpl = cosineBatchUnrolled4x
		dotProductBatchImpl = dotBatchUnrolled4x
		l2SquaredImpl = L2SquaredFloat32
		prefetchImpl = prefetchGeneric
		matchInt64Impl = matchInt64Generic
		matchInt32Impl = matchInt32Generic
		matchFloat32Impl = matchFloat32Generic
		matchFloat64Impl = matchFloat64Generic
		adcDistanceBatchImpl = adcBatchGeneric
		euclideanDistanceVerticalBatchImpl = euclideanBatchGeneric
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric
		euclideanDistanceF16BatchImpl = euclideanF16BatchGeneric
		andBytesImpl = andBytesGeneric
		orBytesImpl = orBytesGeneric
		notBytesImpl = notBytesGeneric
		isAllZerosImpl = isAllZerosGeneric
		euclideanDistanceF16Impl = euclideanF16Unrolled4x
		cosineDistanceF16Impl = cosineF16Unrolled4x
		dotProductF16Impl = dotF16Unrolled4x
		euclideanDistanceComplex64Impl = euclideanComplex64Optimized
		euclideanDistanceComplex128Impl = euclideanComplex128Unrolled
		euclideanDistanceFloat64Impl = euclideanFloat64Unrolled4x
		cosineDistanceFloat64Impl = cosineFloat64Unrolled4x
		euclideanDistanceInt8Impl = euclideanInt8Unrolled4x
		euclideanDistanceUint8Impl = euclideanUint8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16Unrolled4x
		euclideanDistanceUint16Impl = euclideanUint16Unrolled4x
		dotProductInt8Impl = dotInt8Unrolled4x
		dotProductUint8Impl = dotUint8Unrolled4x
		dotProductInt16Impl = dotInt16Unrolled4x
		dotProductUint16Impl = dotUint16Unrolled4x
		dotProductInt4Impl = dotInt4Generic
		dotProductInt2Impl = dotInt2Generic
		memcpyNTAImpl = memcpyGeneric

		int8ToFloat32Impl = dispatch.Int8ToFloat32
		uint8ToFloat32Impl = dispatch.Uint8ToFloat32
		int16ToFloat32Impl = dispatch.Int16ToFloat32
		uint16ToFloat32Impl = dispatch.Uint16ToFloat32
		int32ToFloat32Impl = dispatch.Int32ToFloat32
		uint32ToFloat32Impl = dispatch.Uint32ToFloat32
		float16ToFloat32Impl = dispatch.Float16ToFloat32

		sigmoidFloat32Impl = dispatch.Sigmoid
		softmaxFloat32Impl = dispatch.Softmax
		expFloat32Impl = dispatch.Exp
		logFloat32Impl = dispatch.Log

		sumFloat32Impl = dispatch.Sum
		maxFloat32Impl = dispatch.Max
		minFloat32Impl = dispatch.Min
		matMulFloat32Impl = dispatch.MatMul
		sinFloat32Impl = dispatch.Sin
		cosFloat32Impl = dispatch.Cos
		atan2Float32Impl = dispatch.Atan2
		argMaxFloat32Impl = dispatch.ArgMax
		argMinFloat32Impl = dispatch.ArgMin
		manhattanDistanceImpl = dispatch.ManhattanDistance
		chebyshevDistanceImpl = dispatch.ChebyshevDistance
		brayCurtisDistanceImpl = dispatch.BrayCurtisDistance
		accumulateWeightedScatterFloat32Impl = dispatch.AccumulateWeightedScatter
		haversineBatchImpl = dispatch.HaversineBatch
		unpackTQ2Impl = dispatch.UnpackTQ2
		unpackTQ4Impl = dispatch.UnpackTQ4
		unpackTQ8Impl = dispatch.UnpackTQ8
		packTQ2Impl = dispatch.PackTQ2
		packTQ4Impl = dispatch.PackTQ4
		packTQ8Impl = dispatch.PackTQ8
	}

	// Register current implementations into the new dynamic registry.
	// This enables the transition to polymorphic indexing while preserving
	// existing high-performance paths.

	// Float32 Euclidean
	Registry.Register(MetricEuclidean, DataTypeFloat32, 0, euclideanDistanceImpl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 128, euclideanDistance128Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 384, euclideanDistance384Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 768, euclideanDistance768Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 1024, euclideanDistance1024Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 1536, euclideanDistance1536Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 3072, euclideanDistance3072Impl)

	// Float32 Cosine & Dot Product
	Registry.Register(MetricCosine, DataTypeFloat32, 0, cosineDistanceImpl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 0, dotProductImpl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 128, dotProduct128Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 384, dotProduct384Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 768, dotProduct768Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 1024, dotProduct1024Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 1536, dotProduct1536Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 3072, dotProduct3072Impl)

	// Float16 (Support both native and unrolled paths)
	Registry.Register(MetricEuclidean, DataTypeFloat16, 0, euclideanDistanceF16Impl)
	Registry.Register(MetricCosine, DataTypeFloat16, 0, cosineDistanceF16Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat16, 0, dotProductF16Impl)

	// Complex Numbers (Unrolled Baselines)
	Registry.Register(MetricEuclidean, DataTypeComplex64, 0, euclideanDistanceComplex64Impl)
	Registry.Register(MetricEuclidean, DataTypeComplex128, 0, euclideanDistanceComplex128Impl)

	// Baseline Fallbacks for all other types
	Registry.Register(MetricEuclidean, DataTypeInt8, 0, euclideanDistanceInt8Impl)
	Registry.Register(MetricCosine, DataTypeInt8, 0, CosineDistanceInt8)
	Registry.Register(MetricDotProduct, DataTypeInt8, 0, dotProductInt8Impl)

	Registry.Register(MetricEuclidean, DataTypeInt16, 0, euclideanDistanceInt16Impl)
	Registry.Register(MetricCosine, DataTypeInt16, 0, CosineDistanceInt16)
	Registry.Register(MetricDotProduct, DataTypeInt16, 0, dotProductInt16Impl)

	Registry.Register(MetricEuclidean, DataTypeInt32, 0, euclideanInt32Unrolled4x)
	Registry.Register(MetricCosine, DataTypeInt32, 0, CosineDistanceInt32)
	Registry.Register(MetricDotProduct, DataTypeInt32, 0, dotInt32Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeInt64, 0, euclideanInt64Unrolled4x)
	Registry.Register(MetricCosine, DataTypeInt64, 0, CosineDistanceInt64)
	Registry.Register(MetricDotProduct, DataTypeInt64, 0, dotInt64Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeUint8, 0, euclideanDistanceUint8Impl)
	Registry.Register(MetricEuclidean, DataTypeUint8, 384, Euclidean384Uint8)
	Registry.Register(MetricEuclidean, DataTypeUint8, 768, Euclidean768Uint8)
	Registry.Register(MetricEuclidean, DataTypeUint8, 1024, Euclidean1024Uint8)
	Registry.Register(MetricCosine, DataTypeUint8, 0, CosineDistanceUint8)
	Registry.Register(MetricDotProduct, DataTypeUint8, 0, dotProductUint8Impl)
	Registry.Register(MetricDotProduct, DataTypeUint8, 384, Dot384Uint8)
	Registry.Register(MetricDotProduct, DataTypeUint8, 768, Dot768Uint8)
	Registry.Register(MetricDotProduct, DataTypeUint8, 1024, Dot1024Uint8)

	Registry.Register(MetricEuclidean, DataTypeUint16, 0, euclideanDistanceUint16Impl)
	Registry.Register(MetricEuclidean, DataTypeUint16, 384, Euclidean384Uint16)
	Registry.Register(MetricEuclidean, DataTypeUint16, 768, Euclidean768Uint16)
	Registry.Register(MetricEuclidean, DataTypeUint16, 1024, Euclidean1024Uint16)
	Registry.Register(MetricCosine, DataTypeUint16, 0, CosineDistanceUint16)
	Registry.Register(MetricDotProduct, DataTypeUint16, 0, dotProductUint16Impl)
	Registry.Register(MetricDotProduct, DataTypeUint16, 384, Dot384Uint16)
	Registry.Register(MetricDotProduct, DataTypeUint16, 768, Dot768Uint16)
	Registry.Register(MetricDotProduct, DataTypeUint16, 1024, Dot1024Uint16)

	Registry.Register(MetricEuclidean, DataTypeUint32, 0, euclideanUint32Unrolled4x)
	Registry.Register(MetricEuclidean, DataTypeUint32, 384, Euclidean384Uint32)
	Registry.Register(MetricEuclidean, DataTypeUint32, 768, Euclidean768Uint32)
	Registry.Register(MetricEuclidean, DataTypeUint32, 1024, Euclidean1024Uint32)
	Registry.Register(MetricCosine, DataTypeUint32, 0, CosineDistanceUint32)
	Registry.Register(MetricDotProduct, DataTypeUint32, 0, dotUint32Unrolled4x)
	Registry.Register(MetricDotProduct, DataTypeUint32, 384, Dot384Uint32)
	Registry.Register(MetricDotProduct, DataTypeUint32, 768, Dot768Uint32)
	Registry.Register(MetricDotProduct, DataTypeUint32, 1024, Dot1024Uint32)

	Registry.Register(MetricEuclidean, DataTypeUint64, 0, euclideanUint64Unrolled4x)
	Registry.Register(MetricEuclidean, DataTypeUint64, 384, Euclidean384Uint64)
	Registry.Register(MetricEuclidean, DataTypeUint64, 768, Euclidean768Uint64)
	Registry.Register(MetricEuclidean, DataTypeUint64, 1024, Euclidean1024Uint64)
	Registry.Register(MetricCosine, DataTypeUint64, 0, CosineDistanceUint64)
	Registry.Register(MetricDotProduct, DataTypeUint64, 0, dotUint64Unrolled4x)
	Registry.Register(MetricDotProduct, DataTypeUint64, 384, Dot384Uint64)
	Registry.Register(MetricDotProduct, DataTypeUint64, 768, Dot768Uint64)
	Registry.Register(MetricDotProduct, DataTypeUint64, 1024, Dot1024Uint64)

	Registry.Register(MetricEuclidean, DataTypeFloat64, 0, EuclideanDistanceFloat64)
	Registry.Register(MetricCosine, DataTypeFloat64, 0, CosineDistanceFloat64)
	Registry.Register(MetricDotProduct, DataTypeFloat64, 0, DotProductF64)
	Registry.Register(MetricL2Squared, DataTypeFloat64, 0, L2SquaredFloat64)

	Registry.Register(MetricEuclidean, DataTypeComplex64, 0, euclideanComplex64Unrolled)
	Registry.Register(MetricCosine, DataTypeComplex64, 0, CosineDistanceComplex64)
	Registry.Register(MetricDotProduct, DataTypeComplex64, 0, dotComplex64Unrolled)
	// L2Squared (Polymorphic — generic + dimension-specialized)
	Registry.Register(MetricL2Squared, DataTypeFloat32, 0,    l2SquaredImpl)
	Registry.Register(MetricL2Squared, DataTypeFloat32, 128,  l2Squared128Impl)
	Registry.Register(MetricL2Squared, DataTypeFloat32, 384,  l2Squared384Impl)
	Registry.Register(MetricL2Squared, DataTypeFloat32, 768,  l2Squared768Impl)
	Registry.Register(MetricL2Squared, DataTypeFloat32, 1024, l2Squared1024Impl)
	Registry.Register(MetricL2Squared, DataTypeFloat32, 3072, l2Squared3072Impl)
	Registry.Register(MetricL2Squared, DataTypeInt8,  0, l2SquaredInt8Unrolled4x)
	Registry.Register(MetricL2Squared, DataTypeUint8, 0, l2SquaredUint8Unrolled4x)


	Registry.Register(MetricEuclidean, DataTypeComplex128, 0, euclideanComplex128Unrolled)
	Registry.Register(MetricCosine, DataTypeComplex128, 0, CosineDistanceComplex128)
	Registry.Register(MetricDotProduct, DataTypeComplex128, 0, dotComplex128Unrolled)
}

// DispatchDistance computes the distance between two vectors using the best available kernel.
func DispatchDistance[T any](metric MetricType, a, b []T) (float32, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("simd: dimension mismatch: %d != %d", len(a), len(b))
	}
	if len(a) == 0 {
		return 0, nil
	}

	dt := GetDataType[T]()
	dims := len(a)

	kernel := Registry.Get(metric, dt, dims)
	if kernel == nil {
		return 0, fmt.Errorf("simd: no kernel found for %s/%s dims=%d", metric, dt, dims)
	}

	start := time.Now()
	defer func() {
		metrics.HNSWSimdDispatchLatency.WithLabelValues(dt.String()).Observe(time.Since(start).Seconds())
	}()

	switch k := kernel.(type) {
	case func([]T, []T) (float32, error):
		if k == nil {
			return 0, fmt.Errorf("simd: kernel function is nil for metric %s", metric)
		}
		return k(a, b)
	case distanceFunc:
		if k == nil {
			return 0, fmt.Errorf("simd: distanceFunc kernel is nil for metric %s", metric)
		}
		if va, ok := any(a).([]float32); ok {
			vb := any(b).([]float32)
			return k(va, vb)
		}
	case distanceF16Func:
		if k == nil {
			return 0, fmt.Errorf("simd: distanceF16Func kernel is nil for metric %s", metric)
		}
		if va, ok := any(a).([]float16.Num); ok {
			vb := any(b).([]float16.Num)
			return k(va, vb)
		}
	default:
		return 0, fmt.Errorf("simd: invalid kernel type for %s: %T", dt, kernel)
	}
	return 0, fmt.Errorf("simd: type mismatch between T and kernel for %s", dt)
}

// GetDataType returns the DataType for a given type T.
func GetDataType[T any]() DataType {
	var zero T
	switch any(zero).(type) {
	case float32:
		return DataTypeFloat32
	case float16.Num:
		return DataTypeFloat16
	case int8:
		return DataTypeInt8
	case uint8:
		return DataTypeUint8
	case int16:
		return DataTypeInt16
	case uint16:
		return DataTypeUint16
	case int32:
		return DataTypeInt32
	case uint32:
		return DataTypeUint32
	case int64:
		return DataTypeInt64
	case uint64:
		return DataTypeUint64
	case float64:
		return DataTypeFloat64
	case complex64:
		return DataTypeComplex64
	case complex128:
		return DataTypeComplex128
	}
	return DataTypeFloat32 // Default fallback
}
