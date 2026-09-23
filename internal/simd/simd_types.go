package simd

import (
	"errors"
	"unsafe"

	lbcore "github.com/23skdu/longbow/internal/core"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

var (
	// ErrDimensionMismatch is returned when input vectors have different lengths.
	ErrDimensionMismatch = errors.New("simd: vector dimension mismatch")
	// ErrInitializationFailed is returned when SIMD dispatch or JIT fails to initialize.
	ErrInitializationFailed = errors.New("simd: initialization failed")
)

// Function pointer types for dispatch
type (
	distanceFunc          func(a, b []float32) (float32, error)
	distanceBatchFunc     func(query []float32, vectors [][]float32, results []float32) error
	distanceBatchFlatFunc func(query []float32, flatVectors []float32, numVectors, dims int, results []float32) error
	distanceSQ8BatchFunc  func(query []byte, vectors [][]byte, results []float32) error
	distanceF16BatchFunc  func(query []float16.Num, vectors [][]float16.Num, results []float32) error
	adcDistanceBatchFunc  func(table []float32, flatCodes []byte, m int, results []float32) error

	distanceF16Func func(a, b []float16.Num) (float32, error)

	distanceComplex64Func  func(a, b []complex64) (float32, error)
	distanceComplex128Func func(a, b []complex128) (float32, error)
	distanceFloat64Func    func(a, b []float64) (float32, error)

	// DistanceKernel is a generic distance function type for cached kernels.
	DistanceKernel[T any] func(a, b []T) (float32, error)

	// CompareOp represents a comparison operator for SIMD filters
	CompareOp int
)

const (
	// CompareEq represents equality (==).
	CompareEq CompareOp = iota
	// CompareNeq represents inequality (!=).
	CompareNeq
	// CompareGt represents greater than (>).
	CompareGt
	// CompareGe represents greater than or equal to (>=).
	CompareGe
	// CompareLt represents less than (<).
	CompareLt
	// CompareLe represents less than or equal to (<=).
	CompareLe
)

type (
	matchInt64Func   func(src []int64, val int64, op CompareOp, dst []byte) error
	matchInt32Func   func(src []int32, val int32, op CompareOp, dst []byte) error
	matchFloat32Func func(src []float32, val float32, op CompareOp, dst []byte) error
	matchFloat64Func func(src []float64, val float64, op CompareOp, dst []byte) error
)

var (

	// Function pointers initialized at startup - eliminates switch overhead in hot path
	euclideanDistanceImpl     distanceFunc
	euclideanDistance128Impl  distanceFunc // optimized for dimensions=128
	euclideanDistance384Impl  distanceFunc // optimized for dimensions=384
	euclideanDistance768Impl  distanceFunc // optimized for dimensions=768
	euclideanDistance1024Impl distanceFunc // optimized for dimensions=1024
	euclideanDistance1536Impl distanceFunc // optimized for dimensions=1536
	euclideanDistance3072Impl distanceFunc // optimized for dimensions=3072
	cosineDistanceImpl        distanceFunc

	// DistFunc is the best available Euclidean distance implementation
	DistFunc distanceFunc

	dotProductImpl             distanceFunc
	dotProduct384Impl          distanceFunc
	dotProduct768Impl          distanceFunc
	dotProduct1536Impl         distanceFunc
	dotProduct128Impl          distanceFunc // optimized for dimensions=128
	dotProduct1024Impl         distanceFunc // optimized for dimensions=1024
	dotProduct3072Impl         distanceFunc // optimized for dimensions=3072
	euclideanDistanceBatchImpl distanceBatchFunc
	cosineDistanceBatchImpl    distanceBatchFunc
	dotProductBatchImpl        distanceBatchFunc
	l2SquaredImpl              distanceFunc
	l2Squared128Impl           distanceFunc // optimized for dimensions=128
	l2Squared384Impl           distanceFunc // optimized for dimensions=384
	l2Squared768Impl           distanceFunc // optimized for dimensions=768
	l2Squared1024Impl          distanceFunc // optimized for dimensions=1024
	l2Squared3072Impl          distanceFunc // optimized for dimensions=3072

	matchInt64Impl   matchInt64Func
	matchInt32Impl   matchInt32Func
	matchFloat32Impl matchFloat32Func
	matchFloat64Impl matchFloat64Func

	adcDistanceBatchImpl               adcDistanceBatchFunc
	euclideanDistanceVerticalBatchImpl distanceBatchFunc
	euclideanDistanceSQ8BatchImpl      distanceSQ8BatchFunc
	euclideanDistanceF16BatchImpl      distanceF16BatchFunc

	// Bitwise operations
	andBytesImpl   func(dst, src []byte)
	orBytesImpl    func(dst, src []byte)
	notBytesImpl   func(dst []byte)
	isAllZerosImpl func(src []byte) bool

	euclideanDistanceF16Impl distanceF16Func
	cosineDistanceF16Impl    distanceF16Func
	dotProductF16Impl        distanceF16Func

	manhattanDistanceImpl  distanceFunc
	chebyshevDistanceImpl  distanceFunc
	brayCurtisDistanceImpl distanceFunc

	euclideanDistanceComplex64Impl  distanceComplex64Func
	euclideanDistanceComplex128Impl distanceComplex128Func
	euclideanDistanceFloat64Impl    distanceFloat64Func
	cosineDistanceFloat64Impl       distanceFloat64Func
	dotProductFloat64Impl           distanceFloat64Func
	l2SquaredFloat64Impl            distanceFloat64Func

	euclideanDistanceInt8Impl   func(a, b []int8) (float32, error)
	l2SquaredInt8Impl           func(a, b []int8) (float32, error)
	euclideanDistanceUint8Impl  func(a, b []uint8) (float32, error)
	euclideanDistanceInt16Impl  func(a, b []int16) (float32, error)
	euclideanDistanceUint16Impl func(a, b []uint16) (float32, error)

	dotProductInt8Impl       func(a, b []int8) (float32, error)
	cosineDistanceInt8Impl   func(a, b []int8) (float32, error)
	cosineDistanceUint8Impl  func(a, b []uint8) (float32, error)
	cosineDistanceInt16Impl  func(a, b []int16) (float32, error)
	cosineDistanceUint16Impl func(a, b []uint16) (float32, error)

	euclideanDistanceInt32Impl  func(a, b []int32) (float32, error)
	dotProductInt32Impl         func(a, b []int32) (float32, error)
	cosineDistanceInt32Impl     func(a, b []int32) (float32, error)

	euclideanDistanceUint32Impl  func(a, b []uint32) (float32, error)
	dotProductUint32Impl         func(a, b []uint32) (float32, error)
	cosineDistanceUint32Impl     func(a, b []uint32) (float32, error)

	euclideanDistanceInt64Impl  func(a, b []int64) (float32, error)
	dotProductInt64Impl         func(a, b []int64) (float32, error)
	cosineDistanceInt64Impl     func(a, b []int64) (float32, error)

	euclideanDistanceUint64Impl  func(a, b []uint64) (float32, error)
	dotProductUint64Impl         func(a, b []uint64) (float32, error)
	cosineDistanceUint64Impl     func(a, b []uint64) (float32, error)

	dotProductUint8Impl      func(a, b []uint8) (float32, error)
	dotProductInt16Impl      func(a, b []int16) (float32, error)
	dotProductUint16Impl     func(a, b []uint16) (float32, error)

	dotProductInt4Impl       func(a, b []byte) (float32, error)
	dotProductInt2Impl       func(a, b []byte) (float32, error)

	memcpyNTAImpl func(dst, src unsafe.Pointer, n int)

	// Type conversion kernels
	int8ToFloat32Impl    func(src []int8, dst []float32)
	uint8ToFloat32Impl   func(src []uint8, dst []float32)
	int16ToFloat32Impl   func(src []int16, dst []float32)
	uint16ToFloat32Impl  func(src []uint16, dst []float32)
	int32ToFloat32Impl   func(src []int32, dst []float32)
	uint32ToFloat32Impl  func(src []uint32, dst []float32)
	float16ToFloat32Impl func(src []float16.Num, dst []float32)

	// Activation kernels
	sigmoidFloat32Impl func(src, dst []float32)
	softmaxFloat32Impl func(src, dst []float32)
	expFloat32Impl     func(src, dst []float32)
	logFloat32Impl     func(src, dst []float32)

	sumFloat32Impl                       func(src []float32) float32
	maxFloat32Impl                       func(src []float32) float32
	minFloat32Impl                       func(src []float32) float32
	argMaxFloat32Impl                    func(src []float32) int
	argMinFloat32Impl                    func(src []float32) int
	matMulFloat32Impl                    func(a, b []float32, m, n, k int, dst []float32)
	accumulateWeightedScatterFloat32Impl func(dst []float32, targets []uint32, weights []float32, factor float32)
	haversineBatchImpl                   haversineBatchFunc

	// Transcendental kernels
	sinFloat32Impl    func(src, dst []float32)
	cosFloat32Impl    func(src, dst []float32)
	sincosFloat32Impl func(src, sinDst, cosDst []float32)
	sqrtFloat32Impl   func(src, dst []float32)
	atan2Float32Impl  func(y, x, dst []float32)

	pauseImpl func()

	// TurboQuant kernels
	unpackTQ2Impl func(src []byte, dst []float32, scale, bias float32)
	unpackTQ4Impl func(src []byte, dst []float32, scale, bias float32)
	unpackTQ8Impl func(src []byte, dst []float32, scale, bias float32)
	packTQ2Impl   func(src []float32, dst []byte)
	packTQ4Impl   func(src []float32, dst []byte)
	packTQ8Impl   func(src []float32, dst []byte)
)

type haversineBatchFunc func(centerLat, centerLon float64, points []lbcore.GeoPoint, earthRadius float64, results []float32)
