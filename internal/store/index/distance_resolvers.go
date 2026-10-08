package index

import (
	"log/slog"
	"math"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func getSimdMetric(m basecore.DistanceMetric) simd.MetricType {
	switch m {
	case basecore.MetricCosine:
		return simd.MetricCosine
	case basecore.MetricDotProduct:
		return simd.MetricDotProduct
	case basecore.MetricL2Squared:
		return simd.MetricL2Squared
	default:
		return simd.MetricEuclidean
	}
}

type distanceFallbacks[T any] struct {
	cosine    simd.DistanceKernel[T]
	dot       simd.DistanceKernel[T]
	l2Squared simd.DistanceKernel[T]
	euclidean simd.DistanceKernel[T]
}

// resolveDistanceKernel picks the fastest kernel that provably agrees with the
// scalar reference, and reports what it chose.
//
// elementType is a label, not a type parameter: it names the element type in
// metrics and logs so a fallback can be attributed to a specific dtype. That
// attribution is the whole point - the symptom of a silent fallback is a dtype
// sitting far below its siblings at identical element count, which is otherwise
// indistinguishable from that dtype simply being slower.
//
// The two rejection reasons are deliberately distinct:
//
//   - `unavailable`: no kernel exists for this type/metric/dims. Routine, and
//     usually correct.
//   - `mismatch`: a kernel exists but disagrees with the scalar reference. This is
//     a real defect in the kernel, and it is the one that was invisible.
func resolveDistanceKernel[T any](sm simd.MetricType, dims int, fb distanceFallbacks[T], elementType string) simd.DistanceKernel[T] {
	k := simd.GetKernel[T](sm, dims)

	reason := ""
	if k != nil && !kernelMatchesReference(k, sm, fb, dims) {
		// A kernel that does not reproduce the scalar reference would feed
		// wrong distances into neighbour selection and into the candidate
		// ordering of the search, which silently corrupts the graph instead
		// of failing loudly. Prefer the slower scalar kernel.
		reason = "mismatch"
		k = nil
	} else if k == nil {
		reason = "unavailable"
	}

	if reason != "" {
		metrics.HNSWSIMDKernelFallbacksTotal.WithLabelValues(elementType, sm.String(), reason).Inc()
		slog.Warn("distance kernel rejected, falling back to scalar",
			"element_type", elementType,
			"metric", sm.String(),
			"dims", dims,
			"reason", reason)
	}
	if k != nil {
		metrics.HNSWSIMDKernelResolvedTotal.WithLabelValues(elementType, sm.String(), "simd").Inc()
	} else {
		metrics.HNSWSIMDKernelResolvedTotal.WithLabelValues(elementType, sm.String(), "scalar").Inc()
	}

	if k == nil {
		switch sm {
		case simd.MetricCosine:
			return fb.cosine
		case simd.MetricDotProduct:
			if fb.dot != nil {
				return func(a, b []T) (float32, error) {
					d, err := fb.dot(a, b)
					return -d, err
				}
			}
			return nil
		case simd.MetricL2Squared:
			if fb.l2Squared != nil {
				return fb.l2Squared
			}
			return fb.euclidean
		default:
			return fb.euclidean
		}
	}
	if sm == simd.MetricDotProduct {
		return func(a, b []T) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// kernelMatchesReference reports whether a resolved SIMD kernel agrees with the
// scalar reference kernel for the same metric on a probe pair of the requested
// width. Kernels are specialised for one element type and one dimension, and a
// kernel that is wrong for its type - an unsigned one that differences before
// widening, say - returns plausible-looking distances that are off by many
// orders of magnitude, which is far worse than falling back to the scalar path.
func kernelMatchesReference[T any](k simd.DistanceKernel[T], sm simd.MetricType, fb distanceFallbacks[T], dims int) bool {
	if dims <= 0 {
		return true
	}
	var ref simd.DistanceKernel[T]
	switch sm {
	case simd.MetricCosine:
		ref = fb.cosine
	case simd.MetricDotProduct:
		ref = fb.dot
	case simd.MetricL2Squared:
		ref = fb.l2Squared
		if ref == nil {
			ref = fb.euclidean
		}
	default:
		ref = fb.euclidean
	}
	if ref == nil {
		// Nothing to check against; trust the kernel.
		return true
	}

	// a < b element-wise, so a kernel that differences before widening wraps
	// around for unsigned types instead of going negative.
	a := make([]T, dims)
	b := make([]T, dims)
	fillProbe(a, 1)
	fillProbe(b, 3)

	got, gotErr := k(a, b)
	want, wantErr := ref(a, b)
	if gotErr != nil || wantErr != nil {
		return false
	}
	gotF := float64(got)
	if math.IsNaN(gotF) || math.IsInf(gotF, 0) {
		return false
	}
	scale := math.Max(math.Abs(float64(want)), 1)
	return math.Abs(gotF-float64(want)) <= 1e-3*scale
}

// fillProbe writes base, base+1, ... into a numeric slice. A type parameter
// cannot be converted from an int (and float16.Num is not numeric at all), so
// the concrete element types are spelled out once here.
func fillProbe[T any](dst []T, base int) {
	for i := range dst {
		v := base + i
		switch p := any(&dst[i]).(type) {
		case *float32:
			*p = float32(v)
		case *float64:
			*p = float64(v)
		case *int8:
			*p = int8(v) // #nosec G115 -- base is 1 or 3 and i < dims
		case *int16:
			*p = int16(v) // #nosec G115 -- base is 1 or 3 and i < dims
		case *int32:
			*p = int32(v) // #nosec G115 -- base is 1 or 3 and i < dims
		case *int64:
			*p = int64(v) // #nosec G115 -- base is 1 or 3 and i < dims
		case *uint8:
			*p = uint8(v) // #nosec G115 -- base is 1 or 3 and i < dims
		case *uint16:
			*p = uint16(v) // #nosec G115 -- base is 1 or 3 and i < dims
		case *uint32:
			*p = uint32(v) // #nosec G115 -- base is 1 or 3 and i < dims
		case *uint64:
			*p = uint64(v) // #nosec G115 -- base is 1 or 3 and i < dims
		case *complex64:
			*p = complex(float32(v), float32(v))
		case *complex128:
			*p = complex(float64(v), float64(v))
		case *float16.Num:
			*p = float16.New(float32(v))
		}
	}
}

func resolveL2SquaredKernel[T any](dims int, fallback simd.DistanceKernel[T]) simd.DistanceKernel[T] {
	if k := simd.GetKernel[T](simd.MetricL2Squared, dims); k != nil {
		return k
	}
	return fallback
}

func (h *ArrowHNSW) resolveAllDistanceFuncs() {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())

	h.distFunc = resolveDistanceKernel(sm, dims, distanceFallbacks[float32]{
		cosine: simd.CosineDistance, dot: simd.DotProduct,
		l2Squared: simd.L2SquaredFloat32, euclidean: simd.EuclideanDistance,
	}, "float32")
	h.distFuncSquared = resolveL2SquaredKernel(dims, simd.L2SquaredFloat32)
	h.distFuncF16 = resolveDistanceKernel(sm, dims, distanceFallbacks[float16.Num]{
		cosine: simd.CosineDistanceF16, dot: simd.DotProductF16,
		euclidean: simd.EuclideanDistanceF16,
	}, "float16")
	h.distFuncF64 = resolveDistanceKernel(sm, dims, distanceFallbacks[float64]{
		cosine: simd.CosineDistanceFloat64, dot: simd.DotProductF64,
		l2Squared: simd.L2SquaredFloat64, euclidean: simd.EuclideanDistanceFloat64,
	}, "float64")
	h.distFuncC64 = resolveDistanceKernel(sm, dims, distanceFallbacks[complex64]{
		cosine: simd.CosineDistanceComplex64, dot: simd.DotProductComplex64,
		euclidean: simd.EuclideanDistanceComplex64,
	}, "complex64")
	h.distFuncC128 = resolveDistanceKernel(sm, dims, distanceFallbacks[complex128]{
		cosine: simd.CosineDistanceComplex128, dot: simd.DotProductComplex128,
		euclidean: simd.EuclideanDistanceComplex128,
	}, "complex128")
	h.distFuncInt8 = resolveDistanceKernel(sm, dims, distanceFallbacks[int8]{
		cosine: simd.CosineDistanceInt8, dot: simd.DotProductInt8,
		euclidean: simd.EuclideanDistanceInt8,
	}, "int8")
	h.distFuncInt8Squared = resolveL2SquaredKernel[int8](dims, nil)
	h.distFuncUint8 = resolveDistanceKernel(sm, dims, distanceFallbacks[uint8]{
		cosine: simd.CosineDistanceUint8, dot: simd.DotProductUint8,
		// The unsigned reference differences in its own width, which wraps
		// around; use the widening implementation in this package.
		euclidean: func(a, b []uint8) (float32, error) { return euclideanDistanceUint8(a, b), nil },
	}, "uint8")
	h.distFuncUint8Squared = resolveL2SquaredKernel[uint8](dims, nil)
	h.distFuncInt16 = resolveDistanceKernel(sm, dims, distanceFallbacks[int16]{
		cosine: simd.CosineDistanceInt16, dot: simd.DotProductInt16,
		euclidean: simd.EuclideanDistanceInt16,
	}, "int16")
	h.distFuncUint16 = resolveDistanceKernel(sm, dims, distanceFallbacks[uint16]{
		cosine: simd.CosineDistanceUint16, dot: simd.DotProductUint16,
		euclidean: simd.EuclideanDistanceUint16,
	}, "uint16")
	h.distFuncInt32 = resolveDistanceKernel(sm, dims, distanceFallbacks[int32]{
		cosine: simd.CosineDistanceInt32, dot: simd.DotProductInt32,
		euclidean: simd.EuclideanDistanceInt32,
	}, "int32")
	h.distFuncUint32 = resolveDistanceKernel(sm, dims, distanceFallbacks[uint32]{
		cosine: simd.CosineDistanceUint32, dot: simd.DotProductUint32,
		euclidean: simd.EuclideanDistanceUint32,
	}, "uint32")
	h.distFuncInt64 = resolveDistanceKernel(sm, dims, distanceFallbacks[int64]{
		cosine: simd.CosineDistanceInt64, dot: simd.DotProductInt64,
		euclidean: simd.EuclideanDistanceInt64,
	}, "int64")
	h.distFuncUint64 = resolveDistanceKernel(sm, dims, distanceFallbacks[uint64]{
		cosine: simd.CosineDistanceUint64, dot: simd.DotProductUint64,
		// The unsigned reference differences in its own width, which wraps
		// around; use the widening implementation in this package.
		euclidean: func(a, b []uint64) (float32, error) { return euclideanDistanceUint64(a, b), nil },
	}, "uint64")

	if h.navigator != nil {
		h.navigator.SetDistanceKernel(h.distFunc)
	}
}
