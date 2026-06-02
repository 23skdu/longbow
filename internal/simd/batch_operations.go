package simd

import (
	"errors"
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// =============================================================================
// Batch Operations
// =============================================================================

// EuclideanDistanceF16Batch computes Euclidean distances between one query and multiple Float16 vectors.
func EuclideanDistanceF16Batch(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	return euclideanDistanceF16BatchImpl(query, vectors, results)
}

// EuclideanDistanceBatch computes Euclidean distances between one query and multiple vectors.
// Uses unified dispatch table for optimal cache locality and reduced dispatch overhead.
func EuclideanDistanceBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}

	metrics.SimdDispatchTotal.WithLabelValues(implementation, "euclidean_batch").Inc()

	// Special handling for common dimensions to bypass generic batch overhead if possible
	dims := len(query)
	switch dims {
	case 128, 384, 768:
		euclideanDistanceBatch4Way(query, vectors, results, dims, false)
		return nil
	case 1024:
		for i, v := range vectors {
			if v == nil || len(v) != 1024 {
				results[i] = math.MaxFloat32
				continue
			}
			d, _ := currentDispatch.EuclideanDistance1024(query, v)
			results[i] = d
		}
		return nil
	case 3072:
		for i, v := range vectors {
			if v == nil || len(v) != 3072 {
				results[i] = math.MaxFloat32
				continue
			}
			d, _ := currentDispatch.EuclideanDistance3072(query, v)
			results[i] = d
		}
		return nil
	}

	return currentDispatch.EuclideanDistanceBatch(query, vectors, results)
}

// EuclideanDistanceBatchFlat computes distances against a flat array of vectors.
// Vectors are stored contiguously: [v1[0], v1[1], ..., v1[dims], v2[0], ...]
func EuclideanDistanceBatchFlat(query, flatVectors []float32, numVectors, dims int, results []float32) error {
	if numVectors == 0 {
		return nil
	}
	if currentDispatch == nil || currentDispatch.EuclideanDistanceBatchFlat == nil {
		return errors.New("simd: dispatch not initialized")
	}
	switch dims {
	case 128:
		for i := 0; i < numVectors; i++ {
			offset := i * 128
			v := flatVectors[offset : offset+128]
			d, _ := currentDispatch.EuclideanDistance128(query, v)
			results[i] = d
		}
		return nil
	case 384:
		for i := 0; i < numVectors; i++ {
			offset := i * 384
			v := flatVectors[offset : offset+384]
			d, _ := currentDispatch.EuclideanDistance384(query, v)
			results[i] = d
		}
		return nil
	case 768:
		for i := 0; i < numVectors; i++ {
			offset := i * 768
			v := flatVectors[offset : offset+768]
			d, _ := currentDispatch.EuclideanDistance768(query, v)
			results[i] = d
		}
		return nil
	case 1024:
		for i := 0; i < numVectors; i++ {
			offset := i * 1024
			v := flatVectors[offset : offset+1024]
			d, _ := currentDispatch.EuclideanDistance1024(query, v)
			results[i] = d
		}
		return nil
	case 3072:
		for i := 0; i < numVectors; i++ {
			offset := i * 3072
			v := flatVectors[offset : offset+3072]
			d, _ := currentDispatch.EuclideanDistance3072(query, v)
			results[i] = d
		}
		return nil
	}
	return currentDispatch.EuclideanDistanceBatchFlat(query, flatVectors, numVectors, dims, results)
}

// EuclideanDistanceVerticalBatch optimally calculates distances for multiple vectors at once.
// Uses vertical processing to maximize SIMD register utilization.
func EuclideanDistanceVerticalBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	dims := len(query)
	if dims == 384 || dims == 128 {
		// Fallback to regular batch for special dimensions
		return EuclideanDistanceBatch(query, vectors, results)
	}
	return euclideanDistanceVerticalBatchImpl(query, vectors, results)
}

// EuclideanDistanceSQ8Batch computes Euclidean distances for SQ8 quantized vectors.
func EuclideanDistanceSQ8Batch(query []byte, vectors [][]byte, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	return euclideanDistanceSQ8BatchImpl(query, vectors, results)
}

// ADCDistanceBatch computes Asymmetric Distance Computation distances.
// Uses pre-computed distance tables for PQ code lookups.
func ADCDistanceBatch(table []float32, flatCodes []byte, m int, results []float32) error {
	if len(table) == 0 || len(flatCodes) == 0 {
		return errors.New("simd: empty table or codes")
	}
	if m <= 0 {
		return errors.New("simd: invalid m parameter")
	}
	return adcDistanceBatchImpl(table, flatCodes, m, results)
}

// CosineDistanceBatch computes cosine distances between query and multiple vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func CosineDistanceBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(vectors) == 0 {
		return nil
	}
	if len(results) < len(vectors) {
		return errors.New("simd: results slice too small")
	}
	metrics.CosineBatchCallsTotal.Inc()
	metrics.ParallelReductionVectorsProcessed.Add(float64(len(vectors)))
	metrics.SimdDispatchTotal.WithLabelValues(implementation, "cosine_batch").Inc()
	_ = cosineDistanceBatchImpl(query, vectors, results)
	return nil
}

// DotProductBatch calculates dot product between query and multiple vectors.
// Uses parallel sum reduction with multiple accumulators for ILP optimization.
func DotProductBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(vectors) == 0 {
		return nil
	}
	if len(results) < len(vectors) {
		return errors.New("simd: results slice too small")
	}
	metrics.DotProductBatchCallsTotal.Inc()
	metrics.ParallelReductionVectorsProcessed.Add(float64(len(vectors)))
	metrics.SimdDispatchTotal.WithLabelValues(implementation, "dot_batch").Inc()

	dims := len(query)
	switch dims {
	case 128, 384, 768:
		dotProductBatch4Way(query, vectors, results, dims)
		return nil
	}

	_ = dotProductBatchImpl(query, vectors, results)
	return nil
}

// ToFloat32 converts a float64 slice to a float32 slice (Allocates).
func ToFloat32(v []float64) []float32 {
	res := make([]float32, len(v))
	for i, val := range v {
		res[i] = float32(val)
	}
	return res
}

func float32SliceToBytes(vec []float32) []byte {
	if len(vec) == 0 {
		return nil
	}
	size := len(vec) * 4
	ptr := unsafe.Pointer(&vec[0])          // #nosec G103
	return unsafe.Slice((*byte)(ptr), size) // #nosec G103
}

// L2SquaredDistanceBatch computes L2 squared distances between one query and multiple vectors.
func L2SquaredDistanceBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	metrics.SimdDispatchTotal.WithLabelValues(implementation, "l2squared_batch").Inc()
	dims := len(query)
	switch dims {
	case 128, 384, 768:
		euclideanDistanceBatch4Way(query, vectors, results, dims, true)
		return nil
	case 1024:
		for i, v := range vectors {
			if v == nil || len(v) != 1024 {
				results[i] = math.MaxFloat32
				continue
			}
			d, _ := currentDispatch.L2SquaredDistance128(query, v)
			results[i] = d
		}
		return nil
	case 3072:
		for i, v := range vectors {
			if v == nil || len(v) != 3072 {
				results[i] = math.MaxFloat32
				continue
			}
			d, _ := currentDispatch.L2SquaredDistance3072(query, v)
			results[i] = d
		}
		return nil
	}
	return currentDispatch.L2SquaredDistanceBatch(query, vectors, results)
}

func euclideanDistanceBatch4Way(query []float32, vectors [][]float32, results []float32, dims int, squared bool) {
	n := len(vectors)
	for i := 0; i < n; i += 4 {
		rem := n - i
		if rem >= 4 {
			v0 := vectors[i]
			v1 := vectors[i+1]
			v2 := vectors[i+2]
			v3 := vectors[i+3]
			if v0 != nil && v1 != nil && v2 != nil && v3 != nil &&
				len(v0) == dims && len(v1) == dims && len(v2) == dims && len(v3) == dims {

				var d0, d1, d2, d3 float32
				for k := 0; k < dims; k++ {
					qk := query[k]
					diff0 := qk - v0[k]
					diff1 := qk - v1[k]
					diff2 := qk - v2[k]
					diff3 := qk - v3[k]
					d0 += diff0 * diff0
					d1 += diff1 * diff1
					d2 += diff2 * diff2
					d3 += diff3 * diff3
				}
				if squared {
					results[i] = d0
					results[i+1] = d1
					results[i+2] = d2
					results[i+3] = d3
				} else {
					results[i] = float32(math.Sqrt(float64(d0)))
					results[i+1] = float32(math.Sqrt(float64(d1)))
					results[i+2] = float32(math.Sqrt(float64(d2)))
					results[i+3] = float32(math.Sqrt(float64(d3)))
				}
				continue
			}
		}
		for k := 0; k < rem; k++ {
			idx := i + k
			v := vectors[idx]
			if v == nil || len(v) != dims {
				results[idx] = math.MaxFloat32
				continue
			}
			var d float32
			for k := 0; k < dims; k++ {
				diff := query[k] - v[k]
				d += diff * diff
			}
			if squared {
				results[idx] = d
			} else {
				results[idx] = float32(math.Sqrt(float64(d)))
			}
		}
	}
}

func dotProductBatch4Way(query []float32, vectors [][]float32, results []float32, dims int) {
	n := len(vectors)
	for i := 0; i < n; i += 4 {
		rem := n - i
		if rem >= 4 {
			v0 := vectors[i]
			v1 := vectors[i+1]
			v2 := vectors[i+2]
			v3 := vectors[i+3]
			if v0 != nil && v1 != nil && v2 != nil && v3 != nil &&
				len(v0) == dims && len(v1) == dims && len(v2) == dims && len(v3) == dims {

				var d0, d1, d2, d3 float32
				for k := 0; k < dims; k++ {
					qk := query[k]
					d0 += qk * v0[k]
					d1 += qk * v1[k]
					d2 += qk * v2[k]
					d3 += qk * v3[k]
				}
				results[i] = d0
				results[i+1] = d1
				results[i+2] = d2
				results[i+3] = d3
				continue
			}
		}
		for k := 0; k < rem; k++ {
			idx := i + k
			v := vectors[idx]
			if v == nil || len(v) != dims {
				results[idx] = 0
				continue
			}
			var d float32
			for k := 0; k < dims; k++ {
				d += query[k] * v[k]
			}
			results[idx] = d
		}
	}
}
