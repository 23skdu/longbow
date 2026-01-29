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
	dims := len(query)
	if dims == 384 {
		for i, v := range vectors {
			if v == nil || len(v) != 384 {
				results[i] = math.MaxFloat32
				continue
			}
			d, _ := currentDispatch.EuclideanDistance384(query, v)
			results[i] = d
		}
		return nil
	}
	if dims == 128 {
		for i, v := range vectors {
			if v == nil || len(v) != 128 {
				results[i] = math.MaxFloat32
				continue
			}
			d, _ := currentDispatch.EuclideanDistance128(query, v)
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
	if dims == 384 {
		for i := 0; i < numVectors; i++ {
			offset := i * 384
			v := flatVectors[offset : offset+384]
			d, _ := currentDispatch.EuclideanDistance384(query, v)
			results[i] = d
		}
		return nil
	}
	if dims == 128 {
		for i := 0; i < numVectors; i++ {
			offset := i * 128
			v := flatVectors[offset : offset+128]
			d, _ := currentDispatch.EuclideanDistance128(query, v)
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
	ptr := unsafe.Pointer(&vec[0])
	return unsafe.Slice((*byte)(ptr), size)
}
