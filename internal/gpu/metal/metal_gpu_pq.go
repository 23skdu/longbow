//go:build gpu && darwin && arm64 && cgo

package metal

/*
#include "metal_bridge.h"
*/
import "C"

import (
	"fmt"
	"math/rand"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
)

// SearchPQ runs the search pq operation.
func (idx *MetalIndex) SearchPQ(lookupTable []float32, m int, k int) ([]int64, []float32, error) {
	// Flush any pending batches before search
	if err := idx.Flush(); err != nil {
		return nil, nil, err
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	numVecs := int(C.metal_get_count(idx.handle))
	if numVecs == 0 {
		return []int64{}, []float32{}, nil
	}

	if k > numVecs {
		k = numVecs
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	start := time.Now()
	ret := C.metal_search_pq(
		idx.handle,
		(*C.float)(unsafe.Pointer(&lookupTable[0])),
		C.int(m),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal PQ search failed (check if pipeline initialized)")
	}

	// Record metrics
	metrics.RecordGPUSearch(duration, "metal_pq", k)

	return resultIDs, resultDistances, nil
}

// TrainPQ runs the train pq operation.
func (idx *MetalIndex) TrainPQ(vectors []float32, m int, k int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	dims := idx.dim
	if dims%m != 0 {
		return fmt.Errorf("dimension %d must be divisible by M %d", dims, m)
	}
	subDim := dims / m
	numVecs := len(vectors) / dims

	// Create encoder
	encoder, err := pq.NewPQEncoder(dims, m, k)
	if err != nil {
		return fmt.Errorf("failed to create PQ encoder: %w", err)
	}

	// Train each subspace on GPU
	for i := 0; i < m; i++ {
		// Extract subspace data
		subData := make([]float32, numVecs*subDim)
		for j := 0; j < numVecs; j++ {
			copy(subData[j*subDim:(j+1)*subDim], vectors[j*dims+i*subDim:j*dims+(i+1)*subDim])
		}

		// Initialize centroids randomly
		centroids := make([]float32, k*subDim)
		perm := rand.Perm(numVecs)
		for j := 0; j < k; j++ {
			copy(centroids[j*subDim:(j+1)*subDim], subData[perm[j]*subDim:(perm[j]+1)*subDim])
		}

		// Run GPU K-Means
		res := C.metal_train_kmeans(idx.handle, (*C.float)(&subData[0]), (*C.float)(&centroids[0]), C.int(numVecs), C.int(subDim), C.int(k), 20)
		if res != 0 {
			return fmt.Errorf("GPU K-Means failed for subspace %d", i)
		}

		// Update encoder codebook
		copy(encoder.Codebooks[i], centroids)
	}

	idx.pqEncoder = encoder
	return nil
}

// EncodePQ runs the encode pq operation.
func (idx *MetalIndex) EncodePQ(vectors []float32) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	if idx.pqEncoder == nil {
		return nil, fmt.Errorf("PQ encoder not trained")
	}

	// Encode each vector
	dims := idx.dim
	numVecs := len(vectors) / dims
	codes := make([]byte, numVecs*idx.pqEncoder.M)

	for i := 0; i < numVecs; i++ {
		vec := vectors[i*dims : (i+1)*dims]
		encoded, err := idx.pqEncoder.Encode(vec)
		if err != nil {
			return nil, fmt.Errorf("encoding failed at vector %d: %w", i, err)
		}
		copy(codes[i*idx.pqEncoder.M:(i+1)*idx.pqEncoder.M], encoded)
	}

	return codes, nil
}

// SearchBatch queries the Metal GPU index with multiple vectors in parallel.
func (idx *MetalIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
	if len(vectors) == 0 {
		return nil, nil, nil
	}

	results := make([][]int64, len(vectors))
	distances := make([][]float32, len(vectors))

	for i, vec := range vectors {
		ids, dist, err := idx.Search(vec, k)
		if err != nil {
			return nil, nil, fmt.Errorf("batch search[%d]: %w", i, err)
		}
		results[i] = ids
		distances[i] = dist
	}

	return results, distances, nil
}

// AssignToClusters offloads vector assignment to GPU centroids
func (idx *MetalIndex) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	numVectors := len(vectors) / idx.dim
	numCentroids := len(centroids) / idx.dim
	assignments := make([]uint32, numVectors)

	ret := C.metal_assign_to_clusters(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vectors[0])),
		(*C.float)(unsafe.Pointer(&centroids[0])),
		(*C.uint32_t)(unsafe.Pointer(&assignments[0])),
		C.int(numVectors),
		C.int(numCentroids),
		C.int(idx.dim),
	)

	if ret != 0 {
		return nil, fmt.Errorf("Metal cluster assignment failed")
	}

	return assignments, nil
}
