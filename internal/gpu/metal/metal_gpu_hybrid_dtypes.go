//go:build gpu && darwin && arm64 && cgo

package metal

/*
#include "metal_bridge.h"
*/
import "C"

import (
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (idx *MetalHybridIndex) SearchInt8(vector []int8, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	// Convert int8 to float32 for search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalHybridIndex) SearchUint8(vector []uint8, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalHybridIndex) SearchInt16(vector []int16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalHybridIndex) SearchUint16(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalHybridIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// Convert uint16 (fp16) to float32 for search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float16.FromBits(v).Float32()
	}

	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalHybridIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// complex64 is stored as interleaved float16 (uint16) real/imag pairs
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float16.FromBits(v).Float32()
	}

	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalHybridIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// complex128 stored as interleaved float32 - just use as-is
	return idx.searchFloat32(vector, k)
}

func (idx *MetalHybridIndex) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	return nil
}

func (idx *MetalHybridIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	return nil, nil, nil
}

func (idx *MetalHybridIndex) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
	// CPU fallback for cluster assignment
	numVecs := len(vectors) / idx.dim
	numClusters := len(centroids) / idx.dim
	assignments := make([]uint32, numVecs)

	for i := 0; i < numVecs; i++ {
		vec := vectors[i*idx.dim : (i+1)*idx.dim]
		minDist := float32(math.MaxFloat32)
		bestCluster := uint32(0)

		for j := 0; j < numClusters; j++ {
			centroid := centroids[j*idx.dim : (j+1)*idx.dim]
			dist := float32(0)
			for k := 0; k < idx.dim; k++ {
				diff := vec[k] - centroid[k]
				dist += diff * diff
			}
			if dist < minDist {
				minDist = dist
				bestCluster = uint32(j)
			}
		}
		assignments[i] = bestCluster
	}
	return assignments, nil
}

func (idx *MetalHybridIndex) searchFloat32(vector []float32, k int) ([]int64, []float32, error) {

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	// Step 1: GPU computes all distances in parallel
	vectorCount := int(C.int(idx.handle.vectorCount))
	distances := make([]float32, vectorCount)

	start := time.Now()
	ret := C.metal_hybrid_compute_distances(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		(*C.float)(unsafe.Pointer(&distances[0])),
	)
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, fmt.Errorf("hybrid Metal distance computation failed")
	}

	// Record metrics
	metrics.RecordGPUSearch(duration, "metal_hybrid", k)

	// Step 2: CPU finds top-k using simple selection
	ids := make([]int64, vectorCount)
	for i := 0; i < vectorCount; i++ {
		ids[i] = int64(i)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	for i := 0; i < k && i < vectorCount; i++ {
		minIdx := i
		minDist := distances[i]

		for j := i + 1; j < vectorCount; j++ {
			if distances[j] < minDist {
				minDist = distances[j]
				minIdx = j
			}
		}

		// Swap both distance and original ID
		if minIdx != i {
			distances[i], distances[minIdx] = distances[minIdx], distances[i]
			ids[i], ids[minIdx] = ids[minIdx], ids[i]
		}

		resultIDs[i] = ids[i]
		resultDistances[i] = distances[i]
	}

	return resultIDs, resultDistances, nil
}

func (idx *MetalHybridIndex) AddPQ(ids []int64, codes []byte, m int) error {
	return nil
}

func (idx *MetalHybridIndex) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	return nil
}

func (idx *MetalHybridIndex) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	return nil, nil, nil
}

func (idx *MetalHybridIndex) HaversineSearch(centerLat, centerLon float32, points []float32, earthRadius float32) ([]float32, error) {
	return nil, nil
}

func (idx *MetalHybridIndex) NormBatch(vectors []float32, dims int) ([]float32, error) {
	return nil, nil
}

func (idx *MetalHybridIndex) PruneNeighbors(candidateIds []uint32, candidateDists []float32, maxNeighbors int, allVectors []float32) ([]uint32, error) {
	return nil, nil
}

func (idx *MetalHybridIndex) SearchGreedy(query []float32, entryPoint uint32, entryDist float32) (uint32, float32, error) {
	// CPU fallback for hybrid index
	return entryPoint, entryDist, nil
}

func (idx *MetalHybridIndex) Sync() error {
	return nil
}

func (idx *MetalHybridIndex) Clear() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.closed {
		return fmt.Errorf("index is closed")
	}
	idx.handle.vectorCount = 0
	return nil
}

func (idx *MetalHybridIndex) Reset() error {
	return idx.Clear()
}

func (idx *MetalHybridIndex) SearchBatchDistances(query []float32, candidateIDs []uint32) ([]float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	numCandidates := len(candidateIDs)
	if numCandidates == 0 {
		return []float32{}, nil
	}

	distances := make([]float32, numCandidates)

	ret := C.metal_hybrid_compute_batch_distances(
		idx.handle,
		(*C.float)(unsafe.Pointer(&query[0])),
		(*C.uint32_t)(unsafe.Pointer(&candidateIDs[0])),
		C.int(numCandidates),
		(*C.float)(unsafe.Pointer(&distances[0])),
	)

	if ret != 0 {
		return nil, fmt.Errorf("synchronous Metal batch distance computation failed")
	}

	return distances, nil
}
