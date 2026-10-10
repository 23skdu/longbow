//go:build gpu && darwin && arm64 && cgo

package metal

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (idx *MetalIndexOptimized) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	pow2 := 1
	for pow2 < idx.dim {
		pow2 <<= 1
	}

	// TurboQuant requires query rotation for distance parity (seed 42 is currently used)
	rotatedQuery := make([]float32, pow2)
	copy(rotatedQuery, vector)
	// We use the same hardcoded seed 42 as the CPU implementation for now.
	// In the future, this should be configurable via the index.
	if err := simd.RandomRotation(rotatedQuery, 42); err != nil {
		return nil, nil, fmt.Errorf("failed to rotate query for TQ search: %w", err)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	start := time.Now()
	ret := C.metal_search_tq_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&rotatedQuery[0])),
		C.int(k),
		C.int(pow2),
		C.int(bitsPerAngle),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("optimized Metal TQ search failed")
	}

	metrics.TurboQuantDequantizeLatencySeconds.Observe(time.Since(start).Seconds())
	return resultIDs, resultDistances, nil
}

func packedSize(dims int, bitsPerAngle int) int {
	pow2 := 1
	for pow2 < dims {
		pow2 <<= 1
	}
	angleCount := pow2 - 1
	angleBytes := (angleCount*bitsPerAngle + 7) / 8
	bitBytes := (pow2 + 7) / 8
	size := 4 + angleBytes + bitBytes
	return (size + 3) &^ 3 // Pad to 4 bytes for GPU alignment
}

func (idx *MetalIndexOptimized) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	stride := packedSize(idx.dim, bitsPerAngle)
	n := len(tqData) / stride
	if len(ids) != n {
		return fmt.Errorf("id count %d does not match TQ vector count %d", len(ids), n)
	}

	start := time.Now()
	ret := C.metal_add_tq_vectors_optimized(
		idx.handle,
		(*C.uchar)(unsafe.Pointer(&tqData[0])),
		C.int(stride),
		(*C.int64_t)(unsafe.Pointer(&ids[0])),
		C.int(n),
	)
	metrics.GPUIngestKernelDurationSeconds.Observe(time.Since(start).Seconds())

	if ret != 0 {
		return fmt.Errorf("failed to add TQ vectors to optimized Metal buffer")
	}

	return nil
}

func (idx *MetalIndexOptimized) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index closed")
	}

	// For the optimized backend, we store the graph in unified memory buffers
	// for direct access by Metal kernels.
	idx.graphOffsets = offsets
	idx.graphNeighbors = neighbors
	idx.graphWeights = weights

	// Sync to GPU
	var offsetsPtr, neighborsPtr *C.uint32_t
	var weightsPtr *C.float

	if len(offsets) > 0 {
		offsetsPtr = (*C.uint32_t)(unsafe.Pointer(&offsets[0]))
	}
	if len(neighbors) > 0 {
		neighborsPtr = (*C.uint32_t)(unsafe.Pointer(&neighbors[0]))
	}
	if len(weights) > 0 {
		weightsPtr = (*C.float)(unsafe.Pointer(&weights[0]))
	}

	ret := C.metal_update_graph_optimized(
		idx.handle,
		offsetsPtr, C.int(len(offsets)),
		neighborsPtr, C.int(len(neighbors)),
		weightsPtr, C.int(len(weights)),
	)

	if ret != 0 {
		return fmt.Errorf("failed to update graph on GPU")
	}

	return nil
}

func (idx *MetalIndexOptimized) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index closed")
	}

	if len(idx.graphOffsets) == 0 {
		return nil, nil, fmt.Errorf("graph not initialized")
	}

	// BFS expansion (initially on CPU for stability, kernels to follow)
	visited := make(map[uint32]float32)
	for _, seed := range seeds {
		visited[seed] = 1.0
	}

	currentFrontier := seeds
	for d := 0; d < depth; d++ {
		var nextFrontier []uint32
		for _, nodeID := range currentFrontier {
			if int(nodeID)+1 >= len(idx.graphOffsets) {
				continue
			}
			start := idx.graphOffsets[nodeID]
			end := idx.graphOffsets[nodeID+1]

			for neighborIdx := start; neighborIdx < end; neighborIdx++ {
				neighbor := idx.graphNeighbors[neighborIdx]
				if _, seen := visited[neighbor]; !seen {
					score := visited[nodeID] * alpha
					visited[neighbor] = score
					nextFrontier = append(nextFrontier, neighbor)
				}
			}
		}
		if len(nextFrontier) == 0 {
			break
		}
		currentFrontier = nextFrontier
	}

	outIDs := make([]uint32, 0, len(visited))
	outScores := make([]float32, 0, len(visited))
	for id, score := range visited {
		outIDs = append(outIDs, id)
		outScores = append(outScores, score)
	}

	return outIDs, outScores, nil
}

func (idx *MetalIndexOptimized) SearchBatchDistances(query []float32, candidateIDs []uint32) ([]float32, error) {
	return nil, fmt.Errorf("SearchBatchDistances not implemented for optimized MetalIndex")
}

func (idx *MetalIndexOptimized) HaversineSearch(centerLat, centerLon float32, points []float32, earthRadius float32) ([]float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	count := len(points) / 2
	if count == 0 {
		return nil, nil
	}

	results := make([]float32, count)
	center := []float32{centerLat, centerLon}

	start := time.Now()
	ret := C.metal_haversine_batch_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&center[0])),
		(*C.float)(unsafe.Pointer(&points[0])),
		(*C.float)(unsafe.Pointer(&results[0])),
		C.float(earthRadius),
		C.int(count),
	)

	if ret != 0 {
		// CPU fallback
		const degToRad = math.Pi / 180.0
		lat1 := float64(centerLat) * degToRad
		lon1 := float64(centerLon) * degToRad

		for i := 0; i < count; i++ {
			lat2 := float64(points[i*2]) * degToRad
			lon2 := float64(points[i*2+1]) * degToRad

			dLat := lat2 - lat1
			dLon := lon2 - lon1

			a := math.Sin(dLat/2)*math.Sin(dLat/2) +
				math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
			c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
			results[i] = float32(float64(earthRadius) * c)
		}
	} else {
		metrics.GPUComputeDurationSeconds.WithLabelValues("Apple Silicon GPU (Optimized)", "haversine").Observe(time.Since(start).Seconds())
	}
	return results, nil
}

func (idx *MetalIndexOptimized) NormBatch(vectors []float32, dims int) ([]float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	count := len(vectors) / dims
	if count == 0 {
		return nil, nil
	}

	results := make([]float32, count)

	start := time.Now()
	ret := C.metal_norm_batch_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vectors[0])),
		(*C.float)(unsafe.Pointer(&results[0])),
		C.int(dims),
		C.int(count),
	)

	if ret != 0 {
		// CPU fallback
		for i := 0; i < count; i++ {
			var sum float64
			for j := 0; j < dims; j++ {
				val := float64(vectors[i*dims+j])
				sum += val * val
			}
			results[i] = float32(math.Sqrt(sum))
		}
	} else {
		metrics.GPUComputeDurationSeconds.WithLabelValues("Apple Silicon GPU (Optimized)", "norm_batch").Observe(time.Since(start).Seconds())
	}
	return results, nil
}

func (idx *MetalIndexOptimized) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
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

func (idx *MetalIndexOptimized) SearchGreedy(query []float32, entryPoint uint32, entryDist float32) (uint32, float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return 0, 0, fmt.Errorf("index closed")
	}

	ep := entryPoint
	ed := entryDist
	ret := C.metal_greedy_search_optimized(idx.handle, (*C.float)(unsafe.Pointer(&query[0])), (*C.uint32_t)(unsafe.Pointer(&ep)), (*C.float)(unsafe.Pointer(&ed)))
	if ret != 0 {
		return 0, 0, fmt.Errorf("GPU greedy search failed")
	}
	return ep, ed, nil
}

func (idx *MetalIndexOptimized) SearchGreedyTQ(query []float32, entryPoint uint32, entryDist float32, bitsPerAngle int) (uint32, float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return 0, 0, fmt.Errorf("index closed")
	}

	pow2 := 1
	for pow2 < idx.dim {
		pow2 <<= 1
	}

	// TurboQuant requires query rotation for distance parity
	rotatedQuery := make([]float32, pow2)
	copy(rotatedQuery, query)
	if err := simd.RandomRotation(rotatedQuery, 42); err != nil {
		return 0, 0, fmt.Errorf("failed to rotate query for TQ greedy search: %w", err)
	}

	ep := entryPoint
	ed := entryDist
	start := time.Now()
	ret := C.metal_greedy_search_tq_optimized(idx.handle, (*C.float)(unsafe.Pointer(&rotatedQuery[0])), C.int(pow2), C.int(bitsPerAngle), (*C.uint32_t)(unsafe.Pointer(&ep)), (*C.float)(unsafe.Pointer(&ed)))
	if ret != 0 {
		return 0, 0, fmt.Errorf("GPU greedy TQ search failed")
	}
	metrics.TurboQuantDequantizeLatencySeconds.Observe(time.Since(start).Seconds())
	return ep, ed, nil
}

func (idx *MetalIndexOptimized) PruneNeighbors(candidateIds []uint32, candidateDists []float32, maxNeighbors int, allVectors []float32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	numCandidates := len(candidateIds)
	if numCandidates == 0 {
		return []uint32{}, nil
	}

	selectedIds := make([]uint32, maxNeighbors)
	var selectedCount uint32

	var vecPtr *C.float
	if len(allVectors) > 0 {
		vecPtr = (*C.float)(unsafe.Pointer(&allVectors[0]))
	}

	start := time.Now()
	ret := C.metal_prune_neighbors_optimized(
		idx.handle,
		(*C.uint32_t)(unsafe.Pointer(&candidateIds[0])),
		(*C.float)(unsafe.Pointer(&candidateDists[0])),
		(*C.uint32_t)(unsafe.Pointer(&selectedIds[0])),
		(*C.uint32_t)(unsafe.Pointer(&selectedCount)),
		vecPtr,
		C.int(maxNeighbors),
		C.int(numCandidates),
		C.int(idx.dim),
		C.bool(true),
	)

	if ret == 0 {
		metrics.GPUComputeDurationSeconds.WithLabelValues("Apple Silicon GPU (Optimized)", "prune_neighbors").Observe(time.Since(start).Seconds())
		return selectedIds[:selectedCount], nil
	}

	// CPU fallback: simple distance-based pruning
	type cand struct {
		id   uint32
		dist float32
	}
	cands := make([]cand, len(candidateIds))
	for i := range candidateIds {
		cands[i] = cand{id: candidateIds[i], dist: candidateDists[i]}
	}

	sort.Slice(cands, func(i, j int) bool {
		return cands[i].dist < cands[j].dist
	})

	n := maxNeighbors
	if n > len(cands) {
		n = len(cands)
	}

	pruned := make([]uint32, n)
	for i := 0; i < n; i++ {
		pruned[i] = cands[i].id
	}

	return pruned, nil
}

