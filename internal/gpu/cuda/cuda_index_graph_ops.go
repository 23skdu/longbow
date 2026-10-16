//go:build gpu && linux && cgo

package cuda

/*
#include "cuda_kernels_decl.h"
*/
import "C"

import (
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/metrics"
)

func (idx *CUDAIndex) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
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

func (idx *CUDAIndex) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	var wPtr *C.float
	if len(weights) > 0 {
		wPtr = (*C.float)(unsafe.Pointer(&weights[0]))
	}

	ret := C.cuda_update_graph(
		idx.handle,
		(*C.uint32_t)(unsafe.Pointer(&offsets[0])),
		(*C.uint32_t)(unsafe.Pointer(&neighbors[0])),
		wPtr,
		C.int(len(offsets)-1),
		C.int(len(neighbors)),
	)

	if ret != 0 {
		return fmt.Errorf("failed to update CUDA graph")
	}

	return nil
}

func (idx *CUDAIndex) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if idx.handle.graphOffsets == nil {
		return nil, nil, fmt.Errorf("graph not initialized on GPU")
	}

	nodeCount := int(idx.handle.graphNodeCount)

	// Allocate GPU buffers for BFS through the memory pool (visible to pager)
	d_frontier, err := idx.allocGPUMem(int64(nodeCount * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc frontier failed: %w", err)
	}
	d_nextFrontier, err := idx.allocGPUMem(int64(nodeCount * 4))
	if err != nil {
		idx.freeGPUMem(d_frontier)
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc nextFrontier failed: %w", err)
	}
	d_visited, err := idx.allocGPUMem(int64((nodeCount/64 + 1) * 8))
	if err != nil {
		idx.freeGPUMem(d_frontier)
		idx.freeGPUMem(d_nextFrontier)
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc visited failed: %w", err)
	}
	d_activations, err := idx.allocGPUMem(int64(nodeCount * 4))
	if err != nil {
		idx.freeGPUMem(d_frontier)
		idx.freeGPUMem(d_nextFrontier)
		idx.freeGPUMem(d_visited)
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc activations failed: %w", err)
	}
	d_newActivations, err := idx.allocGPUMem(int64(nodeCount * 4))
	if err != nil {
		idx.freeGPUMem(d_frontier)
		idx.freeGPUMem(d_nextFrontier)
		idx.freeGPUMem(d_visited)
		idx.freeGPUMem(d_activations)
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc newActivations failed: %w", err)
	}
	d_nextSize, err := idx.allocGPUMem(4)
	if err != nil {
		idx.freeGPUMem(d_frontier)
		idx.freeGPUMem(d_nextFrontier)
		idx.freeGPUMem(d_visited)
		idx.freeGPUMem(d_activations)
		idx.freeGPUMem(d_newActivations)
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc nextSize failed: %w", err)
	}

	defer func() {
		idx.freeGPUMem(d_frontier)
		idx.freeGPUMem(d_nextFrontier)
		idx.freeGPUMem(d_visited)
		idx.freeGPUMem(d_activations)
		idx.freeGPUMem(d_newActivations)
		idx.freeGPUMem(d_nextSize)
	}()

	C.cudaMemset(unsafe.Pointer(d_visited), 0, C.size_t((nodeCount/64+1)*8))
	C.cudaMemset(unsafe.Pointer(d_activations), 0, C.size_t(nodeCount*4))
	C.cudaMemset(unsafe.Pointer(d_newActivations), 0, C.size_t(nodeCount*4))

	// Initial seeds
	frontierSize := len(seeds)
	if frontierSize == 0 {
		return nil, nil, nil
	}
	C.cudaMemcpy(unsafe.Pointer(d_frontier), unsafe.Pointer(&seeds[0]), C.size_t(frontierSize*4), C.cudaMemcpyHostToDevice)

	// Initial activations
	h_activations := make([]float32, nodeCount)
	for _, s := range seeds {
		if int(s) < nodeCount {
			h_activations[s] = 1.0
		}
	}
	C.cudaMemcpy(unsafe.Pointer(d_activations), unsafe.Pointer(&h_activations[0]), C.size_t(nodeCount*4), C.cudaMemcpyHostToDevice)

	for d := 0; d < depth; d++ {
		C.cudaMemset(unsafe.Pointer(d_nextSize), 0, 4)

		C.launch_graph_bfs_expand_kernel(
			(*C.uint32_t)(d_frontier), C.int(frontierSize),
			(*C.uint32_t)(idx.handle.graphOffsets),
			(*C.uint32_t)(idx.handle.graphNeighbors),
			(*C.ulonglong)(d_visited), (*C.uint32_t)(d_nextFrontier), (*C.int)(d_nextSize), nil,
		)

		C.launch_graph_activation_propagate_kernel(
			(*C.float)(d_activations), (*C.float)(d_newActivations),
			(*C.uint32_t)(d_frontier), C.int(frontierSize),
			(*C.uint32_t)(idx.handle.graphOffsets),
			(*C.uint32_t)(idx.handle.graphNeighbors),
			(*C.float)(idx.handle.graphWeights),
			C.float(alpha), nil,
		)

		var nextSize C.int
		C.cudaMemcpy(unsafe.Pointer(&nextSize), unsafe.Pointer(d_nextSize), 4, C.cudaMemcpyDeviceToHost)
		if nextSize == 0 {
			break
		}

		// Swap buffers
		d_frontier, d_nextFrontier = d_nextFrontier, d_frontier
		frontierSize = int(nextSize)

		// Accumulate activations
		C.cudaMemcpy(unsafe.Pointer(d_activations), unsafe.Pointer(d_newActivations), C.size_t(nodeCount*4), C.cudaMemcpyDeviceToDevice)
	}

	// Results
	finalActivations := make([]float32, nodeCount)
	C.cudaMemcpy(unsafe.Pointer(&finalActivations[0]), unsafe.Pointer(d_newActivations), C.size_t(nodeCount*4), C.cudaMemcpyDeviceToHost)

	var resIDs []uint32
	var resScores []float32
	for i, s := range finalActivations {
		if s > 1e-6 {
			resIDs = append(resIDs, uint32(i))
			resScores = append(resScores, s)
		}
	}

	return resIDs, resScores, nil
}
func (idx *CUDAIndex) SearchBatchDistances(query []float32, candidateIDs []uint32) ([]float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	nCand := len(candidateIDs)
	if nCand == 0 {
		return []float32{}, nil
	}

	if len(query) != idx.dim {
		return nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}

	if err := idx.Flush(); err != nil {
		return nil, err
	}

	if idx.pager == nil {
		return nil, fmt.Errorf("GPU pager not initialized")
	}

	totalVecs := idx.vectorCount
	if totalVecs == 0 {
		dists := make([]float32, nCand)
		for i := range dists {
			dists[i] = 1e38
		}
		return dists, nil
	}

	numChunks := (totalVecs + vectorsPerPage - 1) / vectorsPerPage
	type pageEntry struct {
		ptr    unsafe.Pointer
		nvecs  int
		pinned bool
	}
	pages := make([]pageEntry, 0, numChunks)
	var pinnedPages []*memory.PageInfo
	for chunk := 0; chunk < numChunks; chunk++ {
		pid := idx.pageIDFor(0, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			continue
		}
		if err := idx.pager.Promote(pi); err != nil {
			continue
		}
		idx.pager.Pin(pi)
		pinnedPages = append(pinnedPages, pi)
		gpuPtr := idx.pager.GetGPUAddr(pi)
		if gpuPtr == nil {
			continue
		}
		vecsInChunk := totalVecs - chunk*vectorsPerPage
		if vecsInChunk > vectorsPerPage {
			vecsInChunk = vectorsPerPage
		}
		pages = append(pages, pageEntry{ptr: gpuPtr, nvecs: vecsInChunk, pinned: true})
	}

	defer func() {
		for _, pi := range pinnedPages {
			idx.pager.Unpin(pi)
		}
	}()

	if len(pages) == 0 {
		return idx.searchBatchDistancesCPU(query, candidateIDs)
	}

	numPages := len(pages)
	hPagePtrs := make([]unsafe.Pointer, numPages)
	for i, p := range pages {
		hPagePtrs[i] = p.ptr
	}

	var cStream C.cudaStream_t
	if idx.handle != nil {
		cStream = idx.handle.streams[0]
	}

	qBytes := int64(idx.dim * 4)
	dQuery, err := idx.allocGPUMem(qBytes)
	if err != nil {
		return idx.searchBatchDistancesCPU(query, candidateIDs)
	}
	defer idx.freeGPUMem(dQuery)
	C.cudaMemcpy(dQuery, unsafe.Pointer(&query[0]), C.size_t(qBytes), C.cudaMemcpyHostToDevice)

	candBytes := int64(nCand * 4)
	dCandidates, err := idx.allocGPUMem(candBytes)
	if err != nil {
		return idx.searchBatchDistancesCPU(query, candidateIDs)
	}
	defer idx.freeGPUMem(dCandidates)
	C.cudaMemcpy(dCandidates, unsafe.Pointer(&candidateIDs[0]), C.size_t(candBytes), C.cudaMemcpyHostToDevice)

	distBytes := int64(nCand * 4)
	dDistances, err := idx.allocGPUMem(distBytes)
	if err != nil {
		return idx.searchBatchDistancesCPU(query, candidateIDs)
	}
	defer idx.freeGPUMem(dDistances)

	pagePtrsBytes := int64(numPages) * int64(unsafe.Sizeof(hPagePtrs[0]))
	dPagePtrs, err := idx.allocGPUMem(pagePtrsBytes)
	if err != nil {
		return idx.searchBatchDistancesCPU(query, candidateIDs)
	}
	defer idx.freeGPUMem(dPagePtrs)
	C.cudaMemcpy(dPagePtrs, unsafe.Pointer(&hPagePtrs[0]), C.size_t(pagePtrsBytes), C.cudaMemcpyHostToDevice)

	profL2Candidates.begin(cStream)
	C.launch_l2_distance_candidates(
		(**C.float)(dPagePtrs),
		(*C.uint32_t)(dCandidates),
		(*C.float)(dQuery),
		(*C.float)(dDistances),
		C.int(idx.dim),
		C.int(nCand),
		C.int(totalVecs),
		cStream,
	)
	profL2Candidates.end(cStream)

	hDistances := make([]float32, nCand)
	C.cudaMemcpy(unsafe.Pointer(&hDistances[0]), dDistances, C.size_t(distBytes), C.cudaMemcpyDeviceToHost)

	return hDistances, nil
}

func (idx *CUDAIndex) searchBatchDistancesCPU(query []float32, candidateIDs []uint32) ([]float32, error) {
	nCand := len(candidateIDs)
	dists := make([]float32, nCand)
	totalVecs := idx.vectorCount
	dim := idx.dim

	for i, cid := range candidateIDs {
		if int(cid) >= totalVecs {
			dists[i] = 1e38
			continue
		}
		chunk := int(cid) / vectorsPerPage
		offset := int(cid) % vectorsPerPage
		pid := idx.pageIDFor(0, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			dists[i] = 1e38
			continue
		}
		buf := idx.pager.GetCPUBuf(pi)
		if len(buf) < (offset+1)*dim*4 {
			dists[i] = 1e38
			continue
		}
		vecBytes := buf[offset*dim*4 : (offset+1)*dim*4]
		vec := unsafe.Slice((*float32)(unsafe.Pointer(&vecBytes[0])), dim)
		var sum float32
		for d := 0; d < dim; d++ {
			diff := query[d] - vec[d]
			sum += diff * diff
		}
		dists[i] = float32(math.Sqrt(float64(sum)))
	}
	return dists, nil
}

func (idx *CUDAIndex) HaversineSearch(centerLat, centerLon float32, points []float32, earthRadius float32) ([]float32, error) {
	idx.mu.RLock()
	closed := idx.closed
	idx.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("index is closed")
	}

	count := len(points) / 2
	if count == 0 {
		return nil, nil
	}

	results := make([]float32, count)
	center := []float32{centerLat, centerLon}

	dCenter, err := idx.allocGPUMem(2 * 4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate center: %w", err)
	}
	defer idx.freeGPUMem(dCenter)

	dPoints, err := idx.allocGPUMem(int64(count) * 2 * 4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate points: %w", err)
	}
	defer idx.freeGPUMem(dPoints)

	dResults, err := idx.allocGPUMem(int64(count) * 4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate results: %w", err)
	}
	defer idx.freeGPUMem(dResults)

	start := time.Now()
	profHaversine.begin(nil)
	ret := C.cuda_haversine_batch(
		idx.handle,
		(*C.float)(dCenter),
		(*C.float)(dPoints),
		(*C.float)(dResults),
		(*C.float)(unsafe.Pointer(&center[0])),
		(*C.float)(unsafe.Pointer(&points[0])),
		(*C.float)(unsafe.Pointer(&results[0])),
		C.float(earthRadius),
		C.int(count),
	)
	profHaversine.end(nil)

	if ret != 0 {
		return nil, fmt.Errorf("cuda_haversine_batch failed")
	}

	metrics.GPUComputeDurationSeconds.WithLabelValues(idx.deviceInfo.Name, "haversine").Observe(time.Since(start).Seconds())
	return results, nil
}

func (idx *CUDAIndex) NormBatch(vectors []float32, dims int) ([]float32, error) {
	idx.mu.RLock()
	closed := idx.closed
	idx.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("index is closed")
	}

	count := len(vectors) / dims
	if count == 0 {
		return nil, nil
	}

	results := make([]float32, count)

	dVectors, err := idx.allocGPUMem(int64(count) * int64(dims) * 4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate vectors: %w", err)
	}
	defer idx.freeGPUMem(dVectors)

	dResults, err := idx.allocGPUMem(int64(count) * 4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate results: %w", err)
	}
	defer idx.freeGPUMem(dResults)

	start := time.Now()
	profNormBatch.begin(nil)
	ret := C.cuda_norm_batch_f32(
		idx.handle,
		(*C.float)(dVectors),
		(*C.float)(dResults),
		(*C.float)(unsafe.Pointer(&vectors[0])),
		(*C.float)(unsafe.Pointer(&results[0])),
		C.int(dims),
		C.int(count),
	)
	profNormBatch.end(nil)

	if ret != 0 {
		return nil, fmt.Errorf("cuda_norm_batch_f32 failed")
	}

	metrics.GPUComputeDurationSeconds.WithLabelValues(idx.deviceInfo.Name, "norm").Observe(time.Since(start).Seconds())
	return results, nil
}

func (idx *CUDAIndex) PruneNeighbors(candidateIds []uint32, candidateDists []float32, maxNeighbors int, allVectors []float32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, fmt.Errorf("index closed")
	}

	numCandidates := len(candidateIds)
	selectedIds := make([]uint32, maxNeighbors)
	var selectedCount uint32

	var hPageStarts []C.int
	var hPagePtrs []unsafe.Pointer
	var totalVecs int
	var numPages int

	// For pager case, we hold pins until after kernel launch
	var pinnedPages []*memory.PageInfo
	var dAllVectors unsafe.Pointer

	if len(allVectors) > 0 {
		// Monolithic case (fallback) — upload vectors to GPU
		totalVecs = len(allVectors) / idx.dim
		var err error
		dAllVectors, err = idx.allocGPUMem(int64(len(allVectors)) * 4)
		if err != nil {
			return nil, fmt.Errorf("failed to allocate GPU memory for vectors: %w", err)
		}
		C.cudaMemcpy(dAllVectors, unsafe.Pointer(&allVectors[0]), C.size_t(len(allVectors)*4), C.cudaMemcpyHostToDevice)

		numPages = 1
		hPageStarts = []C.int{0, C.int(totalVecs)}
		hPagePtrs = []unsafe.Pointer{dAllVectors}
	} else {
		// Pager case
		n := idx.vectorCount
		numChunks := (n + vectorsPerPage - 1) / vectorsPerPage
		hPageStarts = make([]C.int, numChunks+1)
		hPagePtrs = make([]unsafe.Pointer, numChunks)

		var chunkCount int
		for chunk := 0; chunk < numChunks; chunk++ {
			pid := idx.pageIDFor(0, chunk)
			pi := idx.pager.PageInfo(pid)
			if pi == nil {
				continue
			}
			if err := idx.pager.Promote(pi); err != nil {
				continue
			}
			idx.pager.Pin(pi)
			pinnedPages = append(pinnedPages, pi)
			gpuPtr := idx.pager.GetGPUAddr(pi)
			if gpuPtr == nil {
				continue
			}
			vecsInChunk := n - chunk*vectorsPerPage
			if vecsInChunk > vectorsPerPage {
				vecsInChunk = vectorsPerPage
			}
			hPagePtrs[chunkCount] = gpuPtr
			hPageStarts[chunkCount+1] = hPageStarts[chunkCount] + C.int(vecsInChunk)
			chunkCount++
		}
		numPages = chunkCount
		totalVecs = int(hPageStarts[numPages])
		if numPages == 0 {
			return nil, fmt.Errorf("no resident pages available for prune neighbors")
		}
	}

	defer func() {
		for _, pi := range pinnedPages {
			idx.pager.Unpin(pi)
		}
		if dAllVectors != nil {
			idx.freeGPUMem(dAllVectors)
		}
	}()

	// Allocate temporary GPU buffers through the pool
	dCandIds, err := idx.allocGPUMem(int64(numCandidates) * 4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate candidate IDs: %w", err)
	}
	defer idx.freeGPUMem(dCandIds)

	dCandDists, err := idx.allocGPUMem(int64(numCandidates) * 4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate candidate dists: %w", err)
	}
	defer idx.freeGPUMem(dCandDists)

	dSelIds, err := idx.allocGPUMem(int64(maxNeighbors) * 4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate selected IDs: %w", err)
	}
	defer idx.freeGPUMem(dSelIds)

	dSelCount, err := idx.allocGPUMem(4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate selected count: %w", err)
	}
	defer idx.freeGPUMem(dSelCount)

	dPagePtrs, err := idx.allocGPUMem(int64(numPages) * int64(unsafe.Sizeof(hPagePtrs[0])))
	if err != nil {
		return nil, fmt.Errorf("failed to allocate page ptrs: %w", err)
	}
	defer idx.freeGPUMem(dPagePtrs)

	dPageStarts, err := idx.allocGPUMem(int64(numPages+1) * 4)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate page starts: %w", err)
	}
	defer idx.freeGPUMem(dPageStarts)

	// Copy data to GPU
	C.cudaMemcpy(dCandIds, unsafe.Pointer(&candidateIds[0]), C.size_t(numCandidates)*4, C.cudaMemcpyHostToDevice)
	C.cudaMemcpy(dCandDists, unsafe.Pointer(&candidateDists[0]), C.size_t(numCandidates)*4, C.cudaMemcpyHostToDevice)
	C.cudaMemset(dSelCount, 0, 4)
	C.cudaMemcpy(dPagePtrs, unsafe.Pointer(&hPagePtrs[0]), C.size_t(numPages)*C.size_t(unsafe.Sizeof(hPagePtrs[0])), C.cudaMemcpyHostToDevice)
	C.cudaMemcpy(dPageStarts, unsafe.Pointer(&hPageStarts[0]), C.size_t((numPages+1)*4), C.cudaMemcpyHostToDevice)

	profHNSWPrune.begin(nil)

	ret := C.cuda_prune_neighbors(
		idx.handle,
		(*C.uint32_t)(dCandIds),
		(*C.float)(dCandDists),
		(*C.uint32_t)(dSelIds),
		(*C.uint32_t)(dSelCount),
		(**C.float)(dPagePtrs),
		(*C.int)(dPageStarts),
		(*C.uint32_t)(unsafe.Pointer(&selectedIds[0])),
		(*C.uint32_t)(unsafe.Pointer(&selectedCount)),
		C.int(maxNeighbors),
		C.int(numCandidates),
		C.int(idx.dim),
		C.int(totalVecs),
		C.int(numPages),
		C.bool(true),
	)

	profHNSWPrune.end(nil)

	if ret != 0 {
		return nil, fmt.Errorf("cuda_prune_neighbors failed: %d", ret)
	}

	return selectedIds[:selectedCount], nil
}
