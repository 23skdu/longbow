//go:build gpu && linux && cgo

package cuda

/*
#include "cuda_kernels_decl.h"
*/
import "C"

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
)

func (idx *CUDAIndex) SearchPQ(lookupTable []float32, m int, k int) ([]int64, []float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	if m > 2147483647 || k > 2147483647 {
		return nil, nil, fmt.Errorf("m or k too large")
	}
	if idx.pager == nil {
		return nil, nil, fmt.Errorf("GPU pager not initialized")
	}

	n := idx.vectorCount
	if n == 0 {
		return nil, nil, nil
	}
	if k > n {
		k = n
	}

	start := time.Now()

	// Upload lookup table to GPU
	tableSize := int64(m) * 256 * 4
	dTable, err := idx.allocGPUMem(tableSize)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate lookup table GPU memory: %w", err)
	}
	defer idx.freeGPUMem(dTable)
	C.cudaMemcpy(dTable, unsafe.Pointer(&lookupTable[0]), C.size_t(tableSize), C.cudaMemcpyHostToDevice)

	// Per-page distance buffer
	dPageDists, err := idx.allocGPUMem(int64(vectorsPerPage * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate per-page distance buffer: %w", err)
	}
	defer idx.freeGPUMem(dPageDists)

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage
	type scored struct {
		dist float32
		pos  int
	}
	all := make([]scored, 0, n)
	hPageDists := make([]float32, vectorsPerPage)

	var pinnedPages []*memory.PageInfo
	defer func() {
		for _, pi := range pinnedPages {
			idx.pager.Unpin(pi)
		}
	}()

	for chunk := 0; chunk < numChunks; chunk++ {
		pid := idx.pageIDFor(2, chunk)
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

		C.launch_pq_distance_kernel(
			(*C.float)(dTable),
			(*C.uchar)(gpuPtr),
			(*C.float)(dPageDists),
			C.int(m),
			C.int(vecsInChunk),
			nil,
		)

		hPageDists = hPageDists[:vecsInChunk]
		C.cudaMemcpy(
			unsafe.Pointer(&hPageDists[0]),
			dPageDists,
			C.size_t(vecsInChunk*4),
			C.cudaMemcpyDeviceToHost,
		)

		base := chunk * vectorsPerPage
		for i, d := range hPageDists {
			all = append(all, scored{dist: d, pos: base + i})
		}
	}

	if len(all) == 0 {
		return nil, nil, fmt.Errorf("no resident PQ pages available")
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].dist < all[j].dist
	})
	if k > len(all) {
		k = len(all)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)
	for i := 0; i < k; i++ {
		resultDistances[i] = all[i].dist
		if all[i].pos < len(idx.idList) {
			resultIDs[i] = idx.idList[all[i].pos]
		}
	}

	metrics.RecordGPUSearch(time.Since(start), "cuda_pq", k)
	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) TrainPQ(vectors []float32, m int, k int) error {
	if err := idx.acquireGPUOp(); err != nil {
		return fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

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

	encoder, err := pq.NewPQEncoder(dims, m, k)
	if err != nil {
		return fmt.Errorf("failed to create PQ encoder: %w", err)
	}

	// Pre-allocate GPU buffers for K-Means (reused across subspaces)
	vecSize := int64(numVecs) * int64(subDim) * 4
	centSize := int64(k) * int64(subDim) * 4
	assignSize := int64(numVecs) * 4
	countSize := int64(k) * 4

	dVectors, err := idx.allocGPUMem(vecSize)
	if err != nil {
		return fmt.Errorf("failed to allocate K-Means vectors: %w", err)
	}
	defer idx.freeGPUMem(dVectors)

	dCentroids, err := idx.allocGPUMem(centSize)
	if err != nil {
		return fmt.Errorf("failed to allocate K-Means centroids: %w", err)
	}
	defer idx.freeGPUMem(dCentroids)

	dSumCentroids, err := idx.allocGPUMem(centSize)
	if err != nil {
		return fmt.Errorf("failed to allocate K-Means sum centroids: %w", err)
	}
	defer idx.freeGPUMem(dSumCentroids)

	dAssignments, err := idx.allocGPUMem(assignSize)
	if err != nil {
		return fmt.Errorf("failed to allocate K-Means assignments: %w", err)
	}
	defer idx.freeGPUMem(dAssignments)

	dCounts, err := idx.allocGPUMem(countSize)
	if err != nil {
		return fmt.Errorf("failed to allocate K-Means counts: %w", err)
	}
	defer idx.freeGPUMem(dCounts)

	// Train each subspace
	for i := 0; i < m; i++ {
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

		// Run GPU K-Means with pre-allocated buffers
		res := C.cuda_train_kmeans(
			idx.handle,
			(*C.float)(dVectors), (*C.float)(dCentroids), (*C.float)(dSumCentroids),
			(*C.uint32_t)(dAssignments), (*C.uint32_t)(dCounts),
			(*C.float)(unsafe.Pointer(&subData[0])),
			(*C.float)(unsafe.Pointer(&centroids[0])),
			C.int(numVecs), C.int(subDim), C.int(k), 20,
		)
		if res != 0 {
			return fmt.Errorf("GPU K-Means failed for subspace %d", i)
		}

		copy(encoder.Codebooks[i], centroids)
	}

	idx.pqEncoder = encoder
	return nil
}

func (idx *CUDAIndex) EncodePQ(vectors []float32) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	if idx.pqEncoder == nil {
		return nil, fmt.Errorf("PQ encoder not trained")
	}

	numVecs := len(vectors) / idx.dim
	codes := make([]byte, numVecs*idx.pqEncoder.M)

	for i := 0; i < numVecs; i++ {
		vec := vectors[i*idx.dim : (i+1)*idx.dim]
		encoded, err := idx.pqEncoder.Encode(vec)
		if err != nil {
			return nil, fmt.Errorf("encoding failed at vector %d: %w", i, err)
		}
		copy(codes[i*idx.pqEncoder.M:(i+1)*idx.pqEncoder.M], encoded)
	}

	return codes, nil
}

func (idx *CUDAIndex) PQEncode(vectors []float32, codebooks []float32, m, subDim int) ([]byte, error) {
	idx.mu.RLock()
	closed := idx.closed
	idx.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("index is closed")
	}

	numVectors := len(vectors) / (m * subDim)
	if numVectors == 0 {
		return nil, nil
	}

	codes := make([]byte, numVectors*m)

	// Allocate GPU buffers through the pool
	vecSize := int64(numVectors) * int64(m) * int64(subDim) * 4
	cbSize := int64(m) * 256 * int64(subDim) * 4
	codeSize := int64(numVectors) * int64(m)

	dVectors, err := idx.allocGPUMem(vecSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate vectors: %w", err)
	}
	defer idx.freeGPUMem(dVectors)

	dCodebooks, err := idx.allocGPUMem(cbSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate codebooks: %w", err)
	}
	defer idx.freeGPUMem(dCodebooks)

	dCodes, err := idx.allocGPUMem(codeSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate codes: %w", err)
	}
	defer idx.freeGPUMem(dCodes)

	start := time.Now()
	ret := C.cuda_pq_encode(
		idx.handle,
		(*C.float)(dVectors),
		(*C.float)(dCodebooks),
		(*C.uchar)(dCodes),
		(*C.float)(unsafe.Pointer(&vectors[0])),
		(*C.float)(unsafe.Pointer(&codebooks[0])),
		(*C.uchar)(unsafe.Pointer(&codes[0])),
		C.int(numVectors),
		C.int(m),
		C.int(subDim),
	)

	if ret != 0 {
		return nil, fmt.Errorf("cuda_pq_encode failed")
	}

	metrics.GPUComputeDurationSeconds.WithLabelValues(idx.deviceInfo.Name, "pq_encode").Observe(time.Since(start).Seconds())
	return codes, nil
}
