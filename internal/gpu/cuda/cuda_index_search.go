//go:build gpu && linux && cgo

package cuda

/*
#include "cuda_kernels_decl.h"
*/
import "C"

import (
	"fmt"
	"sort"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/metrics"
)

func (idx *CUDAIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	if err := idx.Flush(); err != nil {
		return nil, nil, err
	}

	if idx.pager == nil {
		return nil, nil, fmt.Errorf("GPU pager not initialized")
	}

	n := idx.vectorCount
	if n == 0 {
		return nil, nil, nil
	}

	if k > 2147483647 {
		return nil, nil, fmt.Errorf("k too large")
	}
	if k > n {
		k = n
	}

	start := time.Now()

	// Stream handle for asynchronous memory copies and kernel dispatch
	var stream unsafe.Pointer
	var cStream C.cudaStream_t
	if idx.handle != nil {
		stream = unsafe.Pointer(idx.handle.streams[0])
		cStream = idx.handle.streams[0]
	}

	// Upload query to GPU asynchronously using pinned host memory when available
	qBytes := int64(idx.dim * 4)
	dQuery, err := idx.allocGPUMem(qBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate query GPU memory: %w", err)
	}
	defer idx.freeGPUMem(dQuery)

	if idx.pinnedPool != nil {
		hQueryBuf, err := idx.pinnedPool.Get(qBytes)
		if err == nil {
			C.memcpy(hQueryBuf, unsafe.Pointer(&vector[0]), C.size_t(qBytes))
			_ = MemcpyAsync(dQuery, hQueryBuf, qBytes, MemcpyHostToDevice, stream)
			defer idx.pinnedPool.Put(hQueryBuf, qBytes)
		} else {
			C.cudaMemcpy(dQuery, unsafe.Pointer(&vector[0]), C.size_t(qBytes), C.cudaMemcpyHostToDevice)
		}
	} else {
		C.cudaMemcpy(dQuery, unsafe.Pointer(&vector[0]), C.size_t(qBytes), C.cudaMemcpyHostToDevice)
	}

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage

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
		vecsInChunk := n - chunk*vectorsPerPage
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
		return nil, nil, fmt.Errorf("no resident pages available for search")
	}

	numPages := len(pages)
	hPageStarts := make([]C.int, numPages+1)
	hPagePtrs := make([]unsafe.Pointer, numPages)
	for i, p := range pages {
		hPagePtrs[i] = p.ptr
		hPageStarts[i+1] = hPageStarts[i] + C.int(p.nvecs)
	}
	totalVecs := int(hPageStarts[numPages])

	// Allocate single output buffer for all vectors
	dAllDists, err := idx.allocGPUMem(int64(totalVecs * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate distance buffer: %w", err)
	}
	defer idx.freeGPUMem(dAllDists)

	// Allocate device-side arrays for batched launch
	dPagePtrs, err := idx.allocGPUMem(int64(numPages) * int64(unsafe.Sizeof(hPagePtrs[0])))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate page pointers buffer: %w", err)
	}
	defer idx.freeGPUMem(dPagePtrs)
	C.cudaMemcpy(dPagePtrs, unsafe.Pointer(&hPagePtrs[0]), C.size_t(numPages)*C.size_t(unsafe.Sizeof(hPagePtrs[0])), C.cudaMemcpyHostToDevice)

	dPageStarts, err := idx.allocGPUMem(int64((numPages + 1) * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate page starts buffer: %w", err)
	}
	defer idx.freeGPUMem(dPageStarts)
	C.cudaMemcpy(dPageStarts, unsafe.Pointer(&hPageStarts[0]), C.size_t((numPages+1)*4), C.cudaMemcpyHostToDevice)

	if idx.dim > 1024 {
		C.launch_l2_distance_kernel_large_v2_batched(
			(**C.float)(dPagePtrs),
			(*C.int)(dPageStarts),
			(*C.float)(dQuery),
			(*C.float)(dAllDists),
			C.int(idx.dim),
			C.int(totalVecs),
			C.int(numPages),
			cStream,
		)
	} else {
		C.launch_l2_distance_kernel_v2_batched(
			(**C.float)(dPagePtrs),
			(*C.int)(dPageStarts),
			(*C.float)(dQuery),
			(*C.float)(dAllDists),
			C.int(idx.dim),
			C.int(totalVecs),
			C.int(numPages),
			cStream,
		)
	}

	hAllDists := make([]float32, totalVecs)
	distBytes := int64(totalVecs * 4)

	// Transfer distance results asynchronously using pinned host memory when available
	if idx.pinnedPool != nil {
		hDistBuf, err := idx.pinnedPool.Get(distBytes)
		if err == nil {
			_ = MemcpyAsync(hDistBuf, dAllDists, distBytes, MemcpyDeviceToHost, stream)
			_ = StreamSynchronize(stream)
			C.memcpy(unsafe.Pointer(&hAllDists[0]), hDistBuf, C.size_t(distBytes))
			idx.pinnedPool.Put(hDistBuf, distBytes)
		} else {
			C.cudaMemcpy(
				unsafe.Pointer(&hAllDists[0]),
				dAllDists,
				C.size_t(distBytes),
				C.cudaMemcpyDeviceToHost,
			)
		}
	} else {
		C.cudaMemcpy(
			unsafe.Pointer(&hAllDists[0]),
			dAllDists,
			C.size_t(distBytes),
			C.cudaMemcpyDeviceToHost,
		)
	}

	type scored struct {
		dist float32
		pos  int
	}
	all := make([]scored, 0, totalVecs)
	for i, d := range hAllDists {
		all = append(all, scored{dist: d, pos: i})
	}

	// Sort all distances to find top-K
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

	duration := time.Since(start)
	metrics.RecordGPUSearch(duration, "cuda", k)

	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
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

func (idx *CUDAIndex) SearchWithFilter(query []float32, k int, bitset []uint64) ([]int64, []float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", idx.dim, len(query))
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
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

	// Upload query to GPU
	dQuery, err := idx.allocGPUMem(int64(idx.dim * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("cudaMalloc query failed: %w", err)
	}
	defer idx.freeGPUMem(dQuery)
	C.cudaMemcpy(dQuery, unsafe.Pointer(&query[0]), C.size_t(idx.dim*4), C.cudaMemcpyHostToDevice)

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage

	type pageEntry struct {
		ptr   unsafe.Pointer
		nvecs int
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
		vecsInChunk := n - chunk*vectorsPerPage
		if vecsInChunk > vectorsPerPage {
			vecsInChunk = vectorsPerPage
		}
		pages = append(pages, pageEntry{ptr: gpuPtr, nvecs: vecsInChunk})
	}

	defer func() {
		for _, pi := range pinnedPages {
			idx.pager.Unpin(pi)
		}
	}()

	if len(pages) == 0 {
		return nil, nil, nil
	}

	numPages := len(pages)
	hPageStarts := make([]C.int, numPages+1)
	hPagePtrs := make([]unsafe.Pointer, numPages)
	for i, p := range pages {
		hPagePtrs[i] = p.ptr
		hPageStarts[i+1] = hPageStarts[i] + C.int(p.nvecs)
	}
	totalVecs := int(hPageStarts[numPages])

	dAllDists, err := idx.allocGPUMem(int64(totalVecs * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("cudaMalloc distances failed: %w", err)
	}
	defer idx.freeGPUMem(dAllDists)

	dPagePtrs, err := idx.allocGPUMem(int64(numPages) * int64(unsafe.Sizeof(hPagePtrs[0])))
	if err != nil {
		return nil, nil, fmt.Errorf("cudaMalloc page ptrs failed: %w", err)
	}
	defer idx.freeGPUMem(dPagePtrs)
	C.cudaMemcpy(dPagePtrs, unsafe.Pointer(&hPagePtrs[0]), C.size_t(numPages)*C.size_t(unsafe.Sizeof(hPagePtrs[0])), C.cudaMemcpyHostToDevice)

	dPageStarts, err := idx.allocGPUMem(int64((numPages + 1) * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("cudaMalloc page starts failed: %w", err)
	}
	defer idx.freeGPUMem(dPageStarts)
	C.cudaMemcpy(dPageStarts, unsafe.Pointer(&hPageStarts[0]), C.size_t((numPages+1)*4), C.cudaMemcpyHostToDevice)

	if idx.dim > 1024 {
		C.launch_l2_distance_kernel_large_v2_batched(
			(**C.float)(dPagePtrs),
			(*C.int)(dPageStarts),
			(*C.float)(dQuery),
			(*C.float)(dAllDists),
			C.int(idx.dim),
			C.int(totalVecs),
			C.int(numPages),
			nil,
		)
	} else {
		C.launch_l2_distance_kernel_v2_batched(
			(**C.float)(dPagePtrs),
			(*C.int)(dPageStarts),
			(*C.float)(dQuery),
			(*C.float)(dAllDists),
			C.int(idx.dim),
			C.int(totalVecs),
			C.int(numPages),
			nil,
		)
	}

	hAllDists := make([]float32, totalVecs)
	C.cudaMemcpy(
		unsafe.Pointer(&hAllDists[0]),
		dAllDists,
		C.size_t(totalVecs*4),
		C.cudaMemcpyDeviceToHost,
	)

	type scored struct {
		dist float32
		pos  int
	}
	all := make([]scored, 0, totalVecs)
	for i, d := range hAllDists {
		if bitset != nil {
			if i < len(idx.idList) {
				id := idx.idList[i]
				if id >= 0 && int(id/64) < len(bitset) && (bitset[id/64]>>uint(id%64))&1 == 0 {
					continue
				}
			}
		}
		all = append(all, scored{dist: d, pos: i})
	}

	if len(all) == 0 {
		return nil, nil, nil
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

	metrics.RecordGPUSearch(time.Since(start), "cuda_filtered", k)
	return resultIDs, resultDistances, nil
}
