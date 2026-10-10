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

func (idx *CUDAIndex) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}
	count := len(ids)
	if count == 0 {
		return nil
	}
	if idx.pager == nil {
		return fmt.Errorf("GPU pager not initialized")
	}

	stride := len(tqData) / count
	prevCount := idx.vectorCount
	newCount := prevCount + count

	idx.idList = append(idx.idList, ids...)
	pageVecs := vectorsPerPage

	for i := 0; i < count; {
		globalPos := prevCount + i
		chunk := globalPos / pageVecs
		offset := globalPos % pageVecs
		space := pageVecs - offset
		toCopy := count - i
		if toCopy > space {
			toCopy = space
		}

		pid := idx.pageIDFor(3, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			var err error
			pi, err = idx.pager.Alloc(pid)
			if err != nil {
				return fmt.Errorf("failed to allocate pager page for TQ chunk %d: %w", chunk, err)
			}
		}

		cpuBuf := idx.pager.GetCPUBuf(pi)
		srcStart := i * stride
		srcEnd := (i + toCopy) * stride
		dstStart := offset * stride
		copy(cpuBuf[dstStart:dstStart+toCopy*stride], tqData[srcStart:srcEnd])

		if err := idx.pager.Promote(pi); err != nil {
			return fmt.Errorf("failed to promote TQ page %d: %w", pid, err)
		}

		i += toCopy
	}

	idx.vectorCount = newCount
	idx.tqStride = stride
	idx.tqBitsAngle = bitsPerAngle
	return nil
}

func (idx *CUDAIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
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

	pow2 := 1
	for pow2 < idx.dim {
		pow2 <<= 1
	}

	start := time.Now()

	// Part 4: Use explicit CUDA stream instead of nil
	var cStream C.cudaStream_t
	if idx.handle != nil {
		cStream = idx.handle.streams[0]
	}

	// Upload query to GPU
	dQuery, err := idx.allocGPUMem(int64(idx.dim * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate query GPU memory: %w", err)
	}
	defer idx.freeGPUMem(dQuery)
	C.cudaMemcpy(dQuery, unsafe.Pointer(&vector[0]), C.size_t(idx.dim*4), C.cudaMemcpyHostToDevice)

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage

	// Collect resident pages (same pattern as FP32 Search)
	type pageEntry struct {
		ptr   unsafe.Pointer
		nvecs int
	}
	pages := make([]pageEntry, 0, numChunks)
	var pinnedPages []*memory.PageInfo
	defer func() {
		for _, pi := range pinnedPages {
			idx.pager.Unpin(pi)
		}
	}()

	for chunk := 0; chunk < numChunks; chunk++ {
		pid := idx.pageIDFor(3, chunk)
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

	if len(pages) == 0 {
		return nil, nil, fmt.Errorf("no resident TQ pages available")
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

	// Part 1: Single batched kernel launch for all pages
	C.launch_turboquant_distance_kernel_v2_batched(
		(**C.float)(dPagePtrs),
		(*C.int)(dPageStarts),
		(*C.float)(dQuery),
		(*C.float)(dAllDists),
		C.int(idx.dim),
		C.int(pow2),
		C.int(bitsPerAngle),
		C.int(totalVecs),
		C.int(numPages),
		cStream,
	)

	if err := GetLastError(); err != nil {
		return nil, nil, fmt.Errorf("TQ batched kernel launch failed (dim=%d pow2=%d): %w", idx.dim, pow2, err)
	}

	// Transfer results
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
		all = append(all, scored{dist: d, pos: i})
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

	metrics.RecordGPUSearch(time.Since(start), "cuda_tq", k)
	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) SearchGreedy(query []float32, entryPoint uint32, entryDist float32) (uint32, float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return 0, 0, fmt.Errorf("index is closed")
	}

	if idx.handle == nil || idx.handle.graphOffsets == nil {
		return entryPoint, entryDist, nil
	}
	if idx.pager == nil {
		return entryPoint, entryDist, nil
	}
	if len(query) != idx.dim {
		return 0, 0, fmt.Errorf("query dimension %d does not match index dimension %d", len(query), idx.dim)
	}

	pow2 := 1
	for pow2 < idx.dim {
		pow2 <<= 1
	}

	// Part 4: Use explicit CUDA stream
	var cStream C.cudaStream_t
	if idx.handle != nil {
		cStream = idx.handle.streams[0]
	}

	// Upload query to GPU
	dQuery, err := idx.allocGPUMem(int64(idx.dim * 4))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate query GPU memory: %w", err)
	}
	defer idx.freeGPUMem(dQuery)
	C.cudaMemcpy(dQuery, unsafe.Pointer(&query[0]), C.size_t(idx.dim*4), C.cudaMemcpyHostToDevice)

	// Collect resident TQ pages
	n := idx.vectorCount
	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage

	type pageEntry struct {
		ptr   unsafe.Pointer
		nvecs int
	}
	pages := make([]pageEntry, 0, numChunks)
	var pinnedPages []*memory.PageInfo
	defer func() {
		for _, pi := range pinnedPages {
			idx.pager.Unpin(pi)
		}
	}()

	for chunk := 0; chunk < numChunks; chunk++ {
		pid := idx.pageIDFor(3, chunk)
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

	if len(pages) == 0 {
		return entryPoint, entryDist, nil
	}

	numPages := len(pages)
	hPageStarts := make([]C.int, numPages+1)
	hPagePtrs := make([]unsafe.Pointer, numPages)
	for i, p := range pages {
		hPagePtrs[i] = p.ptr
		hPageStarts[i+1] = hPageStarts[i] + C.int(p.nvecs)
	}
	totalVecs := int(hPageStarts[numPages])

	// Upload page pointers and page starts to GPU
	dPagePtrs, err := idx.allocGPUMem(int64(numPages) * int64(unsafe.Sizeof(hPagePtrs[0])))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate page pointers: %w", err)
	}
	defer idx.freeGPUMem(dPagePtrs)
	C.cudaMemcpy(dPagePtrs, unsafe.Pointer(&hPagePtrs[0]), C.size_t(numPages)*C.size_t(unsafe.Sizeof(hPagePtrs[0])), C.cudaMemcpyHostToDevice)

	dPageStarts, err := idx.allocGPUMem(int64((numPages + 1) * 4))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate page starts: %w", err)
	}
	defer idx.freeGPUMem(dPageStarts)
	C.cudaMemcpy(dPageStarts, unsafe.Pointer(&hPageStarts[0]), C.size_t((numPages+1)*4), C.cudaMemcpyHostToDevice)

	// Allocate device-side entry point and distance
	dEntryPoint, err := idx.allocGPUMem(4)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate entry point: %w", err)
	}
	defer idx.freeGPUMem(dEntryPoint)
	ep := entryPoint
	C.cudaMemcpy(dEntryPoint, unsafe.Pointer(&ep), 4, C.cudaMemcpyHostToDevice)

	dEntryDist, err := idx.allocGPUMem(4)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate entry dist: %w", err)
	}
	defer idx.freeGPUMem(dEntryDist)
	ed := entryDist
	C.cudaMemcpy(dEntryDist, unsafe.Pointer(&ed), 4, C.cudaMemcpyHostToDevice)

	// Launch greedy descent kernel
	start := time.Now()
	C.launch_turboquant_greedy_descent_kernel(
		(*C.float)(dQuery),
		(**C.float)(dPagePtrs),
		(*C.int)(dPageStarts),
		(*C.uint32_t)(idx.handle.graphOffsets),
		(*C.uint32_t)(idx.handle.graphNeighbors),
		(*C.uint32_t)(dEntryPoint),
		(*C.float)(dEntryDist),
		C.int(idx.dim),
		C.int(pow2),
		C.int(idx.tqBitsAngle),
		C.int(totalVecs),
		C.int(numPages),
		cStream,
	)

	if err := GetLastError(); err != nil {
		return 0, 0, fmt.Errorf("greedy descent kernel launch failed: %w", err)
	}

	// Read back results
	var resultEP uint32
	var resultDist float32
	C.cudaMemcpy(unsafe.Pointer(&resultEP), dEntryPoint, 4, C.cudaMemcpyDeviceToHost)
	C.cudaMemcpy(unsafe.Pointer(&resultDist), dEntryDist, 4, C.cudaMemcpyDeviceToHost)

	metrics.RecordGPUSearch(time.Since(start), "cuda_tq_greedy", 1)

	// Convert local vector index to global ID
	if int(resultEP) < len(idx.idList) {
		resultEP = uint32(idx.idList[resultEP])
	}

	return resultEP, resultDist, nil
}
