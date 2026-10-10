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
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (idx *CUDAIndex) SearchInt8(vector []int8, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert int8 to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchUint8(vector []uint8, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert uint8 to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchInt16(vector []int16, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert int16 to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchUint16(vector []uint16, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert uint16 to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert float16 query to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float16.FromBits(v).Float32()
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// complex64 queries arrive as interleaved float16 bit patterns [re, im].
	// Stored vectors are float32 (Add); decode fp16 bits → float32 for the kernel.
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float16.FromBits(v).Float32()
	}

	if len(f32Vec) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(f32Vec), idx.dim)
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
	if k > n {
		k = n
	}

	start := time.Now()

	var cStream C.cudaStream_t
	if idx.handle != nil {
		cStream = idx.handle.streams[0]
	}

	qBytes := int64(idx.dim * 4)
	dQuery, err := idx.allocGPUMem(qBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate query GPU memory: %w", err)
	}
	defer idx.freeGPUMem(dQuery)
	C.cudaMemcpy(dQuery, unsafe.Pointer(&f32Vec[0]), C.size_t(qBytes), C.cudaMemcpyHostToDevice)

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
		return nil, nil, fmt.Errorf("no resident pages available for search")
	}

	totalVecs := 0
	for _, p := range pages {
		totalVecs += p.nvecs
	}

	dAllDists, err := idx.allocGPUMem(int64(totalVecs * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate distance buffer: %w", err)
	}
	defer idx.freeGPUMem(dAllDists)

	// Per-page launch: complex kernels expect a flat vectors base, not page-pointer arrays.
	offset := 0
	for _, p := range pages {
		distPtr := unsafe.Pointer(uintptr(dAllDists) + uintptr(offset*4)) // #nosec G115
		C.launch_l2_distance_complex64_kernel(
			(*C.float)(p.ptr),
			(*C.float)(dQuery),
			(*C.float)(distPtr),
			C.int(idx.dim),
			C.int(p.nvecs),
			cStream,
		)
		offset += p.nvecs
	}

	hAllDists := make([]float32, totalVecs)
	distBytes := int64(totalVecs * 4)
	C.cudaMemcpy(unsafe.Pointer(&hAllDists[0]), dAllDists, C.size_t(distBytes), C.cudaMemcpyDeviceToHost)

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

	duration := time.Since(start)
	metrics.RecordGPUSearch(duration, "cuda", k)

	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
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
	if k > n {
		k = n
	}

	start := time.Now()

	var cStream C.cudaStream_t
	if idx.handle != nil {
		cStream = idx.handle.streams[0]
	}

	qBytes := int64(idx.dim * 4)
	dQuery, err := idx.allocGPUMem(qBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate query GPU memory: %w", err)
	}
	defer idx.freeGPUMem(dQuery)
	C.cudaMemcpy(dQuery, unsafe.Pointer(&vector[0]), C.size_t(qBytes), C.cudaMemcpyHostToDevice)

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
		return nil, nil, fmt.Errorf("no resident pages available for search")
	}

	totalVecs := 0
	for _, p := range pages {
		totalVecs += p.nvecs
	}

	dAllDists, err := idx.allocGPUMem(int64(totalVecs * 4))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate distance buffer: %w", err)
	}
	defer idx.freeGPUMem(dAllDists)

	// Per-page launch: complex kernels expect a flat vectors base, not page-pointer arrays.
	// (Previously dPagePtrs was passed as vectors — broken for multi-page and wrong for single-page.)
	offset := 0
	for _, p := range pages {
		distPtr := unsafe.Pointer(uintptr(dAllDists) + uintptr(offset*4)) // #nosec G115
		C.launch_l2_distance_complex128_kernel(
			(*C.float)(p.ptr),
			(*C.float)(dQuery),
			(*C.float)(distPtr),
			C.int(idx.dim),
			C.int(p.nvecs),
			cStream,
		)
		offset += p.nvecs
	}

	hAllDists := make([]float32, totalVecs)
	distBytes := int64(totalVecs * 4)
	C.cudaMemcpy(unsafe.Pointer(&hAllDists[0]), dAllDists, C.size_t(distBytes), C.cudaMemcpyDeviceToHost)

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

	duration := time.Since(start)
	metrics.RecordGPUSearch(duration, "cuda", k)

	return resultIDs, resultDistances, nil
}
