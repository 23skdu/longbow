//go:build gpu && linux && cgo

package cuda

/*
#include "cuda_kernels_decl.h"
*/
import "C"

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
)

func (idx *CUDAIndex) Add(ids []int64, vectors []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	if len(vectors)%idx.dim != 0 {
		return fmt.Errorf("vector data length %d not divisible by dimension %d", len(vectors), idx.dim)
	}

	n := len(vectors) / idx.dim
	if len(ids) != n {
		return fmt.Errorf("id count %d does not match vector count %d", len(ids), n)
	}

	idx.batchMu.Lock()
	idx.batchIDs = append(idx.batchIDs, ids...)
	idx.batchVectors = append(idx.batchVectors, vectors...)
	batchSize := len(idx.batchIDs)
	idx.batchMu.Unlock()

	if batchSize >= 1000 {
		return idx.Flush()
	}

	return nil
}

func (idx *CUDAIndex) Flush() error {
	if err := idx.acquireGPUOp(); err != nil {
		return fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.batchMu.Lock()
	defer idx.batchMu.Unlock()

	if len(idx.batchIDs) == 0 {
		return nil
	}

	start := time.Now()
	batchCount := len(idx.batchIDs)

	if batchCount > 2147483647 {
		return fmt.Errorf("batch too large")
	}

	if idx.pager == nil {
		return fmt.Errorf("GPU pager not initialized")
	}

	dim := idx.dim
	maxMem := idx.maxMemory
	prevCount := idx.vectorCount
	newCount := prevCount + batchCount

	// Estimate total memory needed with paging
	totalPages := (newCount + vectorsPerPage - 1) / vectorsPerPage
	estimatedMem := int64(totalPages) * int64(vectorsPerPage) * int64(dim) * 4
	if maxMem > 0 && estimatedMem > maxMem {
		return &types.GPUSyncError{
			BatchSize: batchCount,
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("GPU memory limit exceeded: estimated %d bytes, limit %d", estimatedMem, maxMem),
		}
	}

	vecSize := dim * 4 // float32 bytes per vector
	pageVecs := vectorsPerPage

	for i := 0; i < batchCount; {
		globalPos := prevCount + i
		chunk := globalPos / pageVecs
		offset := globalPos % pageVecs
		space := pageVecs - offset
		toCopy := batchCount - i
		if toCopy > space {
			toCopy = space
		}

		pid := idx.pageIDFor(0, chunk)

		// Get or create page in pager
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			var err error
			pi, err = idx.pager.Alloc(pid)
			if err != nil {
				return &types.GPUSyncError{
					BatchSize: batchCount,
					DeviceID:  idx.deviceInfo.DeviceID,
					Cause:     fmt.Errorf("failed to allocate pager page %d: %w", pid, err),
				}
			}
		}

		// Copy vector data to page's CPU buffer
		cpuBuf := idx.pager.GetCPUBuf(pi)
		srcVec := idx.batchVectors[i*int(dim) : (i+toCopy)*int(dim)]
		dstOffset := offset * vecSize
		copy(cpuBuf[dstOffset:dstOffset+toCopy*vecSize], unsafe.Slice((*byte)(unsafe.Pointer(&srcVec[0])), toCopy*vecSize))

		// Promote page to GPU (copies CPU->GPU, evicts LRU if needed)
		if err := idx.pager.Promote(pi); err != nil {
			return &types.GPUSyncError{
				BatchSize: batchCount,
				DeviceID:  idx.deviceInfo.DeviceID,
				Cause:     fmt.Errorf("failed to promote page %d to GPU: %w", pid, err),
			}
		}

		i += toCopy
	}

	// Update tracking
	idx.vectorCount = newCount
	idx.idList = append(idx.idList, idx.batchIDs...)

	duration := time.Since(start)
	metrics.RecordGPUSync(duration, batchCount)

	idx.batchIDs = idx.batchIDs[:0]
	idx.batchVectors = idx.batchVectors[:0]
	idx.lastSyncTime = time.Now()

	return nil
}

func (idx *CUDAIndex) AddPQ(ids []int64, codes []byte, m int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}
	if len(ids) > 2147483647 || m > 2147483647 {
		return fmt.Errorf("ids or M too large")
	}
	if idx.pager == nil {
		return fmt.Errorf("GPU pager not initialized")
	}

	prevCount := idx.vectorCount
	newCount := prevCount + len(ids)
	codeBytesPerVec := m

	pageVecs := vectorsPerPage

	idx.idList = append(idx.idList, ids...)

	for i := 0; i < len(ids); {
		globalPos := prevCount + i
		chunk := globalPos / pageVecs
		offset := globalPos % pageVecs
		space := pageVecs - offset
		toCopy := len(ids) - i
		if toCopy > space {
			toCopy = space
		}

		pid := idx.pageIDFor(2, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			var err error
			pi, err = idx.pager.Alloc(pid)
			if err != nil {
				return fmt.Errorf("failed to allocate pager page for PQ chunk %d: %w", chunk, err)
			}
		}

		cpuBuf := idx.pager.GetCPUBuf(pi)
		srcStart := i * codeBytesPerVec
		srcEnd := (i + toCopy) * codeBytesPerVec
		dstStart := offset * codeBytesPerVec
		copy(cpuBuf[dstStart:dstStart+toCopy*codeBytesPerVec], codes[srcStart:srcEnd])

		if err := idx.pager.Promote(pi); err != nil {
			return fmt.Errorf("failed to promote PQ page %d: %w", pid, err)
		}

		i += toCopy
	}

	idx.vectorCount = newCount
	idx.pqM = m
	return nil
}

// allocGPUMem allocates GPU memory through the pool so the pager's eviction
// mechanism knows about it. Falls back to raw cudaMalloc if no pool is available.
