//go:build gpu && linux

package memory

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

/*
#include <cuda_runtime.h>
#include <stdlib.h>
*/
import "C"

type GPUMemoryPool struct {
	deviceID         int
	smallBuffers     map[int]*sync.Pool
	largeAllocations map[int][]byte
	mu               sync.RWMutex
	totalAllocated   int64
	maxMemory        int64
	hitCount         int64
	missCount        int64
}

var gpuMemoryPools map[int]*GPUMemoryPool
var gpuMemoryPoolsMu sync.RWMutex

const (
	SmallBufferThreshold = 64 * 1024 // 64KB
	NumSmallSizes        = 16
)

func GetGPUMemoryPool(deviceID int) *GPUMemoryPool {
	gpuMemoryPoolsMu.RLock()
	pool, exists := gpuMemoryPools[deviceID]
	gpuMemoryPoolsMu.RUnlock()

	if exists {
		return pool
	}

	gpuMemoryPoolsMu.Lock()
	defer gpuMemoryPoolsMu.Unlock()

	pool = &GPUMemoryPool{
		deviceID:         deviceID,
		smallBuffers:     make(map[int]*sync.Pool),
		largeAllocations: make(map[int][]byte),
		maxMemory:        8 * 1024 * 1024 * 1024, // 8GB default
	}

	for size := 4096; size <= SmallBufferThreshold; size *= 2 {
		pool.smallBuffers[size] = &sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		}
	}

	if gpuMemoryPools == nil {
		gpuMemoryPools = make(map[int]*GPUMemoryPool)
	}
	gpuMemoryPools[deviceID] = pool

	return pool
}

func (p *GPUMemoryPool) Allocate(size int) []byte {
	if size <= SmallBufferThreshold {
		for s := 4096; s <= SmallBufferThreshold; s *= 2 {
			if s >= size {
				atomic.AddInt64(&p.hitCount, 1)
				return p.smallBuffers[s].Get().([]byte)[:size]
			}
		}
	}

	atomic.AddInt64(&p.missCount, 1)

	allocated := atomic.AddInt64(&p.totalAllocated, int64(size))
	if allocated > p.maxMemory {
		atomic.AddInt64(&p.totalAllocated, -int64(size))
		return make([]byte, size)
	}

	return make([]byte, size)
}

func (p *GPUMemoryPool) Deallocate(buf []byte, size int) {
	if size <= SmallBufferThreshold {
		for s := 4096; s <= SmallBufferThreshold; s *= 2 {
			if s >= size {
				p.smallBuffers[s].Put(buf[:s])
				return
			}
		}
	}

	atomic.AddInt64(&p.totalAllocated, -int64(size))
}

func (p *GPUMemoryPool) Stats() (totalAllocated int64, hitCount int64, missCount int64) {
	return p.totalAllocated, p.hitCount, p.missCount
}

func (p *GPUMemoryPool) HitRate() float64 {
	total := p.hitCount + p.missCount
	if total == 0 {
		return 0
	}
	return float64(p.hitCount) / float64(total)
}

type PinnedMemoryPool struct {
	pool      *sync.Pool
	pageSize  int
	allocated int64
	mu        sync.Mutex
}

var pinnedPools map[int]*PinnedMemoryPool
var pinnedPoolsMu sync.RWMutex

func GetPinnedMemoryPool(deviceID int) *PinnedMemoryPool {
	pinnedPoolsMu.RLock()
	pool, exists := pinnedPools[deviceID]
	pinnedPoolsMu.RUnlock()

	if exists {
		return pool
	}

	pinnedPoolsMu.Lock()
	defer pinnedPoolsMu.Unlock()

	pool = &PinnedMemoryPool{
		pageSize: 4096,
	}

	pool.pool = &sync.Pool{
		New: func() interface{} {
			return pool.allocatePage()
		},
	}

	if pinnedPools == nil {
		pinnedPools = make(map[int]*PinnedMemoryPool)
	}
	pinnedPools[deviceID] = pool

	return pool
}

func (p *PinnedMemoryPool) allocatePage() []byte {
	cBuf := C.calloc(C.size_t(p.pageSize), C.size_t(1))
	if cBuf == nil {
		return make([]byte, p.pageSize)
	}
	buf := unsafe.Slice((*byte)(cBuf), p.pageSize)
	return buf
}

func (p *PinnedMemoryPool) Get(size int) []byte {
	if size <= p.pageSize {
		return p.pool.Get().([]byte)[:size]
	}

	numPages := (size + p.pageSize - 1) / p.pageSize
	p.mu.Lock()
	p.allocated += int64(numPages * p.pageSize)
	p.mu.Unlock()

	buf := make([]byte, numPages*p.pageSize)
	cBuf := C.calloc(C.size_t(numPages), C.size_t(p.pageSize))
	if cBuf != nil {
		go func() {
			C.free(cBuf)
		}()
	}
	return buf[:size]
}

func (p *PinnedMemoryPool) Put(buf []byte) {
	if cap(buf) == p.pageSize {
		C.free(unsafe.Pointer(&buf[0]))
		p.pool.Put(buf)
	} else if cap(buf) > p.pageSize {
		p.mu.Lock()
		p.allocated -= int64(cap(buf))
		p.mu.Unlock()
	}
}

type TransferBuffer struct {
	cpuBuf   []byte
	gpuBuf   unsafe.Pointer
	size     int
	isPinned bool
	deviceID int
	stream   unsafe.Pointer
}

func NewTransferBuffer(size int, deviceID int) (*TransferBuffer, error) {
	tb := &TransferBuffer{
		size:     size,
		deviceID: deviceID,
	}

	tb.cpuBuf = GetPinnedMemoryPool(deviceID).Get(size)
	tb.isPinned = true

	result := C.cudaMalloc(&tb.gpuBuf, C.size_t(size))
	if result != C.cudaSuccess {
		return nil, fmt.Errorf("failed to allocate GPU memory: %v", result)
	}

	return tb, nil
}

func (tb *TransferBuffer) CPUBuffer() []byte {
	return tb.cpuBuf
}

func (tb *TransferBuffer) GPUBuffer() unsafe.Pointer {
	return tb.gpuBuf
}

func (tb *TransferBuffer) Size() int {
	return tb.size
}

func (tb *TransferBuffer) CopyToGPU() error {
	result := C.cudaMemcpy(tb.gpuBuf, unsafe.Pointer(&tb.cpuBuf[0]), C.size_t(tb.size), C.cudaMemcpyHostToDevice)
	if result != C.cudaSuccess {
		return fmt.Errorf("failed to copy to GPU: %v", result)
	}
	return nil
}

func (tb *TransferBuffer) CopyFromGPU() error {
	result := C.cudaMemcpy(unsafe.Pointer(&tb.cpuBuf[0]), tb.gpuBuf, C.size_t(tb.size), C.cudaMemcpyDeviceToHost)
	if result != C.cudaSuccess {
		return fmt.Errorf("failed to copy from GPU: %v", result)
	}
	return nil
}

func (tb *TransferBuffer) Close() error {
	if tb.cpuBuf != nil && tb.isPinned {
		GetPinnedMemoryPool(tb.deviceID).Put(tb.cpuBuf)
		tb.cpuBuf = nil
	}

	if tb.gpuBuf != nil {
		C.cudaFree(tb.gpuBuf)
		tb.gpuBuf = nil
	}

	return nil
}
