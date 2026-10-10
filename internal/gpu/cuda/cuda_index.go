//go:build gpu && linux && cgo

package cuda

/*
#cgo LDFLAGS: -lcudart -lcublas -Wl,--no-as-needed -lm -Wl,--as-needed ${SRCDIR}/kernels.o
#include "cuda_kernels_decl.h"
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/pq"
	"golang.org/x/sync/semaphore"
)

const vectorsPerPage = 1024

type chunkTracker struct {
	startChunk int
	numChunks  int
}

type CUDAIndex struct {
	handle     *C.CUDAIndexHandle
	dim        int
	mu         sync.RWMutex
	closed     bool
	memPool    *memory.GPUMemPool
	pager      *memory.GPUPager
	deviceInfo *types.GPUInfo
	pqEncoder  *pq.PQEncoder // CPU fallback for PQ operations

	batchIDs     []int64
	batchVectors []float32
	batchMu      sync.Mutex
	lastSyncTime time.Time
	syncTicker   *time.Ticker
	stopSync     chan struct{}

	maxMemory  int64
	usedMemory int64
	pinnedPool *PinnedHostPool

	// opSem limits concurrent GPU operations to prevent VRAM oversubscription.
	opSem *semaphore.Weighted

	vectorCount int
	idList      []int64
	pqM         int // PQ subquantizer count
	tqStride    int // TQ bytes per vector
	tqBitsAngle int // TQ bits per angle

	// chunk tracking per dtype
	fp32Chunks chunkTracker
	fp16Chunks chunkTracker
	pqChunks   chunkTracker
	tqChunks   chunkTracker

	// PageID base for each dtype to avoid collisions
	nextPageID memory.PageID
}

// pageIDFor returns a unique PageID for a given dtype and chunk index.
func (idx *CUDAIndex) pageIDFor(dtype int, chunk int) memory.PageID {
	return memory.PageID(dtype)*1_000_000_000 + memory.PageID(chunk)
}

// NewCUDAIndex creates a new CUDA index for the given configuration.
func NewCUDAIndex(cfg types.GPUConfig) (types.Index, error) {
	return NewCUDAIndexImpl(cfg)
}

func NewCUDAIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	if cfg.Dimension <= 0 {
		return nil, &types.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  types.BackendCUDA,
			Cause:    fmt.Errorf("dimension must be positive, got %d", cfg.Dimension),
		}
	}

	if err := SetDevice(cfg.DeviceID); err != nil {
		return nil, &types.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  types.BackendCUDA,
			Cause:    err,
		}
	}

	initialCapacity := 10000
	if cfg.Dimension > 2147483647 || initialCapacity > 2147483647 {
		return nil, fmt.Errorf("dimension or capacity too large")
	}
	handle := C.cuda_init(C.int(cfg.Dimension)) // #nosec G115
	if handle == nil {
		return nil, &types.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  types.BackendCUDA,
			Cause:    fmt.Errorf("failed to initialize CUDA device"),
		}
	}

	nameBuf := make([]C.char, 256)
	var totalMem C.uint64_t
	C.cuda_get_device_info(handle, &nameBuf[0], C.int(len(nameBuf)), &totalMem) // #nosec G115

	maxVRAM := cfg.MaxMemory
	if maxVRAM <= 0 {
		maxVRAM = int64(totalMem) // use all available GPU memory
	}

	pageSize := int64(vectorsPerPage) * int64(cfg.Dimension) * 4

	maxGPUOps := runtime.GOMAXPROCS(0)
	if maxGPUOps < 4 {
		maxGPUOps = 4
	}
	idx := &CUDAIndex{
		handle: handle,
		dim:    cfg.Dimension,
		deviceInfo: &types.GPUInfo{
			Backend:  types.BackendCUDA,
			Name:     C.GoString(&nameBuf[0]),
			DeviceID: cfg.DeviceID,
			MemoryMB: int64(totalMem) / (1024 * 1024), // #nosec G115 -- safe division
		},
		lastSyncTime: time.Now(),
		stopSync:     make(chan struct{}),
		maxMemory:    maxVRAM,
		opSem:        semaphore.NewWeighted(int64(maxGPUOps)),
		pinnedPool:   NewPinnedHostPool(),
	}

	pool, err := memory.NewGPUMemPool(types.BackendCUDA, cfg.DeviceID)
	if err == nil {
		pool.SetTotalMemory(maxVRAM)
		idx.memPool = pool
		idx.pager = memory.NewGPUPager(pool, maxVRAM, pageSize)
	}

	idx.startSyncTicker(cfg)

	runtime.SetFinalizer(idx, (*CUDAIndex).Close)
	return idx, nil
}

func (idx *CUDAIndex) allocGPUMem(size int64) (unsafe.Pointer, error) {
	if idx.memPool != nil {
		return idx.memPool.AllocateGPU(size)
	}
	var ptr unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&ptr)), C.size_t(size)); ret != C.cudaSuccess {
		return nil, fmt.Errorf("cudaMalloc failed")
	}
	return ptr, nil
}

// freeGPUMem frees GPU memory allocated by allocGPUMem.
func (idx *CUDAIndex) freeGPUMem(ptr unsafe.Pointer) error {
	if idx.memPool != nil {
		return idx.memPool.FreeGPU(ptr)
	}
	C.cudaFree(ptr)
	return nil
}

// acquireGPUOp blocks until a GPU operation slot is available or context is cancelled.
// Callers MUST defer releaseGPUOp.
func (idx *CUDAIndex) acquireGPUOp() error {
	if idx.opSem == nil {
		return nil
	}
	return idx.opSem.Acquire(context.Background(), 1)
}

// releaseGPUOp releases a GPU operation slot.
func (idx *CUDAIndex) releaseGPUOp() {
	if idx.opSem == nil {
		return
	}
	idx.opSem.Release(1)
}

func (idx *CUDAIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.syncTicker != nil {
		idx.syncTicker.Stop()
		close(idx.stopSync)
	}

	idx.Flush()

	if idx.pager != nil {
		idx.pager.Close()
	}

	if idx.pinnedPool != nil {
		idx.pinnedPool.Close()
		idx.pinnedPool = nil
	}

	if idx.memPool != nil {
		idx.memPool.Close()
	}

	if idx.handle != nil {
		C.cuda_cleanup(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}

func (idx *CUDAIndex) Backend() types.GPUBackend {
	return types.BackendCUDA
}

func (idx *CUDAIndex) DeviceID() int32 {
	return idx.deviceInfo.DeviceID
}

func (idx *CUDAIndex) GetDeviceInfo() (*types.GPUInfo, error) {
	return idx.deviceInfo, nil
}

func (idx *CUDAIndex) GetMemoryInfo() (total, free, used int64, err error) {
	if idx.memPool != nil {
		total = idx.memPool.GetTotalMemory()
		used = idx.memPool.GetUsedMemory()
		free = total - used
		return
	}
	return idx.deviceInfo.MemoryMB * 1024 * 1024, 0, 0, nil
}

func (idx *CUDAIndex) GetDeviceCount() int {
	return GetDeviceCount()
}

func (idx *CUDAIndex) GetUtilization() (float32, error) {
	return 50.0, nil
}

func (idx *CUDAIndex) Initialize(deviceID int32) error {
	return SetDevice(deviceID)
}

func (idx *CUDAIndex) startSyncTicker(cfg types.GPUConfig) {
	if cfg.SyncInterval <= 0 {
		return
	}

	idx.syncTicker = time.NewTicker(cfg.SyncInterval)
	go func() {
		for {
			select {
			case <-idx.syncTicker.C:
				idx.batchMu.Lock()
				if len(idx.batchIDs) > 0 && time.Since(idx.lastSyncTime) >= cfg.SyncInterval {
					idx.Flush()
				}
				idx.batchMu.Unlock()
			case <-idx.stopSync:
				return
			}
		}
	}()
}

func (idx *CUDAIndex) Clear() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	idx.batchMu.Lock()
	idx.batchIDs = idx.batchIDs[:0]
	idx.batchVectors = idx.batchVectors[:0]
	idx.batchMu.Unlock()

	idx.vectorCount = 0
	idx.idList = idx.idList[:0]
	return nil
}

func (idx *CUDAIndex) Reset() error {
	return idx.Clear()
}

func (idx *CUDAIndex) Sync() error {
	return idx.Flush()
}
