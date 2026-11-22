//go:build gpu && darwin && arm64 && cgo

package metal

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation

#include "metal_bridge.h"
*/
import "C"
import (
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
)

const vectorsPerPage = 4096

// MetalIndexOptimized implements GPU-accelerated vector search using Metal compute shaders
type MetalIndexOptimized struct {
	handle         *C.MetalIndexOptimized
	dim            int
	mu             sync.RWMutex
	closed         bool
	pqEncoder      *pq.PQEncoder
	graphOffsets   []uint32
	graphNeighbors []uint32
	graphWeights   []float32

	// Paging and Memory
	memPool     *memory.GPUMemPool
	pager       *memory.GPUPager
	vectorCount int
	idList      []int64

	// Batching
	batchMu      sync.Mutex
	batchIDs     []int64
	batchVectors []float32
	maxMemory    int64
	lastSyncTime time.Time
	syncTicker   *time.Ticker
	stopSync     chan struct{}
}

// NewMetalIndexOptimized creates an optimized Metal-based GPU index with compute shaders
func NewMetalIndexOptimized(cfg types.GPUConfig) (types.Index, error) {
	if err := initMetalContextFromEmbedded(); err != nil {
		return nil, err
	}

	ctx := GetContext()
	if ctx == nil {
		return nil, fmt.Errorf("failed to get shared metal context")
	}

	handle := C.metal_init_optimized(C.int(cfg.Dimension))
	if handle == nil {
		return nil, fmt.Errorf("failed to initialize optimized Metal device")
	}

	// Resolve all required pipelines
	l2, err := ctx.GetPipelineState("compute_l2_distances")
	if err != nil {
		return nil, err
	}
	l2Paged, err := ctx.GetPipelineState("compute_l2_distances_paged")
	if err != nil {
		return nil, err
	}
	cosine, err := ctx.GetPipelineState("compute_cosine_similarity_paged")
	if err != nil {
		cosine, err = ctx.GetPipelineState("compute_cosine_similarity")
		if err != nil {
			return nil, err
		}
	}
	dot, err := ctx.GetPipelineState("compute_dot_product_paged")
	if err != nil {
		dot, err = ctx.GetPipelineState("compute_dot_product")
		if err != nil {
			return nil, err
		}
	}
	topK, err := ctx.GetPipelineState("find_top_k_heap")
	if err != nil {
		return nil, err
	}
	l2Fp16, err := ctx.GetPipelineState("compute_l2_distances_fp16")
	if err != nil {
		return nil, err
	}
	cosineFp16, err := ctx.GetPipelineState("compute_cosine_similarity_fp16")
	if err != nil {
		return nil, err
	}
	dotFp16, err := ctx.GetPipelineState("compute_dot_product_fp16")
	if err != nil {
		return nil, err
	}
	l2C128, err := ctx.GetPipelineState("compute_l2_distances_complex128")
	if err != nil {
		return nil, err
	}
	cosineC128, err := ctx.GetPipelineState("compute_cosine_similarity_complex128")
	if err != nil {
		return nil, err
	}
	l2C64, err := ctx.GetPipelineState("compute_l2_distances_complex64")
	if err != nil {
		return nil, err
	}
	cosineC64, err := ctx.GetPipelineState("compute_cosine_similarity_complex64")
	if err != nil {
		return nil, err
	}
	tq, err := ctx.GetPipelineState("compute_tq_distances")
	if err != nil {
		return nil, err
	}
	haversine, err := ctx.GetPipelineState("haversine_batch")
	if err != nil {
		return nil, err
	}
	norm, err := ctx.GetPipelineState("norm_batch_f32")
	if err != nil {
		return nil, err
	}
	prune, err := ctx.GetPipelineState("hnsw_prune_neighbors")
	if err != nil {
		return nil, err
	}
	greedy, err := ctx.GetPipelineState("hnsw_greedy_search")
	if err != nil {
		return nil, err
	}
	greedyTQ, err := ctx.GetPipelineState("hnsw_greedy_search_tq")
	if err != nil {
		return nil, err
	}

	C.metal_set_pipelines_optimized(
		handle,
		ctx.GetDevice(), ctx.GetCommandQueue(),
		l2, l2Paged, cosine, dot, topK,
		l2Fp16, cosineFp16, dotFp16,
		l2C128, cosineC128, l2C64, cosineC64,
		tq, haversine, norm, prune, greedy, greedyTQ,
	)

	maxVRAM := cfg.MaxMemory
	if maxVRAM <= 0 {
		maxVRAM = 1024 * 1024 * 1024 // 1GB default for Metal
	}
	pageSize := int64(vectorsPerPage) * int64(cfg.Dimension) * 4

	idx := &MetalIndexOptimized{
		handle:       handle,
		dim:          cfg.Dimension,
		idList:       make([]int64, 0),
		lastSyncTime: time.Now(),
		stopSync:     make(chan struct{}),
		maxMemory:    maxVRAM,
	}

	pool, err := memory.NewGPUMemPool(types.BackendMetal, cfg.DeviceID)
	if err == nil {
		idx.memPool = pool
		idx.pager = memory.NewGPUPager(pool, maxVRAM, pageSize)
	}

	idx.startSyncTicker(cfg)

	runtime.SetFinalizer(idx, (*MetalIndexOptimized).Close)
	return idx, nil
}

func (idx *MetalIndexOptimized) pageIDFor(dataType int, chunkIdx int) int64 {
	return int64(dataType)<<32 | int64(chunkIdx)
}

func (idx *MetalIndexOptimized) startSyncTicker(cfg types.GPUConfig) {
	interval := cfg.SyncInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	idx.syncTicker = ticker
	go func() {
		for {
			select {
			case <-ticker.C:
				_ = idx.Flush()
			case <-idx.stopSync:
				return
			}
		}
	}()
}

// Flush runs the flush operation.
func (idx *MetalIndexOptimized) Flush() error {
	idx.batchMu.Lock()
	defer idx.batchMu.Unlock()

	if len(idx.batchIDs) == 0 {
		return nil
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
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

	totalPages := (newCount + vectorsPerPage - 1) / vectorsPerPage
	estimatedMem := int64(totalPages) * int64(vectorsPerPage) * int64(dim) * 4
	if maxMem > 0 && estimatedMem > maxMem {
		return &types.GPUSyncError{
			BatchSize: batchCount,
			DeviceID:  0,
			Cause:     fmt.Errorf("GPU memory limit exceeded: estimated %d bytes, limit %d", estimatedMem, maxMem),
		}
	}

	vecSize := dim * 4
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

		pid := memory.PageID(idx.pageIDFor(0, chunk))

		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			var err error
			pi, err = idx.pager.Alloc(pid)
			if err != nil {
				return &types.GPUSyncError{
					BatchSize: batchCount,
					DeviceID:  0,
					Cause:     fmt.Errorf("failed to allocate pager page %d: %w", pid, err),
				}
			}
		}

		cpuBuf := idx.pager.GetCPUBuf(pi)
		srcVec := idx.batchVectors[i*int(dim) : (i+toCopy)*int(dim)]
		dstOffset := offset * vecSize
		copy(cpuBuf[dstOffset:dstOffset+toCopy*vecSize], unsafe.Slice((*byte)(unsafe.Pointer(&srcVec[0])), toCopy*vecSize))

		if err := idx.pager.Promote(pi); err != nil {
			return &types.GPUSyncError{
				BatchSize: batchCount,
				DeviceID:  0,
				Cause:     fmt.Errorf("failed to promote page %d to GPU: %w", pid, err),
			}
		}

		i += toCopy
	}

	idx.vectorCount = newCount
	idx.idList = append(idx.idList, idx.batchIDs...)

	duration := time.Since(start)
	metrics.RecordGPUSync(duration, batchCount)

	idx.batchIDs = idx.batchIDs[:0]
	idx.batchVectors = idx.batchVectors[:0]
	idx.lastSyncTime = time.Now()

	return nil
}

// Add runs the add operation.
func (idx *MetalIndexOptimized) Add(ids []int64, vectors []float32) error {
	idx.mu.RLock()
	closed := idx.closed
	idx.mu.RUnlock()

	if closed {
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

// Len returns the number of vectors in the index
func (idx *MetalIndexOptimized) Len() int {
	idx.batchMu.Lock()
	pending := len(idx.batchIDs)
	idx.batchMu.Unlock()

	idx.mu.RLock()
	defer idx.mu.RUnlock()
	n := idx.vectorCount
	if idx.handle != nil {
		if cCount := int(C.metal_get_count_optimized(idx.handle)); cCount > n {
			n = cCount
		}
	}
	return n + pending
}

// Search queries the optimized Metal GPU index using compute shaders
func (idx *MetalIndexOptimized) Search(vector []float32, k int) ([]int64, []float32, error) {
	if err := idx.Flush(); err != nil {
		return nil, nil, err
	}

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

	if k > 2147483647 {
		return nil, nil, fmt.Errorf("k too large")
	}
	if k > n {
		k = n
	}

	start := time.Now()

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage

	type pageEntry struct {
		ptr   unsafe.Pointer
		nvecs int
	}
	pages := make([]pageEntry, 0, numChunks)
	for chunk := 0; chunk < numChunks; chunk++ {
		pid := memory.PageID(idx.pageIDFor(0, chunk))
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			continue
		}
		if err := idx.pager.Promote(pi); err != nil {
			continue
		}
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

	ids := make([]int64, k)
	distances := make([]float32, k)

	ret := C.metal_search_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		(*unsafe.Pointer)(unsafe.Pointer(&hPagePtrs[0])),
		(*C.int)(unsafe.Pointer(&hPageStarts[0])),
		C.int(numPages),
		C.int(totalVecs),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&ids[0])),
		(*C.float)(unsafe.Pointer(&distances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("failed to search optimized Metal buffer")
	}

	// Because we replaced idBuffer logic directly into idList in Go (like CUDA),
	// the C returned IDs are actually LOCAL offsets within the totalVecs.
	// So we must remap local offsets to global IDs using idx.idList.
	// Wait! I will just use idx.idList in Go!
	for i := 0; i < k; i++ {
		localIdx := int(ids[i])
		if localIdx >= 0 && localIdx < len(idx.idList) {
			ids[i] = idx.idList[localIdx]
		}
	}

	metrics.GPUComputeDurationSeconds.WithLabelValues("Apple Silicon GPU (Optimized)", "search").Observe(time.Since(start).Seconds())

	return ids, distances, nil
}

// SearchBatch runs the search batch operation.
func (idx *MetalIndexOptimized) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
	if len(vectors) == 0 {
		return nil, nil, nil
	}

	// Fallback to sequential for small batches
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

// Close releases optimized Metal GPU resources
func (idx *MetalIndexOptimized) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.syncTicker != nil {
		idx.syncTicker.Stop()
		close(idx.stopSync)
		idx.syncTicker = nil
	}

	if idx.handle != nil {
		C.metal_cleanup_optimized(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}
