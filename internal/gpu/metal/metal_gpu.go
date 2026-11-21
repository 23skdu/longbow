//go:build gpu && darwin && arm64 && cgo

package metal

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation

#include "metal_bridge.h"
*/
import "C"

import (
	"embed"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
)

//go:embed kernels.metallib
var metalFS embed.FS

func (m *MetalIndex) Init(dimensions, initialCapacity int) error {
	libData, err := metalFS.ReadFile("kernels.metallib")
	if err != nil {
		return fmt.Errorf("failed to read embedded metal library: %w", err)
	}

	if err := InitGlobalContext(libData); err != nil {
		return err
	}

	ctx := GetContext()
	handle := (*C.MetalIndexHandle)(C.malloc(C.sizeof_MetalIndexHandle))
	handle.device = ctx.GetDevice()
	handle.commandQueue = ctx.GetCommandQueue()
	handle.vectorBuffer = nil
	handle.idBuffer = nil
	handle.vectorCount = 0
	handle.dimensions = C.int(dimensions)
	handle.capacity = C.int(initialCapacity)
	if handle.capacity <= 0 {
		handle.capacity = 10000
	}

	// Load pipelines from shared context
	handle.pqPipeline, _ = ctx.GetPipelineState("compute_pq_distances")
	handle.assignPipeline, _ = ctx.GetPipelineState("assign_to_clusters")
	handle.fusedGraphPipeline, _ = ctx.GetPipelineState("graph_rag_fused")
	handle.l2DistancePipeline, _ = ctx.GetPipelineState("vector_distance_l2")
	handle.ipDistancePipeline, _ = ctx.GetPipelineState("vector_distance_ip")
	handle.quantizeSQ8Pipeline, _ = ctx.GetPipelineState("quantize_sq8")
	handle.sigmoidPipeline, _ = ctx.GetPipelineState("sigmoid_f32")
	handle.haversinePipeline, _ = ctx.GetPipelineState("haversine_batch")
	handle.normPipeline, _ = ctx.GetPipelineState("norm_batch_f32")
	handle.sumCentroidsPipeline, _ = ctx.GetPipelineState("sum_centroids")
	handle.finalizeCentroidsPipeline, _ = ctx.GetPipelineState("finalize_centroids")
	handle.prunePipeline, _ = ctx.GetPipelineState("hnsw_prune_neighbors")
	// Note: bfsExpandPipeline and actPropagatePipeline are legacy, merged into fusedGraphPipeline
	handle.bfsExpandPipeline = nil
	handle.actPropagatePipeline = nil

	handle.graphOffsets = nil
	handle.graphNeighbors = nil
	handle.graphWeights = nil
	handle.graphNodeCount = 0
	handle.graphEdgeCount = 0

	// Initialize pruning scratch buffers
	handle.pruneScratchCapacity = 1024 // Sufficient for most HNSW pruning tasks
	handle.pruneCandIdBuffer = C.create_shared_buffer(handle.device, C.size_t(handle.pruneScratchCapacity)*C.sizeof_uint32_t)
	handle.pruneCandDistBuffer = C.create_shared_buffer(handle.device, C.size_t(handle.pruneScratchCapacity)*C.sizeof_float)
	handle.pruneSelIdBuffer = C.create_shared_buffer(handle.device, C.size_t(handle.pruneScratchCapacity)*C.sizeof_uint32_t)
	handle.pruneSelCountBuffer = C.create_shared_buffer(handle.device, C.sizeof_uint32_t)

	bufferSize := C.size_t(handle.capacity) * C.size_t(dimensions) * C.sizeof_float
	buffer := C.create_shared_buffer(handle.device, bufferSize)
	if buffer != nil {
		handle.vectorBuffer = buffer
	}

	m.handle = handle
	return nil
}

// Heal re-initializes the global Metal context and all indices' pipelines on resource exhaustion or panics
func (m *MetalIndex) Heal() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	libData, err := metalFS.ReadFile("kernels.metallib")
	if err != nil {
		return fmt.Errorf("failed to read metal library for healing: %w", err)
	}

	if err := ResetGlobalContext(libData); err != nil {
		return fmt.Errorf("failed to reset global context for healing: %w", err)
	}

	ctx := GetContext()
	m.handle.device = ctx.GetDevice()
	m.handle.commandQueue = ctx.GetCommandQueue()

	// Re-load all compute pipelines
	m.handle.pqPipeline, _ = ctx.GetPipelineState("compute_pq_distances")
	m.handle.assignPipeline, _ = ctx.GetPipelineState("assign_to_clusters")
	m.handle.fusedGraphPipeline, _ = ctx.GetPipelineState("graph_rag_fused")
	m.handle.l2DistancePipeline, _ = ctx.GetPipelineState("vector_distance_l2")
	m.handle.ipDistancePipeline, _ = ctx.GetPipelineState("vector_distance_ip")
	m.handle.quantizeSQ8Pipeline, _ = ctx.GetPipelineState("quantize_sq8")
	m.handle.sigmoidPipeline, _ = ctx.GetPipelineState("sigmoid_f32")
	m.handle.haversinePipeline, _ = ctx.GetPipelineState("haversine_batch")
	m.handle.normPipeline, _ = ctx.GetPipelineState("norm_batch_f32")
	m.handle.sumCentroidsPipeline, _ = ctx.GetPipelineState("sum_centroids")
	m.handle.finalizeCentroidsPipeline, _ = ctx.GetPipelineState("finalize_centroids")
	m.handle.prunePipeline, _ = ctx.GetPipelineState("hnsw_prune_neighbors")

	return nil
}

type MetalIndex struct {
	handle     *C.MetalIndexHandle
	dim        int
	mu         sync.RWMutex
	closed     bool
	memPool    *memory.GPUMemPool
	deviceInfo *gputypes.GPUInfo
	pqEncoder  *pq.PQEncoder // CPU fallback for PQ operations

	// Batch sync support
	batchIDs     []int64
	batchVectors []float32
	batchMu      sync.Mutex
	lastSyncTime time.Time
	syncTicker   *time.Ticker
	stopSync     chan struct{}

	// Memory management
	maxMemory  int64
	usedMemory int64
}

// NewMetalIndexImpl creates a new Metal-based GPU index with integrated memory pool
func NewMetalIndexImpl(cfg gputypes.GPUConfig) (gputypes.Index, error) {
	if cfg.Dimension <= 0 {
		return nil, &gputypes.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  gputypes.BackendMetal,
			Cause:    fmt.Errorf("dimension must be positive, got %d", cfg.Dimension),
		}
	}

	idx := &MetalIndex{
		dim: cfg.Dimension,
		deviceInfo: &gputypes.GPUInfo{
			Backend:  gputypes.BackendMetal,
			DeviceID: cfg.DeviceID,
		},
		lastSyncTime: time.Now(),
		stopSync:     make(chan struct{}),
		maxMemory:    cfg.MaxMemory,
	}

	if err := idx.Init(cfg.Dimension, 10000); err != nil {
		return nil, &gputypes.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  gputypes.BackendMetal,
			Cause:    err,
		}
	}

	// Get actual device info
	nameBuf := make([]C.char, 256)
	var totalMem C.uint64_t
	C.metal_get_device_info(idx.handle, &nameBuf[0], C.int(len(nameBuf)), &totalMem)
	idx.deviceInfo.Name = C.GoString(&nameBuf[0])
	idx.deviceInfo.MemoryMB = int64(totalMem) / (1024 * 1024)

	// Initialize memory pool
	pool, err := memory.NewGPUMemPool(gputypes.BackendMetal, cfg.DeviceID)
	if err == nil {
		idx.memPool = pool
	}

	// Start batch sync ticker
	idx.startSyncTicker(cfg)

	runtime.SetFinalizer(idx, (*MetalIndex).Close)
	return idx, nil
}

// Add adds vectors to the Metal GPU index with batching support
func (idx *MetalIndex) Add(ids []int64, vectors []float32) error {
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

	// Add to batch
	idx.batchMu.Lock()
	idx.batchIDs = append(idx.batchIDs, ids...)
	idx.batchVectors = append(idx.batchVectors, vectors...)
	batchSize := len(idx.batchIDs)
	idx.batchMu.Unlock()

	// Flush immediately if batch is large enough
	if batchSize >= 1000 {
		return idx.Flush()
	}

	return nil
}

// Flush synchronizes pending batch to GPU
func (idx *MetalIndex) Flush() error {
	idx.batchMu.Lock()
	defer idx.batchMu.Unlock()

	if len(idx.batchIDs) == 0 {
		return nil
	}

	start := time.Now()
	batchCount := len(idx.batchIDs)

	// Take mutation lock
	idx.mu.Lock()
	defer idx.mu.Unlock()

	currentCount := int(C.metal_get_count(idx.handle))
	currentCapacity := int(idx.handle.capacity)

	requiredCapacity := currentCount + batchCount
	if requiredCapacity > currentCapacity {
		// Estimate new capacity (doubling)
		newCapacity := currentCapacity
		if newCapacity == 0 {
			newCapacity = 10000
		}
		for newCapacity < requiredCapacity {
			newCapacity *= 2
		}

		// Calculate total memory: vectors (float32) + IDs (int64)
		// vectors: newCapacity * dim * 4
		// IDs: newCapacity * 8
		estimatedMem := int64(newCapacity) * int64(idx.dim) * 4
		estimatedMem += int64(newCapacity) * 8

		if idx.maxMemory > 0 && estimatedMem > idx.maxMemory {
			return &gputypes.GPUSyncError{
				BatchSize: batchCount,
				DeviceID:  idx.deviceInfo.DeviceID,
				Cause:     fmt.Errorf("GPU memory limit exceeded: estimated %d bytes, limit %d", estimatedMem, idx.maxMemory),
			}
		}
	}

	ret := C.metal_add_vectors(
		idx.handle,
		(*C.float)(unsafe.Pointer(&idx.batchVectors[0])),
		(*C.int64_t)(unsafe.Pointer(&idx.batchIDs[0])),
		C.int(len(idx.batchIDs)),
	)

	duration := time.Since(start)

	if ret != 0 {
		return &gputypes.GPUSyncError{
			BatchSize: len(idx.batchIDs),
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("failed to add vectors to Metal buffer"),
		}
	}

	// Record metrics
	metrics.RecordGPUSync(duration, len(idx.batchIDs))

	// Clear batch
	idx.batchIDs = idx.batchIDs[:0]
	idx.batchVectors = idx.batchVectors[:0]
	idx.lastSyncTime = time.Now()

	return nil
}

// Search queries the Metal GPU index for k-nearest neighbors
func (idx *MetalIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	// Flush any pending batches before search
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

	// Flush any pending batches before search
	if err := idx.Flush(); err != nil {
		return nil, nil, err
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	// Initialize distances to infinity
	for i := range resultDistances {
		resultDistances[i] = math.MaxFloat32
	}

	start := time.Now()
	ret := C.metal_search(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)

	if ret != 0 {
		// Watchdog: attempt self-healing recovery and retry
		if healErr := idx.Heal(); healErr == nil {
			ret = C.metal_search(
				idx.handle,
				(*C.float)(unsafe.Pointer(&vector[0])),
				C.int(k),
				(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
				(*C.float)(unsafe.Pointer(&resultDistances[0])),
			)
		}
	}

	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, &gputypes.GPUComputeError{
			Operation: "search",
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("Metal search failed after self-healing retry"),
		}
	}

	// Record metrics
	metrics.RecordGPUSearch(duration, "metal", k)

	return resultIDs, resultDistances, nil
}

// Close releases Metal GPU resources
func (idx *MetalIndex) Close() error {
	// Flush pending batches
	idx.Flush()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	// Stop sync ticker
	if idx.syncTicker != nil {
		idx.syncTicker.Stop()
		close(idx.stopSync)
	}

	// Close memory pool
	if idx.memPool != nil {
		idx.memPool.Close()
	}

	if idx.handle != nil {
		C.metal_cleanup(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}

// Backend returns GPU backend type
func (idx *MetalIndex) Backend() gputypes.GPUBackend {
	return gputypes.BackendMetal
}

func (idx *MetalIndex) DeviceID() int32 {
	return idx.deviceInfo.DeviceID
}

// GetDeviceInfo returns information about the GPU device
func (idx *MetalIndex) GetDeviceInfo() (*gputypes.GPUInfo, error) {
	return idx.deviceInfo, nil
}

// GetMemoryInfo returns GPU memory information
func (idx *MetalIndex) GetMemoryInfo() (total, free, used int64, err error) {
	if idx.memPool != nil {
		total = idx.memPool.GetTotalMemory()
		used = idx.memPool.GetUsedMemory()
		free = total - used
		return
	}
	// Fallback to approximate values
	return idx.deviceInfo.MemoryMB * 1024 * 1024, 0, 0, nil
}

// GetDeviceCount returns the number of GPU devices
func (idx *MetalIndex) GetDeviceCount() int {
	return 1
}

// GetUtilization returns GPU utilization (Metal doesn't expose this directly)
func (idx *MetalIndex) GetUtilization() (float32, error) {
	return 50.0, nil
}

// Initialize initializes the GPU device
func (idx *MetalIndex) Initialize(deviceID int32) error {
	return nil
}

// startSyncTicker starts background sync for batch operations
func (idx *MetalIndex) startSyncTicker(cfg gputypes.GPUConfig) {
	if cfg.SyncInterval <= 0 {
		return
	}

	idx.syncTicker = time.NewTicker(cfg.SyncInterval)
	go func() {
		for {
			select {
			case <-idx.syncTicker.C:
				idx.Flush()
			case <-idx.stopSync:
				return
			}
		}
	}()
}
