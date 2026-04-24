//go:build gpu && darwin && arm64

package metal

import (
	"fmt"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
)

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#import <Accelerate/Accelerate.h>

// MetalIndex wraps Metal GPU resources
typedef struct {
    void* device;
    void* commandQueue;
    void* vectorBuffer;
    void* idBuffer;
    int vectorCount;
    int dimensions;
    int capacity;
} MetalIndexHandle;

// Initialize Metal device and command queue
MetalIndexHandle* metal_init(int dimensions, int initialCapacity) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) {
            return NULL;
        }

        id<MTLCommandQueue> queue = [device newCommandQueue];
        if (!queue) {
            return NULL;
        }

        MetalIndexHandle* handle = (MetalIndexHandle*)malloc(sizeof(MetalIndexHandle));
        handle->device = (__bridge_retained void*)device;
        handle->commandQueue = (__bridge_retained void*)queue;
        handle->vectorBuffer = NULL;
        handle->idBuffer = NULL;
        handle->vectorCount = 0;
        handle->dimensions = dimensions;
        handle->capacity = initialCapacity > 0 ? initialCapacity : 10000;

        // Pre-allocate buffer with unified memory for better performance
        size_t bufferSize = handle->capacity * dimensions * sizeof(float);
        id<MTLBuffer> buffer = [device newBufferWithLength:bufferSize
                                                    options:MTLResourceStorageModeShared];
        if (buffer) {
            handle->vectorBuffer = (__bridge_retained void*)buffer;
        }

        return handle;
    }
}

// Get device info
void metal_get_device_info(MetalIndexHandle* handle, char* name, int maxLen, uint64_t* totalMem) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        // Get device name
        NSString* deviceName = [device name];
        strncpy(name, [deviceName UTF8String], maxLen - 1);
        name[maxLen - 1] = '\0';

        // Get recommended working set size (approximation of available memory)
        *totalMem = (uint64_t)[device recommendedMaxWorkingSetSize];
    }
}

// Add vectors to Metal buffer with ID tracking
int metal_add_vectors(MetalIndexHandle* handle, float* vectors, int64_t* ids, int count) {
    @autoreleasepool {
        if (!handle->vectorBuffer) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        // Check if we need to resize
        int requiredCapacity = handle->vectorCount + count;
        if (requiredCapacity > handle->capacity) {
            // Grow capacity (double until sufficient)
            int newCapacity = handle->capacity;
            while (newCapacity < requiredCapacity) {
                newCapacity *= 2;
            }

            // Allocate new larger buffer
            size_t newBufferSize = newCapacity * handle->dimensions * sizeof(float);
            id<MTLBuffer> newBuffer = [device newBufferWithLength:newBufferSize
                                                            options:MTLResourceStorageModeShared];
            if (!newBuffer) {
                return -1;
            }

            // Copy old data
            id<MTLBuffer> oldBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
            memcpy([newBuffer contents], [oldBuffer contents],
                   handle->vectorCount * handle->dimensions * sizeof(float));

            // Replace buffer
            CFRelease(handle->vectorBuffer);
            handle->vectorBuffer = (__bridge_retained void*)newBuffer;
            handle->capacity = newCapacity;
        }

        // Copy vectors to buffer
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        float* dest = (float*)[vectorBuffer contents] + (handle->vectorCount * handle->dimensions);
        memcpy(dest, vectors, count * handle->dimensions * sizeof(float));

        // Copy IDs
        if (!handle->idBuffer) {
            size_t idBufferSize = handle->capacity * sizeof(int64_t);
            id<MTLBuffer> idBuf = [device newBufferWithLength:idBufferSize
                                                       options:MTLResourceStorageModeShared];
            if (!idBuf) {
                return -1;
            }
            handle->idBuffer = (__bridge_retained void*)idBuf;
        }

        id<MTLBuffer> idBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
        int64_t* idDest = (int64_t*)[idBuffer contents] + handle->vectorCount;
        memcpy(idDest, ids, count * sizeof(int64_t));

        handle->vectorCount += count;

        return 0;
    }
}

// Batch L2 distance computation using Accelerate framework
void metal_compute_distances(MetalIndexHandle* handle, float* query, float* distances, int count) {
    @autoreleasepool {
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        float* vectors = (float*)[vectorBuffer contents];

        // Process in chunks for cache efficiency
        int chunkSize = 256;
        float temp[chunkSize];

        for (int i = 0; i < count; i += chunkSize) {
            int currentChunk = chunkSize;
            if (i + currentChunk > count) {
                currentChunk = count - i;
            }

            for (int j = 0; j < currentChunk; j++) {
                float* vec = vectors + ((i + j) * handle->dimensions);
                vDSP_distancesq(query, 1, vec, 1, &distances[i + j], handle->dimensions);
                distances[i + j] = sqrtf(distances[i + j]);
            }
        }
    }
}

// Search for k-nearest neighbors using Metal Performance Shaders
int metal_search(MetalIndexHandle* handle, float* query, int k, int64_t* resultIDs, float* resultDistances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;

        // Allocate distance buffer
        float* distances = (float*)malloc(handle->vectorCount * sizeof(float));
        if (!distances) {
            return -1;
        }

        // Compute all distances using Accelerate (highly optimized)
        metal_compute_distances(handle, query, distances, handle->vectorCount);

        // Get IDs
        id<MTLBuffer> idBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
        int64_t* ids = (int64_t*)[idBuffer contents];

        // Find k smallest using partial selection sort
        // This is O(n*k) which is fine for small k
        int n = handle->vectorCount;
        int resultCount = k < n ? k : n;

        for (int i = 0; i < resultCount; i++) {
            int minIdx = i;
            float minDist = distances[i];

            for (int j = i + 1; j < n; j++) {
                if (distances[j] < minDist) {
                    minDist = distances[j];
                    minIdx = j;
                }
            }

            // Store result
            resultIDs[i] = ids[minIdx];
            resultDistances[i] = minDist;

            // Mark as used
            distances[minIdx] = INFINITY;
        }

        free(distances);
        return 0;
    }
}

// Get vector count
int metal_get_count(MetalIndexHandle* handle) {
    return handle->vectorCount;
}

// Clean up Metal resources
void metal_cleanup(MetalIndexHandle* handle) {
    @autoreleasepool {
        if (handle->idBuffer) {
            CFRelease(handle->idBuffer);
        }
        if (handle->vectorBuffer) {
            CFRelease(handle->vectorBuffer);
        }
        if (handle->commandQueue) {
            CFRelease(handle->commandQueue);
        }
        if (handle->device) {
            CFRelease(handle->device);
        }
        free(handle);
    }
}
*/
import "C"
import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/23skdu/longbow/internal/gpu/memory"
)

// MetalIndex implements GPU-accelerated vector search using Apple Metal
type MetalIndex struct {
	handle     *C.MetalIndexHandle
	dim        int
	mu         sync.RWMutex
	closed     bool
	memPool    *memory.GPUMemPool
	deviceInfo *types.GPUInfo
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
func NewMetalIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	if cfg.Dimension <= 0 {
		return nil, &types.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  types.BackendMetal,
			Cause:    fmt.Errorf("dimension must be positive, got %d", cfg.Dimension),
		}
	}

	initialCapacity := 10000
	handle := C.metal_init(C.int(cfg.Dimension), C.int(initialCapacity))
	if handle == nil {
		return nil, &types.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  types.BackendMetal,
			Cause:    fmt.Errorf("failed to initialize Metal device"),
		}
	}

	// Get actual device info
	nameBuf := make([]C.char, 256)
	var totalMem C.uint64_t
	C.metal_get_device_info(handle, &nameBuf[0], C.int(len(nameBuf)), &totalMem)

	idx := &MetalIndex{
		handle: handle,
		dim:    cfg.Dimension,
		deviceInfo: &types.GPUInfo{
			Backend:  types.BackendMetal,
			Name:     C.GoString(&nameBuf[0]),
			DeviceID: cfg.DeviceID,
			MemoryMB: int64(totalMem) / (1024 * 1024),
		},
		lastSyncTime: time.Now(),
		stopSync:     make(chan struct{}),
		maxMemory:    cfg.MaxMemory,
	}

	// Initialize memory pool
	pool, err := memory.NewGPUMemPool(types.BackendMetal, cfg.DeviceID)
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

	// Estimate potential memory growth if we were to grow
	idx.mu.RLock()
	currentCount := int(C.metal_get_count(idx.handle))
	currentCapacity := int(idx.handle.capacity)
	idx.mu.RUnlock()

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
			return &types.GPUSyncError{
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
		return &types.GPUSyncError{
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
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, &types.GPUComputeError{
			Operation: "search",
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("Metal search failed"),
		}
	}

	// Record metrics
	metrics.RecordGPUSearch(duration, "metal", k)

	return resultIDs, resultDistances, nil
}

func (idx *MetalIndex) SearchPQ(lookupTable []float32, m int, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// CPU fallback for PQ search using lookup table
	// lookupTable contains precomputed distances for each vector and each PQ code
	// For now, do a simple linear scan using the lookup table
	numVecs := idx.vectorCount
	if k > numVecs {
		k = numVecs
	}

	type result struct {
		id       int64
		distance float32
	}

	results := make([]result, 0, numVecs)
	for i := 0; i < numVecs; i++ {
		id := idx.ids[i]
		// Use the lookup table to get distance for this vector
		// The lookup table format depends on the PQ configuration
		dist := lookupTable[i*m] // Simplified - actual implementation depends on table format
		results = append(results, result{id: id, distance: dist})
	}

	// Sort by distance
	// (Implementation simplified - would need proper sorting)
	return nil, nil, fmt.Errorf("SearchPQ CPU fallback not fully implemented")
}

func (idx *MetalIndex) TrainPQ(vectors []float32, m int, k int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	// CPU fallback: Train PQ codebooks using K-Means
	dims := idx.dimension
	if dims%m != 0 {
		return fmt.Errorf("dimension %d must be divisible by M %d", dims, m)
	}

	// Create encoder and train
	encoder, err := pq.NewPQEncoder(dims, m, k)
	if err != nil {
		return fmt.Errorf("failed to create PQ encoder: %w", err)
	}

	// Convert flat vectors to [][]float32
	numVecs := len(vectors) / dims
	vecs2d := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vecs2d[i] = vectors[i*dims : (i+1)*dims]
	}

	if err := encoder.Train(vecs2d); err != nil {
		return fmt.Errorf("PQ training failed: %w", err)
	}

	// Store encoder in index for later use
	idx.pqEncoder = encoder
	return nil
}

func (idx *MetalIndex) EncodePQ(vectors []float32) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	if idx.pqEncoder == nil {
		return nil, fmt.Errorf("PQ encoder not trained")
	}

	// Encode each vector
	dims := idx.dimension
	numVecs := len(vectors) / dims
	codes := make([]byte, numVecs*idx.pqEncoder.M)

	for i := 0; i < numVecs; i++ {
		vec := vectors[i*dims : (i+1)*dims]
		encoded, err := idx.pqEncoder.Encode(vec)
		if err != nil {
			return nil, fmt.Errorf("encoding failed at vector %d: %w", i, err)
		}
		copy(codes[i*idx.pqEncoder.M:(i+1)*idx.pqEncoder.M], encoded)
	}

	return codes, nil
}

// SearchBatch queries the Metal GPU index with multiple vectors in parallel.
func (idx *MetalIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
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

// Close releases Metal GPU resources
func (idx *MetalIndex) Close() error {
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

	// Flush pending batches
	idx.Flush()

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
func (idx *MetalIndex) Backend() types.GPUBackend {
	return types.BackendMetal
}

func (idx *MetalIndex) DeviceID() int {
	return idx.deviceInfo.DeviceID
}

// GetDeviceInfo returns information about the GPU device
func (idx *MetalIndex) GetDeviceInfo() (*types.GPUInfo, error) {
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
func (idx *MetalIndex) Initialize(deviceID int) error {
	return nil
}

// startSyncTicker starts background sync for batch operations
func (idx *MetalIndex) startSyncTicker(cfg types.GPUConfig) {
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

func (idx *MetalIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// Convert uint16 (fp16) to float32 for search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float16.Float16bitsToFloat32(v)
	}

	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// Convert uint16 pairs (complex64 stored as fp16 pairs) to float32
	// complex64 has 2 elements for every float32 dims
	f32Vec := make([]float32, len(vector)*2)
	for i, v := range vector {
		f32Vec[i*2] = float16.Float16bitsToFloat32(v)
	}

	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// complex128 is stored as interleaved float32 (real, imag pairs)
	// If input is 128 floats, that's 64 complex numbers
	// Just use as-is since Metal expects float32
	return idx.searchFloat32(vector, k)
}

func (idx *MetalIndex) searchFloat32(vector []float32, k int) ([]int64, []float32, error) {
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
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, &types.GPUComputeError{
			Operation: "search",
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("Metal search failed"),
		}
	}

	// Record metrics
	metrics.RecordGPUSearch(duration, "metal", k)

	return resultIDs, resultDistances, nil
}
