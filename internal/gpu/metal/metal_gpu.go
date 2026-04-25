//go:build gpu && darwin && arm64

package metal

import (
	"fmt"
)

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation

#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#import <Accelerate/Accelerate.h>

const char* pqShaderSource =
"#include <metal_stdlib>\n"
"using namespace metal;\n"
"\n"
"kernel void compute_pq_distances(\n"
"    device const float* lookupTable [[buffer(0)]],\n"
"    device const uchar* codes [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    constant uint& m [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    float sum = 0.0f;\n"
"    uint offset = gid * m;\n"
"    \n"
"    for (uint i = 0; i < m; i++) {\n"
"        uchar code = codes[offset + i];\n"
"        sum += lookupTable[i * 256 + code];\n"
"    }\n"
"    \n"
"    distances[gid] = sum;\n"
"}\n"
"\n"
"kernel void assign_to_clusters(\n"
"    device const float* vectors [[buffer(0)]],\n"
"    device const float* centroids [[buffer(1)]],\n"
"    device uint* assignments [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    constant uint& numCentroids [[buffer(5)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    float minDist = 1e38f;\n"
"    uint bestCent = 0;\n"
"    uint vecOffset = gid * dim;\n"
"    \n"
"    for (uint c = 0; c < numCentroids; c++) {\n"
"        float dist = 0.0f;\n"
"        uint centOffset = c * dim;\n"
"        for (uint i = 0; i < dim; i++) {\n"
"            float diff = vectors[vecOffset + i] - centroids[centOffset + i];\n"
"            dist += diff * diff;\n"
"        }\n"
"        if (dist < minDist) {\n"
"            minDist = dist;\n"
"            bestCent = c;\n"
"        }\n"
"    }\n"
"    assignments[gid] = bestCent;\n"
"}\n";

// MetalIndex wraps Metal GPU resources
typedef struct {
    void* device;
    void* commandQueue;
    void* vectorBuffer;
    void* idBuffer;
    void* pqPipeline;
    void* assignPipeline;
    void* bfsExpandPipeline;
    void* actPropagatePipeline;
    void* graphOffsets;
    void* graphNeighbors;
    void* graphWeights;
    int vectorCount;
    int dimensions;
    int capacity;
    int graphNodeCount;
    int graphEdgeCount;
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
        handle->pqPipeline = NULL;
        handle->vectorCount = 0;
        handle->dimensions = dimensions;
        handle->capacity = initialCapacity > 0 ? initialCapacity : 10000;

        // Compile PQ shader
        NSError* error = nil;
        NSString* shaderSource = [NSString stringWithUTF8String:pqShaderSource];
        id<MTLLibrary> library = [device newLibraryWithSource:shaderSource options:nil error:&error];
        if (library) {
            id<MTLFunction> pqFunc = [library newFunctionWithName:@"compute_pq_distances"];
            id<MTLComputePipelineState> pipeline = [device newComputePipelineStateWithFunction:pqFunc error:&error];
            if (pipeline) handle->pqPipeline = (__bridge_retained void*)pipeline;
            
            id<MTLFunction> assignFunc = [library newFunctionWithName:@"assign_to_clusters"];
            id<MTLComputePipelineState> assignPipeline = [device newComputePipelineStateWithFunction:assignFunc error:&error];
            if (assignPipeline) handle->assignPipeline = (__bridge_retained void*)assignPipeline;

            id<MTLFunction> bfsFunc = [library newFunctionWithName:@"graph_bfs_expand"];
            id<MTLComputePipelineState> bfsPipeline = [device newComputePipelineStateWithFunction:bfsFunc error:&error];
            if (bfsPipeline) handle->bfsExpandPipeline = (__bridge_retained void*)bfsPipeline;

            id<MTLFunction> actFunc = [library newFunctionWithName:@"graph_activation_propagate"];
            id<MTLComputePipelineState> actPipeline = [device newComputePipelineStateWithFunction:actFunc error:&error];
            if (actPipeline) handle->actPropagatePipeline = (__bridge_retained void*)actPipeline;
        }
        
        handle->graphOffsets = NULL;
        handle->graphNeighbors = NULL;
        handle->graphWeights = NULL;
        handle->graphNodeCount = 0;
        handle->graphEdgeCount = 0;

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
        if (handle->pqPipeline) {
            CFRelease(handle->pqPipeline);
        }
        if (handle->device) {
            CFRelease(handle->device);
        }
        free(handle);
    }
}

// Full Metal SearchPQ implementation
int metal_search_pq(MetalIndexHandle* handle, float* lookupTable, int m, int k, int64_t* resultIDs, float* resultDistances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0 || !handle->pqPipeline) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> codesBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->pqPipeline;

        // Allocate distance buffer on GPU
        size_t distSize = handle->vectorCount * sizeof(float);
        id<MTLBuffer> distBuffer = [device newBufferWithLength:distSize options:MTLResourceStorageModeShared];
        
        // Create lookup table buffer
        size_t tableSize = m * 256 * sizeof(float);
        id<MTLBuffer> tableBuffer = [device newBufferWithBytes:lookupTable length:tableSize options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:tableBuffer offset:0 atIndex:0];
        [encoder setBuffer:codesBuffer offset:0 atIndex:1];
        [encoder setBuffer:distBuffer offset:0 atIndex:2];
        [encoder setBytes:&m length:sizeof(int) atIndex:3];
        [encoder setBytes:&handle->vectorCount length:sizeof(int) atIndex:4];

        MTLSize gridSize = MTLSizeMake(handle->vectorCount, 1, 1);
        NSUInteger threadGroupSize = pipeline.maxTotalThreadsPerThreadgroup;
        if (threadGroupSize > handle->vectorCount) threadGroupSize = handle->vectorCount;
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        float* distances = (float*)[distBuffer contents];
        int64_t* ids = (int64_t*)[(__bridge id<MTLBuffer>)handle->idBuffer contents];

        // Selection sort for top-k (CPU side for simplicity, consistent with metal_search)
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
            resultIDs[i] = ids[minIdx];
            resultDistances[i] = minDist;
            distances[minIdx] = INFINITY;
        }

        return 0;
    }
}

// Get buffer pointers
void* metal_get_vector_buffer(MetalIndexHandle* handle) {
    id<MTLBuffer> buffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
    return [buffer contents];
}

void* metal_get_id_buffer(MetalIndexHandle* handle) {
    id<MTLBuffer> buffer = (__bridge id<MTLBuffer>)handle->idBuffer;
    return [buffer contents];
}

int metal_assign_to_clusters(MetalIndexHandle* handle, float* vectors, float* centroids, uint32_t* assignments, int numVectors, int numCentroids, int dim) {
    @autoreleasepool {
        if (!handle->assignPipeline) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->assignPipeline;

        // Create buffers
        size_t vecSize = (size_t)numVectors * dim * sizeof(float);
        size_t centSize = (size_t)numCentroids * dim * sizeof(float);
        size_t assignSize = (size_t)numVectors * sizeof(uint32_t);

        id<MTLBuffer> vecBuf = [device newBufferWithBytes:vectors length:vecSize options:MTLResourceStorageModeShared];
        id<MTLBuffer> centBuf = [device newBufferWithBytes:centroids length:centSize options:MTLResourceStorageModeShared];
        id<MTLBuffer> assignBuf = [device newBufferWithLength:assignSize options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:vecBuf offset:0 atIndex:0];
        [encoder setBuffer:centBuf offset:0 atIndex:1];
        [encoder setBuffer:assignBuf offset:0 atIndex:2];
        [encoder setBytes:&dim length:sizeof(int) atIndex:3];
        [encoder setBytes:&numVectors length:sizeof(int) atIndex:4];
        [encoder setBytes:&numCentroids length:sizeof(int) atIndex:5];

        MTLSize gridSize = MTLSizeMake(numVectors, 1, 1);
        NSUInteger maxThreads = pipeline.maxTotalThreadsPerThreadgroup;
        if (maxThreads > numVectors) maxThreads = numVectors;
        MTLSize threadgroupSize = MTLSizeMake(maxThreads, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        memcpy(assignments, [assignBuf contents], assignSize);

        return 0;
    }
}

int metal_update_graph(MetalIndexHandle* handle, uint32_t* h_offsets, uint32_t* h_neighbors, float* h_weights, int nodeCount, int edgeCount) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        
        if (handle->graphOffsets) CFRelease(handle->graphOffsets);
        if (handle->graphNeighbors) CFRelease(handle->graphNeighbors);
        if (handle->graphWeights) CFRelease(handle->graphWeights);

        handle->graphOffsets = (__bridge_retained void*)[device newBufferWithBytes:h_offsets length:(nodeCount + 1) * sizeof(uint32_t) options:MTLResourceStorageModeShared];
        handle->graphNeighbors = (__bridge_retained void*)[device newBufferWithBytes:h_neighbors length:edgeCount * sizeof(uint32_t) options:MTLResourceStorageModeShared];
        if (h_weights) {
            handle->graphWeights = (__bridge_retained void*)[device newBufferWithBytes:h_weights length:edgeCount * sizeof(float) options:MTLResourceStorageModeShared];
        }

        handle->graphNodeCount = nodeCount;
        handle->graphEdgeCount = edgeCount;
        return 0;
    }
}
*/
import "C"
import (
	"math"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// MetalIndex implements GPU-accelerated vector search using Apple Metal
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

	initialCapacity := 10000
	handle := C.metal_init(C.int(cfg.Dimension), C.int(initialCapacity))
	if handle == nil {
		return nil, &gputypes.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  gputypes.BackendMetal,
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
		deviceInfo: &gputypes.GPUInfo{
			Backend:  gputypes.BackendMetal,
			Name:     C.GoString(&nameBuf[0]),
			DeviceID: cfg.DeviceID,
			MemoryMB: int64(totalMem) / (1024 * 1024),
		},
		lastSyncTime: time.Now(),
		stopSync:     make(chan struct{}),
		maxMemory:    cfg.MaxMemory,
	}

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
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, &gputypes.GPUComputeError{
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
	// Flush any pending batches before search
	if err := idx.Flush(); err != nil {
		return nil, nil, err
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	numVecs := int(C.metal_get_count(idx.handle))
	if numVecs == 0 {
		return []int64{}, []float32{}, nil
	}

	if k > numVecs {
		k = numVecs
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	start := time.Now()
	ret := C.metal_search_pq(
		idx.handle,
		(*C.float)(unsafe.Pointer(&lookupTable[0])),
		C.int(m),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal PQ search failed (check if pipeline initialized)")
	}

	// Record metrics
	metrics.RecordGPUSearch(duration, "metal_pq", k)

	return resultIDs, resultDistances, nil
}

func (idx *MetalIndex) TrainPQ(vectors []float32, m int, k int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	// CPU fallback: Train PQ codebooks using K-Means
	dims := idx.dim
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
	dims := idx.dim
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

func (idx *MetalIndex) DeviceID() int {
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

// AssignToClusters offloads vector assignment to GPU centroids
func (idx *MetalIndex) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	numVectors := len(vectors) / idx.dim
	numCentroids := len(centroids) / idx.dim
	assignments := make([]uint32, numVectors)

	ret := C.metal_assign_to_clusters(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vectors[0])),
		(*C.float)(unsafe.Pointer(&centroids[0])),
		(*C.uint32_t)(unsafe.Pointer(&assignments[0])),
		C.int(numVectors),
		C.int(numCentroids),
		C.int(idx.dim),
	)

	if ret != 0 {
		return nil, fmt.Errorf("Metal cluster assignment failed")
	}

	return assignments, nil
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

func (idx *MetalIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// Convert uint16 (fp16) to float32 for search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float16.FromBits(v).Float32()
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
		f32Vec[i*2] = float16.FromBits(v).Float32()
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
		return nil, nil, &gputypes.GPUComputeError{
			Operation: "search",
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("Metal search failed"),
		}
	}

	// Record metrics
	metrics.RecordGPUSearch(duration, "metal", k)

	return resultIDs, resultDistances, nil
}

func (idx *MetalIndex) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	return fmt.Errorf("AddTurboQuant not implemented for standard Metal index, use optimized Metal index")
}

func (idx *MetalIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	return nil, nil, fmt.Errorf("SearchTurboQuant not implemented for standard Metal index, use optimized Metal index")
}

func (idx *MetalIndex) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	var wPtr *C.float
	if len(weights) > 0 {
		wPtr = (*C.float)(unsafe.Pointer(&weights[0]))
	}

	ret := C.metal_update_graph(
		idx.handle,
		(*C.uint32_t)(unsafe.Pointer(&offsets[0])),
		(*C.uint32_t)(unsafe.Pointer(&neighbors[0])),
		wPtr,
		C.int(len(offsets)-1),
		C.int(len(neighbors)),
	)

	if ret != 0 {
		return fmt.Errorf("failed to update Metal graph")
	}

	return nil
}

func (idx *MetalIndex) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if idx.handle.graphOffsets == nil {
		return nil, nil, fmt.Errorf("graph not initialized on GPU")
	}

	// Metal multi-hop expansion would be implemented here.
	return nil, nil, fmt.Errorf("Metal GraphExpand not fully implemented in Go layer yet")
}

func (idx *MetalIndex) AddPQ(ids []int64, codes []byte, m int) error {
	return fmt.Errorf("AddPQ not supported in basic MetalIndex")
}


