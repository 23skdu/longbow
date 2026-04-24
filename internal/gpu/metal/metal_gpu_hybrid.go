//go:build gpu && darwin && arm64

package metal

import "github.com/23skdu/longbow/internal/gpu/types"

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#import <Accelerate/Accelerate.h>

// Metal shader for distance calculation only
const char* hybridShaderSource =
"#include <metal_stdlib>\n"
"using namespace metal;\n"
"\n"
"kernel void compute_l2_distances(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    float sum = 0.0f;\n"
"    uint offset = gid * dim;\n"
"    \n"
"    for (uint i = 0; i < dim; i++) {\n"
"        float diff = query[i] - vectors[offset + i];\n"
"        sum += diff * diff;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sum);\n"
"}\n";

typedef struct {
    void* device;
    void* commandQueue;
    void* vectorBuffer;
    void* distancePipeline;
    int vectorCount;
    int dimensions;
} MetalHybridIndex;

MetalHybridIndex* metal_hybrid_init(int dimensions) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) return NULL;

        id<MTLCommandQueue> queue = [device newCommandQueue];
        if (!queue) return NULL;

        NSError* error = nil;
        NSString* shaderSource = [NSString stringWithUTF8String:hybridShaderSource];
        id<MTLLibrary> library = [device newLibraryWithSource:shaderSource options:nil error:&error];
        if (!library) {
            NSLog(@"Failed to compile hybrid shader: %@", error);
            return NULL;
        }

        id<MTLFunction> distanceFunc = [library newFunctionWithName:@"compute_l2_distances"];
        id<MTLComputePipelineState> pipeline = [device newComputePipelineStateWithFunction:distanceFunc error:&error];
        if (!pipeline) {
            NSLog(@"Failed to create pipeline: %@", error);
            return NULL;
        }

        MetalHybridIndex* handle = (MetalHybridIndex*)malloc(sizeof(MetalHybridIndex));
        handle->device = (__bridge_retained void*)device;
        handle->commandQueue = (__bridge_retained void*)queue;
        handle->vectorBuffer = NULL;
        handle->distancePipeline = (__bridge_retained void*)pipeline;
        handle->vectorCount = 0;
        handle->dimensions = dimensions;

        return handle;
    }
}

int metal_hybrid_add(MetalHybridIndex* handle, float* vectors, int count) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        size_t bufferSize = count * handle->dimensions * sizeof(float);
        id<MTLBuffer> buffer = [device newBufferWithBytes:vectors
                                                   length:bufferSize
                                                  options:MTLResourceStorageModeShared];
        if (!buffer) return -1;

        if (handle->vectorBuffer) {
            CFRelease(handle->vectorBuffer);
        }

        handle->vectorBuffer = (__bridge_retained void*)buffer;
        handle->vectorCount = count;
        return 0;
    }
}

// GPU computes distances, returns them for CPU processing
int metal_hybrid_compute_distances(MetalHybridIndex* handle, float* query, float* distances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0) return -1;

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->distancePipeline;

        id<MTLBuffer> queryBuffer = [device newBufferWithBytes:query
                                                        length:handle->dimensions * sizeof(float)
                                                       options:MTLResourceStorageModeShared];

        id<MTLBuffer> distanceBuffer = [device newBufferWithLength:handle->vectorCount * sizeof(float)
                                                            options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:queryBuffer offset:0 atIndex:0];
        [encoder setBuffer:vectorBuffer offset:0 atIndex:1];
        [encoder setBuffer:distanceBuffer offset:0 atIndex:2];
        [encoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:3];
        [encoder setBytes:&handle->vectorCount length:sizeof(uint32_t) atIndex:4];

        MTLSize gridSize = MTLSizeMake(handle->vectorCount, 1, 1);
        NSUInteger threadGroupSize = pipeline.maxTotalThreadsPerThreadgroup;
        if (threadGroupSize > handle->vectorCount) threadGroupSize = handle->vectorCount;
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        // Copy distances to output
        memcpy(distances, [distanceBuffer contents], handle->vectorCount * sizeof(float));

        return 0;
    }
}

void metal_hybrid_cleanup(MetalHybridIndex* handle) {
    @autoreleasepool {
        if (handle->vectorBuffer) CFRelease(handle->vectorBuffer);
        if (handle->distancePipeline) CFRelease(handle->distancePipeline);
        if (handle->commandQueue) CFRelease(handle->commandQueue);
        if (handle->device) CFRelease(handle->device);
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
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// MetalHybridIndex uses GPU for distances, CPU for selection
type MetalHybridIndex struct {
	handle    *C.MetalHybridIndex
	dim       int
	mu        sync.RWMutex
	closed    bool
	pqEncoder *pq.PQEncoder // CPU fallback for PQ operations
}

// NewMetalHybridIndex creates a hybrid Metal/CPU index
func NewMetalHybridIndex(cfg types.GPUConfig) (types.Index, error) {
	handle := C.metal_hybrid_init(C.int(cfg.Dimension))
	if handle == nil {
		return nil, fmt.Errorf("failed to initialize hybrid Metal device")
	}

	idx := &MetalHybridIndex{
		handle: handle,
		dim:    cfg.Dimension,
	}

	runtime.SetFinalizer(idx, (*MetalHybridIndex).Close)
	return idx, nil
}

func (idx *MetalHybridIndex) Add(ids []int64, vectors []float32) error {
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

	ret := C.metal_hybrid_add(idx.handle, (*C.float)(unsafe.Pointer(&vectors[0])), C.int(n))
	if ret != 0 {
		return fmt.Errorf("failed to add vectors to hybrid Metal buffer")
	}

	return nil
}

func (idx *MetalHybridIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	// Step 1: GPU computes all distances in parallel
	vectorCount := int(C.int(idx.handle.vectorCount))
	distances := make([]float32, vectorCount)

	ret := C.metal_hybrid_compute_distances(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		(*C.float)(unsafe.Pointer(&distances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("hybrid Metal distance computation failed")
	}

	// Step 2: CPU finds top-k using simple selection (could use vDSP for further optimization)
	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	for i := 0; i < k && i < vectorCount; i++ {
		minIdx := i
		minDist := distances[i]

		for j := i + 1; j < vectorCount; j++ {
			if distances[j] < minDist {
				minDist = distances[j]
				minIdx = j
			}
		}

		// Swap
		if minIdx != i {
			distances[i], distances[minIdx] = distances[minIdx], distances[i]
		}

		resultIDs[i] = int64(i)
		resultDistances[i] = distances[i]
	}

	return resultIDs, resultDistances, nil
}

func (idx *MetalHybridIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
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

func (idx *MetalHybridIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.handle != nil {
		C.metal_hybrid_cleanup(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}

func (idx *MetalHybridIndex) Backend() types.GPUBackend {
	return types.BackendMetal
}

func (idx *MetalHybridIndex) DeviceID() int {
	return 0
}

func (idx *MetalHybridIndex) GetDeviceInfo() (*types.GPUInfo, error) {
	return &types.GPUInfo{
		Backend:  types.BackendMetal,
		Name:     "Apple Silicon GPU",
		DeviceID: 0,
	}, nil
}

func (idx *MetalHybridIndex) GetMemoryInfo() (int64, int64, int64, error) {
	return 0, 0, 0, nil
}

func (idx *MetalHybridIndex) SearchPQ(lookupTable []float32, m, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// CPU fallback for PQ search
	vectorCount := int(C.int(idx.handle.vectorCount))
	if k > vectorCount {
		k = vectorCount
	}

	// Simplified: return empty results with proper error for now
	return nil, nil, fmt.Errorf("SearchPQ CPU fallback not fully implemented for hybrid index")
}

func (idx *MetalHybridIndex) TrainPQ(vectors []float32, m, k int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	// CPU fallback: Train PQ codebooks using K-Means
	if idx.dim%m != 0 {
		return fmt.Errorf("dimension %d must be divisible by M %d", idx.dim, m)
	}

	encoder, err := pq.NewPQEncoder(idx.dim, m, k)
	if err != nil {
		return fmt.Errorf("failed to create PQ encoder: %w", err)
	}

	// Convert flat vectors to [][]float32
	numVecs := len(vectors) / idx.dim
	vecs2d := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vecs2d[i] = vectors[i*idx.dim : (i+1)*idx.dim]
	}

	if err := encoder.Train(vecs2d); err != nil {
		return fmt.Errorf("PQ training failed: %w", err)
	}

	idx.pqEncoder = encoder
	return nil
}

func (idx *MetalHybridIndex) EncodePQ(vectors []float32) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	if idx.pqEncoder == nil {
		return nil, fmt.Errorf("PQ encoder not trained")
	}

	numVecs := len(vectors) / idx.dim
	codes := make([]byte, numVecs*idx.pqEncoder.M)

	for i := 0; i < numVecs; i++ {
		vec := vectors[i*idx.dim : (i+1)*idx.dim]
		encoded, err := idx.pqEncoder.Encode(vec)
		if err != nil {
			return nil, fmt.Errorf("encoding failed at vector %d: %w", i, err)
		}
		copy(codes[i*idx.pqEncoder.M:(i+1)*idx.pqEncoder.M], encoded)
	}

	return codes, nil
}

func (idx *MetalHybridIndex) GetUtilization() (float32, error) {
	return 50.0, nil
}

func (idx *MetalHybridIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// Convert uint16 (fp16) to float32 for search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float16.New(float32FromUInt16(v)).Float32()
	}

	return idx.searchFloat32(f32Vec, k)
}

func float16FromUInt16(b uint16) float32 {
	f16 := float16.New(float32(math.Float32frombits(uint32(b))))
	return f16.Float32()
}

func (idx *MetalHybridIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// Convert uint16 pairs (complex64 stored as fp16 pairs) to float32
	f32Vec := make([]float32, len(vector)*2)
	for i, v := range vector {
		f32Vec[i*2] = float16.New(float32(math.Float32frombits(uint32(v)))).Float32()
	}

	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalHybridIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// complex128 stored as interleaved float32 - just use as-is
	return idx.searchFloat32(vector, k)
}

func (idx *MetalHybridIndex) searchFloat32(vector []float32, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	// Step 1: GPU computes all distances in parallel
	vectorCount := int(C.int(idx.handle.vectorCount))
	distances := make([]float32, vectorCount)

	start := time.Now()
	ret := C.metal_hybrid_compute_distances(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		(*C.float)(unsafe.Pointer(&distances[0])),
	)
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, fmt.Errorf("hybrid Metal distance computation failed")
	}

	// Record metrics
	metrics.RecordGPUSearch(duration, "metal_hybrid", k)

	// Step 2: CPU finds top-k using simple selection
	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	for i := 0; i < k && i < vectorCount; i++ {
		minIdx := i
		minDist := distances[i]

		for j := i + 1; j < vectorCount; j++ {
			if distances[j] < minDist {
				minDist = distances[j]
				minIdx = j
			}
		}

		if minIdx != i {
			distances[i], distances[minIdx] = distances[minIdx], distances[i]
		}

		resultIDs[i] = int64(i)
		resultDistances[i] = distances[i]
	}

	return resultIDs, resultDistances, nil
}
