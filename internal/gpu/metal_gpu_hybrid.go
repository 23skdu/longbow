//go:build gpu && darwin && arm64

package gpu

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
	"runtime"
	"sync"
	"unsafe"
)

// MetalHybridIndex uses GPU for distances, CPU for selection
type MetalHybridIndex struct {
	handle *C.MetalHybridIndex
	dim    int
	mu     sync.RWMutex
	closed bool
}

// NewMetalHybridIndex creates a hybrid Metal/CPU index
func NewMetalHybridIndex(cfg GPUConfig) (Index, error) {
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
