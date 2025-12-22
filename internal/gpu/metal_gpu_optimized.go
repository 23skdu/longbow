//go:build gpu && darwin && arm64

package gpu

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#import <Accelerate/Accelerate.h>

// Metal shader source for L2 distance calculation and top-k selection
const char* metalShaderSource =
"#include <metal_stdlib>\n"
"using namespace metal;\n"
"\n"
"// Compute L2 distances between query and all vectors\n"
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
"}\n"
"\n"
"// Parallel reduction to find top-k (simplified version)\n"
"kernel void find_top_k(\n"
"    device const float* distances [[buffer(0)]],\n"
"    device int* indices [[buffer(1)]],\n"
"    device float* topDistances [[buffer(2)]],\n"
"    constant uint& numVectors [[buffer(3)]],\n"
"    constant uint& k [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    // Simple selection for now - can be optimized with parallel reduction\n"
"    if (gid == 0) {\n"
"        for (uint i = 0; i < k; i++) {\n"
"            uint minIdx = i;\n"
"            float minDist = distances[i];\n"
"            \n"
"            for (uint j = i + 1; j < numVectors; j++) {\n"
"                if (distances[j] < minDist) {\n"
"                    minDist = distances[j];\n"
"                    minIdx = j;\n"
"                }\n"
"            }\n"
"            \n"
"            indices[i] = minIdx;\n"
"            topDistances[i] = minDist;\n"
"            \n"
"            // Swap to move min to position i\n"
"            if (minIdx != i) {\n"
"                float temp = distances[i];\n"
"                distances[i] = distances[minIdx];\n"
"                distances[minIdx] = temp;\n"
"            }\n"
"        }\n"
"    }\n"
"}\n";

// MetalIndexOptimized wraps Metal GPU resources with compute shaders
typedef struct {
    void* device;
    void* commandQueue;
    void* vectorBuffer;
    void* distanceComputePipeline;
    void* topKPipeline;
    int vectorCount;
    int dimensions;
} MetalIndexOptimized;

// Initialize Metal device with compute shaders
MetalIndexOptimized* metal_init_optimized(int dimensions) {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) {
            return NULL;
        }

        id<MTLCommandQueue> queue = [device newCommandQueue];
        if (!queue) {
            return NULL;
        }

        // Compile Metal shaders
        NSError* error = nil;
        NSString* shaderSource = [NSString stringWithUTF8String:metalShaderSource];
        id<MTLLibrary> library = [device newLibraryWithSource:shaderSource options:nil error:&error];
        if (!library) {
            NSLog(@"Failed to compile Metal shaders: %@", error);
            return NULL;
        }

        // Create compute pipelines
        id<MTLFunction> distanceFunc = [library newFunctionWithName:@"compute_l2_distances"];
        id<MTLFunction> topKFunc = [library newFunctionWithName:@"find_top_k"];

        id<MTLComputePipelineState> distancePipeline = [device newComputePipelineStateWithFunction:distanceFunc error:&error];
        id<MTLComputePipelineState> topKPipeline = [device newComputePipelineStateWithFunction:topKFunc error:&error];

        if (!distancePipeline || !topKPipeline) {
            NSLog(@"Failed to create compute pipelines: %@", error);
            return NULL;
        }

        MetalIndexOptimized* handle = (MetalIndexOptimized*)malloc(sizeof(MetalIndexOptimized));
        handle->device = (__bridge_retained void*)device;
        handle->commandQueue = (__bridge_retained void*)queue;
        handle->vectorBuffer = NULL;
        handle->distanceComputePipeline = (__bridge_retained void*)distancePipeline;
        handle->topKPipeline = (__bridge_retained void*)topKPipeline;
        handle->vectorCount = 0;
        handle->dimensions = dimensions;

        return handle;
    }
}

// Add vectors using optimized path
int metal_add_vectors_optimized(MetalIndexOptimized* handle, float* vectors, int count) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        size_t bufferSize = count * handle->dimensions * sizeof(float);
        id<MTLBuffer> buffer = [device newBufferWithBytes:vectors
                                                   length:bufferSize
                                                  options:MTLResourceStorageModeShared];

        if (!buffer) {
            return -1;
        }

        if (handle->vectorBuffer) {
            CFRelease(handle->vectorBuffer);
        }

        handle->vectorBuffer = (__bridge_retained void*)buffer;
        handle->vectorCount = count;

        return 0;
    }
}

// Search using Metal compute shaders
int metal_search_optimized(MetalIndexOptimized* handle, float* query, int k, int64_t* resultIDs, float* resultDistances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        id<MTLComputePipelineState> distancePipeline = (__bridge id<MTLComputePipelineState>)handle->distanceComputePipeline;
        id<MTLComputePipelineState> topKPipeline = (__bridge id<MTLComputePipelineState>)handle->topKPipeline;

        // Create buffers
        id<MTLBuffer> queryBuffer = [device newBufferWithBytes:query
                                                        length:handle->dimensions * sizeof(float)
                                                       options:MTLResourceStorageModeShared];

        id<MTLBuffer> distanceBuffer = [device newBufferWithLength:handle->vectorCount * sizeof(float)
                                                            options:MTLResourceStorageModeShared];

        id<MTLBuffer> indicesBuffer = [device newBufferWithLength:k * sizeof(int)
                                                           options:MTLResourceStorageModeShared];

        id<MTLBuffer> topDistancesBuffer = [device newBufferWithLength:k * sizeof(float)
                                                                options:MTLResourceStorageModeShared];

        // Create command buffer
        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        // Compute distances
        [encoder setComputePipelineState:distancePipeline];
        [encoder setBuffer:queryBuffer offset:0 atIndex:0];
        [encoder setBuffer:vectorBuffer offset:0 atIndex:1];
        [encoder setBuffer:distanceBuffer offset:0 atIndex:2];
        [encoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:3];
        [encoder setBytes:&handle->vectorCount length:sizeof(uint32_t) atIndex:4];

        MTLSize gridSize = MTLSizeMake(handle->vectorCount, 1, 1);
        NSUInteger threadGroupSize = distancePipeline.maxTotalThreadsPerThreadgroup;
        if (threadGroupSize > handle->vectorCount) threadGroupSize = handle->vectorCount;
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];

        // Find top-k
        [encoder setComputePipelineState:topKPipeline];
        [encoder setBuffer:distanceBuffer offset:0 atIndex:0];
        [encoder setBuffer:indicesBuffer offset:0 atIndex:1];
        [encoder setBuffer:topDistancesBuffer offset:0 atIndex:2];
        [encoder setBytes:&handle->vectorCount length:sizeof(uint32_t) atIndex:3];
        [encoder setBytes:&k length:sizeof(uint32_t) atIndex:4];

        [encoder dispatchThreads:MTLSizeMake(1, 1, 1) threadsPerThreadgroup:MTLSizeMake(1, 1, 1)];

        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        // Copy results
        int* indices = (int*)[indicesBuffer contents];
        float* distances = (float*)[topDistancesBuffer contents];

        for (int i = 0; i < k; i++) {
            resultIDs[i] = indices[i];
            resultDistances[i] = distances[i];
        }

        return 0;
    }
}

// Cleanup
void metal_cleanup_optimized(MetalIndexOptimized* handle) {
    @autoreleasepool {
        if (handle->vectorBuffer) {
            CFRelease(handle->vectorBuffer);
        }
        if (handle->topKPipeline) {
            CFRelease(handle->topKPipeline);
        }
        if (handle->distanceComputePipeline) {
            CFRelease(handle->distanceComputePipeline);
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
	"runtime"
	"sync"
	"unsafe"
)

// MetalIndexOptimized implements GPU-accelerated vector search using Metal compute shaders
type MetalIndexOptimized struct {
	handle *C.MetalIndexOptimized
	dim    int
	mu     sync.RWMutex
	closed bool
}

// NewMetalIndexOptimized creates an optimized Metal-based GPU index with compute shaders
func NewMetalIndexOptimized(cfg GPUConfig) (Index, error) {
	handle := C.metal_init_optimized(C.int(cfg.Dimension))
	if handle == nil {
		return nil, fmt.Errorf("failed to initialize optimized Metal device")
	}

	idx := &MetalIndexOptimized{
		handle: handle,
		dim:    cfg.Dimension,
	}

	runtime.SetFinalizer(idx, (*MetalIndexOptimized).Close)
	return idx, nil
}

// Add adds vectors to the optimized Metal GPU index
func (idx *MetalIndexOptimized) Add(ids []int64, vectors []float32) error {
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

	ret := C.metal_add_vectors_optimized(idx.handle, (*C.float)(unsafe.Pointer(&vectors[0])), C.int(n))
	if ret != 0 {
		return fmt.Errorf("failed to add vectors to optimized Metal buffer")
	}

	return nil
}

// Search queries the optimized Metal GPU index using compute shaders
func (idx *MetalIndexOptimized) Search(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.metal_search_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("optimized Metal search failed")
	}

	return resultIDs, resultDistances, nil
}

// Close releases optimized Metal GPU resources
func (idx *MetalIndexOptimized) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.handle != nil {
		C.metal_cleanup_optimized(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}
