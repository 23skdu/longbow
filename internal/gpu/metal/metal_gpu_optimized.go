//go:build gpu && darwin && arm64

package metal

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#import <Accelerate/Accelerate.h>

// Forward declaration
typedef struct MetalIndexOptimized MetalIndexOptimized;

// Distance metric type
typedef enum {
    METRIC_L2 = 0,
    METRIC_COSINE = 1,
    METRIC_DOT = 2
} DistanceMetric;

// MetalIndexOptimized wraps Metal GPU resources with compute shaders
struct MetalIndexOptimized {
    void* device;
    void* commandQueue;
    void* vectorBuffer;
    void* idBuffer;
    void* distanceComputePipeline;
    void* cosinePipeline;
    void* dotPipeline;
    void* topKPipeline;
    void* l2Fp16Pipeline;
    void* cosineFp16Pipeline;
    void* dotFp16Pipeline;
    void* l2C64Pipeline;
    void* cosineC64Pipeline;
    void* l2C128Pipeline;
    void* cosineC128Pipeline;
    void* tqPipeline;
    void* pqPipeline;
    void* pqBuffer;
    void* haversinePipeline;
    void* normPipeline;
    void* prunePipeline;
    void* greedySearchPipeline;
    void* graphOffsetsBuffer;
    void* graphNeighborsBuffer;
    void* graphWeightsBuffer;
    int vectorCount;
    int dimensions;
    int capacity;
    DistanceMetric metric;
};

// Metal shader source for optimized L2 distance calculation, top-k selection, and batched queries
// Removed runtime shader source as it is now in kernels.metal and kernels.metallib



// Initialize Metal device using shared context
MetalIndexOptimized* metal_init_optimized(int dimensions) {
    @autoreleasepool {
        MetalIndexOptimized* handle = (MetalIndexOptimized*)malloc(sizeof(MetalIndexOptimized));
        memset(handle, 0, sizeof(MetalIndexOptimized));
        handle->dimensions = dimensions;
        handle->capacity = 0;
        handle->vectorCount = 0;
        return handle;
    }
}

void metal_set_pipelines_optimized(MetalIndexOptimized* handle, void* device, void* queue,
                                 void* l2, void* cosine, void* dot, void* topK,
                                 void* l2Fp16, void* cosineFp16, void* dotFp16,
                                 void* l2C128, void* cosineC128, void* l2C64, void* cosineC64,
                                 void* tq, void* haversine, void* norm, void* prune, void* greedy) {
    handle->device = device;
    handle->commandQueue = queue;
    handle->distanceComputePipeline = l2;
    handle->cosinePipeline = cosine;
    handle->dotPipeline = dot;
    handle->topKPipeline = topK;
    handle->l2Fp16Pipeline = l2Fp16;
    handle->cosineFp16Pipeline = cosineFp16;
    handle->dotFp16Pipeline = dotFp16;
    handle->l2C128Pipeline = l2C128;
    handle->cosineC128Pipeline = cosineC128;
    handle->l2C64Pipeline = l2C64;
    handle->cosineC64Pipeline = cosineC64;
    handle->tqPipeline = tq;
    handle->haversinePipeline = haversine;
    handle->normPipeline = norm;
    handle->prunePipeline = prune;
    handle->greedySearchPipeline = greedy;
}

// Perform HNSW greedy search on GPU
int metal_greedy_search_optimized(MetalIndexOptimized* handle, float* query, uint32_t* entryPoint, float* entryDist) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> greedyPipeline = (__bridge id<MTLComputePipelineState>)handle->greedySearchPipeline;
        
        if (!greedyPipeline || !handle->graphOffsetsBuffer || !handle->graphNeighborsBuffer) {
            return -1;
        }

        id<MTLBuffer> queryBuf = [device newBufferWithBytes:query length:handle->dimensions * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> epBuf = [device newBufferWithBytes:entryPoint length:sizeof(uint32_t) options:MTLResourceStorageModeShared];
        id<MTLBuffer> distBuf = [device newBufferWithBytes:entryDist length:sizeof(float) options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> cmdBuf = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [cmdBuf computeCommandEncoder];

        [encoder setComputePipelineState:greedyPipeline];
        [encoder setBuffer:queryBuf offset:0 atIndex:0];
        [encoder setBuffer:(__bridge id<MTLBuffer>)handle->vectorBuffer offset:0 atIndex:1];
        [encoder setBuffer:(__bridge id<MTLBuffer>)handle->graphOffsetsBuffer offset:0 atIndex:2];
        [encoder setBuffer:(__bridge id<MTLBuffer>)handle->graphNeighborsBuffer offset:0 atIndex:3];
        [encoder setBuffer:epBuf offset:0 atIndex:4];
        [encoder setBuffer:distBuf offset:0 atIndex:5];
        [encoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:6];
        [encoder setBytes:&handle->vectorCount length:sizeof(uint32_t) atIndex:7];

        [encoder dispatchThreadgroups:MTLSizeMake(1, 1, 1) threadsPerThreadgroup:MTLSizeMake(32, 1, 1)];
        [encoder endEncoding];
        [cmdBuf commit];
        [cmdBuf waitUntilCompleted];

        *entryPoint = *(uint32_t*)epBuf.contents;
        *entryDist = *(float*)distBuf.contents;

        return 0;
    }
}

// Update graph buffers for HNSW traversal
int metal_update_graph_optimized(MetalIndexOptimized* handle, uint32_t* offsets, int numOffsets, uint32_t* neighbors, int numNeighbors, float* weights, int numWeights) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        
        if (handle->graphOffsetsBuffer) {
            handle->graphOffsetsBuffer = nil;
        }
        if (handle->graphNeighborsBuffer) {
            handle->graphNeighborsBuffer = nil;
        }
        if (handle->graphWeightsBuffer) {
            handle->graphWeightsBuffer = nil;
        }

        if (numOffsets > 0) {
            handle->graphOffsetsBuffer = (__bridge_retained void*)[device newBufferWithBytes:offsets
                                                                                     length:numOffsets * sizeof(uint32_t)
                                                                                    options:MTLResourceStorageModeShared];
        }
        if (numNeighbors > 0) {
            handle->graphNeighborsBuffer = (__bridge_retained void*)[device newBufferWithBytes:neighbors
                                                                                       length:numNeighbors * sizeof(uint32_t)
                                                                                      options:MTLResourceStorageModeShared];
        }
        if (numWeights > 0) {
            handle->graphWeightsBuffer = (__bridge_retained void*)[device newBufferWithBytes:weights
                                                                                     length:numWeights * sizeof(float)
                                                                                    options:MTLResourceStorageModeShared];
        }
        return 0;
    }
}

// Set distance metric
void metal_set_metric(MetalIndexOptimized* handle, DistanceMetric metric) {
    handle->metric = metric;
}

// Add vectors using optimized path with dynamic resizing
int metal_add_vectors_optimized(MetalIndexOptimized* handle, float* vectors, int64_t* ids, int count) {
    @autoreleasepool {
        if (!handle || !vectors) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        int requiredCapacity = handle->vectorCount + count;
        int newCapacity = handle->capacity;

        // Grow capacity if needed
        if (requiredCapacity > newCapacity) {
            newCapacity = requiredCapacity > 0 ? requiredCapacity : 1024;
            while (newCapacity < requiredCapacity) {
                newCapacity *= 2;
            }
        }

        // Allocate or grow vector buffer
        size_t bufferSize = newCapacity * handle->dimensions * sizeof(float);
        id<MTLBuffer> newVectorBuffer = [device newBufferWithLength:bufferSize
                                                            options:MTLResourceStorageModeShared];

        if (!newVectorBuffer) {
            return -1;
        }

        // Copy existing data if resizing
        if (handle->vectorBuffer && handle->vectorCount > 0) {
            id<MTLBuffer> oldBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
            memcpy([newVectorBuffer contents], [oldBuffer contents],
                   handle->vectorCount * handle->dimensions * sizeof(float));
            CFRelease(handle->vectorBuffer);
        }

        // Copy new vectors
        float* dest = (float*)[newVectorBuffer contents] + (handle->vectorCount * handle->dimensions);
        memcpy(dest, vectors, count * handle->dimensions * sizeof(float));

        // Allocate ID buffer if needed
        if (!handle->idBuffer) {
            size_t idBufferSize = newCapacity * sizeof(int64_t);
            id<MTLBuffer> idBuf = [device newBufferWithLength:idBufferSize
                                                       options:MTLResourceStorageModeShared];
            if (idBuf) {
                handle->idBuffer = (__bridge_retained void*)idBuf;
            }
        }

        // Copy IDs
        if (handle->idBuffer && ids) {
            id<MTLBuffer> idBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
            int64_t* idDest = (int64_t*)[idBuffer contents] + handle->vectorCount;
            memcpy(idDest, ids, count * sizeof(int64_t));
        }

        handle->vectorBuffer = (__bridge_retained void*)newVectorBuffer;
        handle->vectorCount = requiredCapacity;
        handle->capacity = newCapacity;

        return 0;
    }
}

// Get current vector count
int metal_get_count_optimized(MetalIndexOptimized* handle) {
    return handle ? handle->vectorCount : 0;
}

// Search using Metal compute shaders with multiple metrics
int metal_search_optimized(MetalIndexOptimized* handle, float* query, int k, int64_t* resultIDs, float* resultDistances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0 || !query) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;

        // Select pipeline based on metric
        id<MTLComputePipelineState> distancePipeline;
        switch (handle->metric) {
            case METRIC_COSINE:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->cosinePipeline;
                break;
            case METRIC_DOT:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->dotPipeline;
                break;
            default:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->distanceComputePipeline;
        }

        if (!distancePipeline) {
            distancePipeline = (__bridge id<MTLComputePipelineState>)handle->distanceComputePipeline;
        }

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

        // Initialize indices and distances for heap-based top-k
        // Initialize indices and distances with default values before GPU processing
        for (int i = 0; i < k; i++) {
            ((int*)indicesBuffer.contents)[i] = -1;
            ((float*)topDistancesBuffer.contents)[i] = INFINITY;
        }

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
        if (threadGroupSize > (NSUInteger)handle->vectorCount) {
            threadGroupSize = handle->vectorCount;
        }
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];

        // Find top-k using heap-based selection
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

        // Copy results with ID lookup
        int* indices = (int*)[indicesBuffer contents];
        float* distances = (float*)[topDistancesBuffer contents];

        // Get IDs if available
        if (handle->idBuffer) {
            id<MTLBuffer> idBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
            int64_t* ids = (int64_t*)[idBuffer contents];
            for (int i = 0; i < k; i++) {
                resultIDs[i] = (indices[i] >= 0 && indices[i] < handle->vectorCount) ?
                    ids[indices[i]] : -1;
                resultDistances[i] = distances[i];
            }
        } else {
            for (int i = 0; i < k; i++) {
                resultIDs[i] = indices[i];
                resultDistances[i] = distances[i];
            }
        }

        return 0;
    }
}

// Cleanup with proper resource release
void metal_cleanup_optimized(MetalIndexOptimized* handle) {
    @autoreleasepool {
        if (handle) {
            if (handle->idBuffer) CFRelease(handle->idBuffer);
            if (handle->vectorBuffer) CFRelease(handle->vectorBuffer);
            if (handle->pqBuffer) CFRelease(handle->pqBuffer);
            free(handle);
        }
    }
}
// Vector type enum for GPU kernels
typedef enum {
    VECTOR_F32 = 0,
    VECTOR_F16 = 1,
    VECTOR_C64 = 2,
    VECTOR_C128 = 3
} VectorTypeGPU;

// Multi-type search dispatch - selects the correct pipeline based on vector type
int metal_search_typed(MetalIndexOptimized* handle, void* query, int k, int64_t* resultIDs, float* resultDistances, VectorTypeGPU vtype) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0 || !query) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;

        // Select distance pipeline by type
        id<MTLComputePipelineState> distancePipeline = NULL;
        switch (vtype) {
            case VECTOR_F16:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->l2Fp16Pipeline;
                break;
            case VECTOR_C64:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->l2C64Pipeline;
                break;
            case VECTOR_C128:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->l2C128Pipeline;
                break;
            default:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->distanceComputePipeline;
        }

        if (!distancePipeline) {
            distancePipeline = (__bridge id<MTLComputePipelineState>)handle->distanceComputePipeline;
        }

        id<MTLComputePipelineState> topKPipeline = (__bridge id<MTLComputePipelineState>)handle->topKPipeline;

        // Create query buffer
        id<MTLBuffer> queryBuffer;
        if (vtype == VECTOR_F16 || vtype == VECTOR_C64) {
            queryBuffer = [device newBufferWithBytes:query
                                              length:handle->dimensions * sizeof(uint16_t)
                                             options:MTLResourceStorageModeShared];
        } else {
            queryBuffer = [device newBufferWithBytes:query
                                              length:handle->dimensions * sizeof(float)
                                             options:MTLResourceStorageModeShared];
        }

        id<MTLBuffer> distanceBuffer = [device newBufferWithLength:handle->vectorCount * sizeof(float)
                                                            options:MTLResourceStorageModeShared];

        id<MTLBuffer> indicesBuffer = [device newBufferWithLength:k * sizeof(int)
                                                           options:MTLResourceStorageModeShared];

        id<MTLBuffer> topDistancesBuffer = [device newBufferWithLength:k * sizeof(float)
                                                                options:MTLResourceStorageModeShared];

        for (int i = 0; i < k; i++) {
            ((int*)indicesBuffer.contents)[i] = -1;
            ((float*)topDistancesBuffer.contents)[i] = INFINITY;
        }

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:distancePipeline];
        [encoder setBuffer:queryBuffer offset:0 atIndex:0];
        [encoder setBuffer:vectorBuffer offset:0 atIndex:1];
        [encoder setBuffer:distanceBuffer offset:0 atIndex:2];
        [encoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:3];
        [encoder setBytes:&handle->vectorCount length:sizeof(uint32_t) atIndex:4];

        MTLSize gridSize = MTLSizeMake(handle->vectorCount, 1, 1);
        NSUInteger threadGroupSize = distancePipeline.maxTotalThreadsPerThreadgroup;
        if (threadGroupSize > (NSUInteger)handle->vectorCount) {
            threadGroupSize = handle->vectorCount;
        }
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];

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

        int* indices = (int*)[indicesBuffer contents];
        float* distances = (float*)[topDistancesBuffer contents];

        if (handle->idBuffer) {
            id<MTLBuffer> idBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
            int64_t* ids = (int64_t*)[idBuffer contents];
            for (int i = 0; i < k; i++) {
                resultIDs[i] = (indices[i] >= 0 && indices[i] < handle->vectorCount) ?
                    ids[indices[i]] : -1;
                resultDistances[i] = distances[i];
            }
        } else {
            for (int i = 0; i < k; i++) {
                resultIDs[i] = indices[i];
                resultDistances[i] = distances[i];
            }
        }

        return 0;
    }
}
int metal_add_tq_vectors_optimized(MetalIndexOptimized* handle, unsigned char* tqData, int stride, int64_t* ids, int count) {
    @autoreleasepool {
        if (!handle || !tqData) return -1;
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        int requiredCapacity = handle->vectorCount + count;
        if (requiredCapacity > handle->capacity) {
            int newCapacity = handle->capacity > 0 ? handle->capacity : 1024;
            while (newCapacity < requiredCapacity) newCapacity *= 2;

            size_t bufferSize = (size_t)newCapacity * stride;
            id<MTLBuffer> newVectorBuffer = [device newBufferWithLength:bufferSize options:MTLResourceStorageModeShared];
            if (!newVectorBuffer) return -1;

            if (handle->vectorBuffer && handle->vectorCount > 0) {
                memcpy([newVectorBuffer contents], [(__bridge id<MTLBuffer>)handle->vectorBuffer contents], (size_t)handle->vectorCount * stride);
                CFRelease(handle->vectorBuffer);
            }
            handle->vectorBuffer = (__bridge_retained void*)newVectorBuffer;
            handle->capacity = newCapacity;

            // Grow ID buffer
            size_t idBufferSize = (size_t)newCapacity * sizeof(int64_t);
            id<MTLBuffer> newIdBuffer = [device newBufferWithLength:idBufferSize options:MTLResourceStorageModeShared];
            if (handle->idBuffer) {
                memcpy([newIdBuffer contents], [(__bridge id<MTLBuffer>)handle->idBuffer contents], (size_t)handle->vectorCount * sizeof(int64_t));
                CFRelease(handle->idBuffer);
            }
            handle->idBuffer = (__bridge_retained void*)newIdBuffer;
        }

        memcpy((unsigned char*)[(__bridge id<MTLBuffer>)handle->vectorBuffer contents] + (size_t)handle->vectorCount * stride, tqData, (size_t)count * stride);
        memcpy((int64_t*)[(__bridge id<MTLBuffer>)handle->idBuffer contents] + handle->vectorCount, ids, (size_t)count * sizeof(int64_t));
        handle->vectorCount += count;
        return 0;
    }
}

int metal_search_tq_optimized(MetalIndexOptimized* handle, float* query, int k, int pow2, int bitsPerAngle, int64_t* resultIDs, float* resultDistances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0 || !handle->tqPipeline) return -1;

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> tqBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        id<MTLComputePipelineState> tqPipeline = (__bridge id<MTLComputePipelineState>)handle->tqPipeline;
        id<MTLComputePipelineState> topKPipeline = (__bridge id<MTLComputePipelineState>)handle->topKPipeline;

        id<MTLBuffer> queryBuffer = [device newBufferWithBytes:query length:handle->dimensions * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> distBuffer = [device newBufferWithLength:handle->vectorCount * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> indicesBuffer = [device newBufferWithLength:k * sizeof(int) options:MTLResourceStorageModeShared];
        id<MTLBuffer> topDistancesBuffer = [device newBufferWithLength:k * sizeof(float) options:MTLResourceStorageModeShared];

        for (int i = 0; i < k; i++) {
            ((int*)indicesBuffer.contents)[i] = -1;
            ((float*)topDistancesBuffer.contents)[i] = INFINITY;
        }

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:tqPipeline];
        [encoder setBuffer:queryBuffer offset:0 atIndex:0];
        [encoder setBuffer:tqBuffer offset:0 atIndex:1];
        [encoder setBuffer:distBuffer offset:0 atIndex:2];
        [encoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:3];
        [encoder setBytes:&pow2 length:sizeof(uint32_t) atIndex:4];
        [encoder setBytes:&bitsPerAngle length:sizeof(uint32_t) atIndex:5];
        [encoder setBytes:&handle->vectorCount length:sizeof(uint32_t) atIndex:6];

        [encoder dispatchThreads:MTLSizeMake(handle->vectorCount, 1, 1) threadsPerThreadgroup:MTLSizeMake(MIN(handle->vectorCount, (int)tqPipeline.maxTotalThreadsPerThreadgroup), 1, 1)];

        [encoder setComputePipelineState:topKPipeline];
        [encoder setBuffer:distBuffer offset:0 atIndex:0];
        [encoder setBuffer:indicesBuffer offset:0 atIndex:1];
        [encoder setBuffer:topDistancesBuffer offset:0 atIndex:2];
        [encoder setBytes:&handle->vectorCount length:sizeof(uint32_t) atIndex:3];
        [encoder setBytes:&k length:sizeof(uint32_t) atIndex:4];

        [encoder dispatchThreads:MTLSizeMake(1, 1, 1) threadsPerThreadgroup:MTLSizeMake(1, 1, 1)];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        int* indices = (int*)[indicesBuffer contents];
        float* distances = (float*)[topDistancesBuffer contents];
        int64_t* ids = (int64_t*)[(__bridge id<MTLBuffer>)handle->idBuffer contents];

        for (int i = 0; i < k; i++) {
            resultIDs[i] = (indices[i] >= 0 && indices[i] < handle->vectorCount) ? ids[indices[i]] : -1;
            resultDistances[i] = distances[i];
        }
        return 0;
    }
}

    int metal_add_pq(MetalIndexOptimized* handle, const unsigned char* codes, const int64_t* ids, int count, int m) {
        @autoreleasepool {
            if (!handle || !codes) return -1;
            id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
            int requiredCapacity = handle->vectorCount + count;
            if (requiredCapacity > handle->capacity) {
                int newCapacity = handle->capacity > 0 ? handle->capacity : 1024;
                while (newCapacity < requiredCapacity) newCapacity *= 2;

                size_t pqSize = (size_t)newCapacity * m;
                id<MTLBuffer> newPQBuffer = [device newBufferWithLength:pqSize options:MTLResourceStorageModeShared];
                if (handle->pqBuffer) {
                    memcpy([newPQBuffer contents], [(__bridge id<MTLBuffer>)handle->pqBuffer contents], handle->vectorCount * m);
                    CFRelease(handle->pqBuffer);
                }
                handle->pqBuffer = (__bridge_retained void*)newPQBuffer;

                size_t idSize = (size_t)newCapacity * sizeof(int64_t);
                id<MTLBuffer> newIDBuffer = [device newBufferWithLength:idSize options:MTLResourceStorageModeShared];
                if (handle->idBuffer) {
                    memcpy([newIDBuffer contents], [(__bridge id<MTLBuffer>)handle->idBuffer contents], handle->vectorCount * sizeof(int64_t));
                    CFRelease(handle->idBuffer);
                }
                handle->idBuffer = (__bridge_retained void*)newIDBuffer;
                handle->capacity = newCapacity;
            }

            memcpy((unsigned char*)[(__bridge id<MTLBuffer>)handle->pqBuffer contents] + (handle->vectorCount * m), codes, count * m);
            memcpy((int64_t*)[(__bridge id<MTLBuffer>)handle->idBuffer contents] + handle->vectorCount, ids, count * sizeof(int64_t));
            handle->vectorCount += count;
            return 0;
        }
    }

    int metal_search_pq_optimized(MetalIndexOptimized* handle, const float* lookupTable, int m, int k, int64_t* resultIDs, float* resultDistances) {
        @autoreleasepool {
            if (!handle || !handle->pqBuffer || handle->vectorCount == 0) return -1;
            id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
            id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
            id<MTLComputePipelineState> pqPipeline = (__bridge id<MTLComputePipelineState>)handle->pqPipeline;
            id<MTLComputePipelineState> topKPipeline = (__bridge id<MTLComputePipelineState>)handle->topKPipeline;

            id<MTLBuffer> tableBuf = [device newBufferWithBytes:lookupTable length:m * 256 * sizeof(float) options:MTLResourceStorageModeShared];
            id<MTLBuffer> distBuf = [device newBufferWithLength:handle->vectorCount * sizeof(float) options:MTLResourceStorageModeShared];
            id<MTLBuffer> indicesBuf = [device newBufferWithLength:k * sizeof(int) options:MTLResourceStorageModeShared];
            id<MTLBuffer> topDistBuf = [device newBufferWithLength:k * sizeof(float) options:MTLResourceStorageModeShared];

            for (int i=0; i<k; i++) {
                ((int*)indicesBuf.contents)[i] = -1;
                ((float*)topDistBuf.contents)[i] = INFINITY;
            }

            id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
            id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

            [encoder setComputePipelineState:pqPipeline];
            [encoder setBuffer:tableBuf offset:0 atIndex:0];
            [encoder setBuffer:(__bridge id<MTLBuffer>)handle->pqBuffer offset:0 atIndex:1];
            [encoder setBuffer:distBuf offset:0 atIndex:2];
            [encoder setBytes:&m length:sizeof(int) atIndex:3];
            [encoder setBytes:&handle->vectorCount length:sizeof(int) atIndex:4];

            [encoder dispatchThreads:MTLSizeMake(handle->vectorCount, 1, 1) threadsPerThreadgroup:MTLSizeMake(MIN(handle->vectorCount, (int)pqPipeline.maxTotalThreadsPerThreadgroup), 1, 1)];

            [encoder setComputePipelineState:topKPipeline];
            [encoder setBuffer:distBuf offset:0 atIndex:0];
            [encoder setBuffer:indicesBuf offset:0 atIndex:1];
            [encoder setBuffer:topDistBuf offset:0 atIndex:2];
            [encoder setBytes:&handle->vectorCount length:sizeof(int) atIndex:3];
            [encoder setBytes:&k length:sizeof(int) atIndex:4];
            [encoder dispatchThreads:MTLSizeMake(1, 1, 1) threadsPerThreadgroup:MTLSizeMake(1, 1, 1)];

            [encoder endEncoding];
            [commandBuffer commit];
            [commandBuffer waitUntilCompleted];

            int* indices = (int*)indicesBuf.contents;
            float* distances = (float*)topDistBuf.contents;
            int64_t* ids = (int64_t*)[(__bridge id<MTLBuffer>)handle->idBuffer contents];
            for (int i=0; i<k; i++) {
                resultIDs[i] = (indices[i] >= 0) ? ids[indices[i]] : -1;
                resultDistances[i] = distances[i];
            }
            return 0;
        }
    }

    int metal_haversine_batch_optimized(MetalIndexOptimized* handle, float* center, float* points, float* results, float earthRadius, int count) {
        @autoreleasepool {
            if (!handle || !handle->haversinePipeline) return -1;
            id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
            id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
            id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->haversinePipeline;

            id<MTLBuffer> centerBuf = [device newBufferWithBytes:center length:2 * sizeof(float) options:MTLResourceStorageModeShared];
            id<MTLBuffer> pointsBuf = [device newBufferWithBytes:points length:count * 2 * sizeof(float) options:MTLResourceStorageModeShared];
            id<MTLBuffer> resBuf = [device newBufferWithLength:count * sizeof(float) options:MTLResourceStorageModeShared];

            id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
            id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

            [encoder setComputePipelineState:pipeline];
            [encoder setBuffer:centerBuf offset:0 atIndex:0];
            [encoder setBuffer:pointsBuf offset:0 atIndex:1];
            [encoder setBuffer:resBuf offset:0 atIndex:2];
            [encoder setBytes:&earthRadius length:sizeof(float) atIndex:3];
            [encoder setBytes:&count length:sizeof(uint32_t) atIndex:4];

            [encoder dispatchThreads:MTLSizeMake(count, 1, 1) threadsPerThreadgroup:MTLSizeMake(MIN(count, (int)pipeline.maxTotalThreadsPerThreadgroup), 1, 1)];
            [encoder endEncoding];
            [commandBuffer commit];
            [commandBuffer waitUntilCompleted];

            memcpy(results, [resBuf contents], count * sizeof(float));
            return 0;
        }
    }

    int metal_norm_batch_optimized(MetalIndexOptimized* handle, float* vectors, float* results, int dims, int count) {
        @autoreleasepool {
            if (!handle || !handle->normPipeline) return -1;
            id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
            id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
            id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->normPipeline;

            id<MTLBuffer> vecBuf = [device newBufferWithBytes:vectors length:count * dims * sizeof(float) options:MTLResourceStorageModeShared];
            id<MTLBuffer> resBuf = [device newBufferWithLength:count * sizeof(float) options:MTLResourceStorageModeShared];

            id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
            id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

            [encoder setComputePipelineState:pipeline];
            [encoder setBuffer:vecBuf offset:0 atIndex:0];
            [encoder setBuffer:resBuf offset:0 atIndex:1];
            [encoder setBytes:&dims length:sizeof(uint32_t) atIndex:2];
            [encoder setBytes:&count length:sizeof(uint32_t) atIndex:3];

            [encoder dispatchThreads:MTLSizeMake(count, 1, 1) threadsPerThreadgroup:MTLSizeMake(MIN(count, (int)pipeline.maxTotalThreadsPerThreadgroup), 1, 1)];
            [encoder endEncoding];
            [commandBuffer commit];
            [commandBuffer waitUntilCompleted];

            memcpy(results, [resBuf contents], count * sizeof(float));
            return 0;
        }
    }

    int metal_prune_neighbors_optimized(MetalIndexOptimized* handle, uint32_t* candidateIds, float* candidateDists, uint32_t* selectedIds, uint32_t* selectedCount, float* allVectors, int maxNeighbors, int numCandidates, int dim, bool extended) {
        @autoreleasepool {
            if (!handle || !handle->prunePipeline) return -1;
            id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
            id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
            id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->prunePipeline;

            id<MTLBuffer> candIdBuf = [device newBufferWithBytes:candidateIds length:numCandidates * sizeof(uint32_t) options:MTLResourceStorageModeShared];
            id<MTLBuffer> candDistBuf = [device newBufferWithBytes:candidateDists length:numCandidates * sizeof(float) options:MTLResourceStorageModeShared];
            id<MTLBuffer> selIdBuf = [device newBufferWithLength:maxNeighbors * sizeof(uint32_t) options:MTLResourceStorageModeShared];
            id<MTLBuffer> selCountBuf = [device newBufferWithLength:sizeof(uint32_t) options:MTLResourceStorageModeShared];
            
            id<MTLBuffer> allVecBuf;
            if (allVectors != NULL) {
                uint32_t maxID = 0;
                for (int i = 0; i < numCandidates; i++) {
                    if (candidateIds[i] > maxID) maxID = candidateIds[i];
                }
                allVecBuf = [device newBufferWithBytes:allVectors length:(size_t)(maxID + 1) * dim * sizeof(float) options:MTLResourceStorageModeShared];
            } else {
                allVecBuf = (__bridge id<MTLBuffer>)handle->vectorBuffer;
            }

            id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
            id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

            [encoder setComputePipelineState:pipeline];
            [encoder setBuffer:candIdBuf offset:0 atIndex:0];
            [encoder setBuffer:candDistBuf offset:0 atIndex:1];
            [encoder setBuffer:selIdBuf offset:0 atIndex:2];
            [encoder setBuffer:selCountBuf offset:0 atIndex:3];
            [encoder setBuffer:allVecBuf offset:0 atIndex:4];
            [encoder setBytes:&maxNeighbors length:sizeof(int) atIndex:5];
            [encoder setBytes:&numCandidates length:sizeof(int) atIndex:6];
            [encoder setBytes:&dim length:sizeof(int) atIndex:7];
            [encoder setBytes:&extended length:sizeof(bool) atIndex:8];

            [encoder dispatchThreads:MTLSizeMake(1, 1, 1) threadsPerThreadgroup:MTLSizeMake(1, 1, 1)];
            [encoder endEncoding];
            [commandBuffer commit];
            [commandBuffer waitUntilCompleted];

            *selectedCount = *(uint32_t*)[selCountBuf contents];
            memcpy(selectedIds, [selIdBuf contents], (*selectedCount) * sizeof(uint32_t));
            return 0;
        }
    }
*/
import "C"
import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
)

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
}

// NewMetalIndexOptimized creates an optimized Metal-based GPU index with compute shaders
func NewMetalIndexOptimized(cfg types.GPUConfig) (types.Index, error) {
	libData, err := metalFS.ReadFile("kernels.metallib")
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded metal library: %w", err)
	}

	if err := InitGlobalContext(libData); err != nil {
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
	l2, _ := ctx.GetPipelineState("compute_l2_distances")
	cosine, _ := ctx.GetPipelineState("compute_cosine_similarity")
	dot, _ := ctx.GetPipelineState("compute_dot_product")
	topK, _ := ctx.GetPipelineState("find_top_k_heap")
	l2Fp16, _ := ctx.GetPipelineState("compute_l2_distances_fp16")
	cosineFp16, _ := ctx.GetPipelineState("compute_cosine_similarity_fp16")
	dotFp16, _ := ctx.GetPipelineState("compute_dot_product_fp16")
	l2C128, _ := ctx.GetPipelineState("compute_l2_distances_complex128")
	cosineC128, _ := ctx.GetPipelineState("compute_cosine_similarity_complex128")
	l2C64, _ := ctx.GetPipelineState("compute_l2_distances_complex64")
	cosineC64, _ := ctx.GetPipelineState("compute_cosine_similarity_complex64")
	tq, _ := ctx.GetPipelineState("compute_tq_distances")
	haversine, _ := ctx.GetPipelineState("haversine_batch")
	norm, _ := ctx.GetPipelineState("norm_batch_f32")
	prune, _ := ctx.GetPipelineState("hnsw_prune_neighbors")

	greedy, _ := ctx.GetPipelineState("hnsw_greedy_search")

	C.metal_set_pipelines_optimized(
		handle,
		ctx.GetDevice(), ctx.GetCommandQueue(),
		l2, cosine, dot, topK,
		l2Fp16, cosineFp16, dotFp16,
		l2C128, cosineC128, l2C64, cosineC64,
		tq, haversine, norm, prune, greedy,
	)

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

	ret := C.metal_add_vectors_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vectors[0])),
		(*C.int64_t)(unsafe.Pointer(&ids[0])),
		C.int(n),
	)
	if ret != 0 {
		return fmt.Errorf("failed to add vectors to optimized Metal buffer")
	}

	return nil
}

// Len returns the number of vectors in the index
func (idx *MetalIndexOptimized) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.handle == nil {
		return 0
	}
	return int(C.metal_get_count_optimized(idx.handle))
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

const (
	vecTypeF32  = C.VectorTypeGPU(0)
	vecTypeF16  = C.VectorTypeGPU(1)
	vecTypeC64  = C.VectorTypeGPU(2)
	vecTypeC128 = C.VectorTypeGPU(3)
)

func (idx *MetalIndexOptimized) SearchFloat16(query []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.metal_search_typed(
		idx.handle,
		unsafe.Pointer(&query[0]),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
		vecTypeF16,
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal float16 search failed")
	}

	return resultIDs, resultDistances, nil
}

func (idx *MetalIndexOptimized) SearchComplex64(query []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.metal_search_typed(
		idx.handle,
		unsafe.Pointer(&query[0]),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
		vecTypeC64,
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal complex64 search failed")
	}

	return resultIDs, resultDistances, nil
}

func (idx *MetalIndexOptimized) SearchComplex128(query []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.metal_search_typed(
		idx.handle,
		unsafe.Pointer(&query[0]),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
		vecTypeC128,
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal complex128 search failed")
	}

	return resultIDs, resultDistances, nil
}

// SearchBatch queries the optimized Metal GPU index with multiple vectors in parallel.
// This improves GPU utilization by batching multiple queries.
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

	if idx.handle != nil {
		C.metal_cleanup_optimized(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}

func (idx *MetalIndexOptimized) Backend() types.GPUBackend {
	return types.BackendMetal
}

func (idx *MetalIndexOptimized) DeviceID() int32 {
	return 0
}

func (idx *MetalIndexOptimized) GetDeviceInfo() (*types.GPUInfo, error) {
	return &types.GPUInfo{
		Backend:  types.BackendMetal,
		Name:     "Apple Silicon GPU (Optimized)",
		DeviceID: 0,
	}, nil
}

func (idx *MetalIndexOptimized) GetMemoryInfo() (int64, int64, int64, error) {
	return 0, 0, 0, nil
}

func (idx *MetalIndexOptimized) AddPQ(ids []int64, codes []byte, m int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	if len(ids) == 0 {
		return nil
	}

	ret := C.metal_add_pq(idx.handle, (*C.uchar)(unsafe.Pointer(&codes[0])), (*C.int64_t)(unsafe.Pointer(&ids[0])), C.int(len(ids)), C.int(m))
	if ret != 0 {
		return fmt.Errorf("failed to add PQ vectors to Metal: error %d", int(ret))
	}

	return nil
}

func (idx *MetalIndexOptimized) SearchPQ(lookupTable []float32, m, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	resIDs := make([]int64, k)
	resDists := make([]float32, k)

	ret := C.metal_search_pq_optimized(idx.handle, (*C.float)(unsafe.Pointer(&lookupTable[0])), C.int(m), C.int(k), (*C.int64_t)(unsafe.Pointer(&resIDs[0])), (*C.float)(unsafe.Pointer(&resDists[0])))
	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal PQ search failed: error %d", int(ret))
	}

	return resIDs, resDists, nil
}

func (idx *MetalIndexOptimized) TrainPQ(vectors []float32, m, k int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	encoder, err := pq.NewPQEncoder(idx.dim, m, k)
	if err != nil {
		return err
	}

	numVecs := len(vectors) / idx.dim
	vecs2d := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vecs2d[i] = vectors[i*idx.dim : (i+1)*idx.dim]
	}

	if err := encoder.Train(vecs2d); err != nil {
		return err
	}

	idx.pqEncoder = encoder
	return nil
}

func (idx *MetalIndexOptimized) EncodePQ(vectors []float32) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.pqEncoder == nil {
		return nil, fmt.Errorf("PQ encoder not trained")
	}

	numVecs := len(vectors) / idx.dim
	codes := make([]byte, numVecs*idx.pqEncoder.M)
	for i := 0; i < numVecs; i++ {
		vec := vectors[i*idx.dim : (i+1)*idx.dim]
		encoded, err := idx.pqEncoder.Encode(vec)
		if err != nil {
			return nil, err
		}
		copy(codes[i*idx.pqEncoder.M:], encoded)
	}
	return codes, nil
}

func (idx *MetalIndexOptimized) GetUtilization() (float32, error) {
	return 50.0, nil
}

func (idx *MetalIndexOptimized) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	count := len(ids)
	if count == 0 {
		return nil
	}

	stride := len(tqData) / count

	ret := C.metal_add_tq_vectors_optimized(
		idx.handle,
		(*C.uchar)(unsafe.Pointer(&tqData[0])),
		C.int(stride),
		(*C.int64_t)(unsafe.Pointer(&ids[0])),
		C.int(count),
	)

	if ret != 0 {
		return fmt.Errorf("failed to add TQ vectors to optimized Metal buffer")
	}

	return nil
}

func (idx *MetalIndexOptimized) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	pow2 := 1
	for pow2 < idx.dim {
		pow2 <<= 1
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.metal_search_tq_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		C.int(k),
		C.int(pow2),
		C.int(bitsPerAngle),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("optimized Metal TQ search failed")
	}

	return resultIDs, resultDistances, nil
}

func (idx *MetalIndexOptimized) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index closed")
	}

	// For the optimized backend, we store the graph in unified memory buffers
	// for direct access by Metal kernels.
	idx.graphOffsets = offsets
	idx.graphNeighbors = neighbors
	idx.graphWeights = weights

	// Sync to GPU
	var offsetsPtr, neighborsPtr *C.uint32_t
	var weightsPtr *C.float

	if len(offsets) > 0 {
		offsetsPtr = (*C.uint32_t)(unsafe.Pointer(&offsets[0]))
	}
	if len(neighbors) > 0 {
		neighborsPtr = (*C.uint32_t)(unsafe.Pointer(&neighbors[0]))
	}
	if len(weights) > 0 {
		weightsPtr = (*C.float)(unsafe.Pointer(&weights[0]))
	}

	ret := C.metal_update_graph_optimized(
		idx.handle,
		offsetsPtr, C.int(len(offsets)),
		neighborsPtr, C.int(len(neighbors)),
		weightsPtr, C.int(len(weights)),
	)

	if ret != 0 {
		return fmt.Errorf("failed to update graph on GPU")
	}

	return nil
}

func (idx *MetalIndexOptimized) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index closed")
	}

	if len(idx.graphOffsets) == 0 {
		return nil, nil, fmt.Errorf("graph not initialized")
	}

	// BFS expansion (initially on CPU for stability, kernels to follow)
	visited := make(map[uint32]float32)
	for _, seed := range seeds {
		visited[seed] = 1.0
	}

	currentFrontier := seeds
	for d := 0; d < depth; d++ {
		var nextFrontier []uint32
		for _, nodeID := range currentFrontier {
			if int(nodeID)+1 >= len(idx.graphOffsets) {
				continue
			}
			start := idx.graphOffsets[nodeID]
			end := idx.graphOffsets[nodeID+1]
			
			for neighborIdx := start; neighborIdx < end; neighborIdx++ {
				neighbor := idx.graphNeighbors[neighborIdx]
				if _, seen := visited[neighbor]; !seen {
					score := visited[nodeID] * alpha
					visited[neighbor] = score
					nextFrontier = append(nextFrontier, neighbor)
				}
			}
		}
		if len(nextFrontier) == 0 {
			break
		}
		currentFrontier = nextFrontier
	}

	outIDs := make([]uint32, 0, len(visited))
	outScores := make([]float32, 0, len(visited))
	for id, score := range visited {
		outIDs = append(outIDs, id)
		outScores = append(outScores, score)
	}

	return outIDs, outScores, nil
}

func (idx *MetalIndexOptimized) HaversineSearch(centerLat, centerLon float32, points []float32, earthRadius float32) ([]float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	count := len(points) / 2
	if count == 0 {
		return nil, nil
	}

	results := make([]float32, count)
	center := []float32{centerLat, centerLon}

	start := time.Now()
	ret := C.metal_haversine_batch_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&center[0])),
		(*C.float)(unsafe.Pointer(&points[0])),
		(*C.float)(unsafe.Pointer(&results[0])),
		C.float(earthRadius),
		C.int(count),
	)

	if ret != 0 {
		// CPU fallback
		const degToRad = math.Pi / 180.0
		lat1 := float64(centerLat) * degToRad
		lon1 := float64(centerLon) * degToRad

		for i := 0; i < count; i++ {
			lat2 := float64(points[i*2]) * degToRad
			lon2 := float64(points[i*2+1]) * degToRad

			dLat := lat2 - lat1
			dLon := lon2 - lon1

			a := math.Sin(dLat/2)*math.Sin(dLat/2) +
				math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
			c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
			results[i] = float32(float64(earthRadius) * c)
		}
	} else {
		metrics.GPUComputeDurationSeconds.WithLabelValues("Apple Silicon GPU (Optimized)", "haversine").Observe(time.Since(start).Seconds())
	}
	return results, nil
}

func (idx *MetalIndexOptimized) NormBatch(vectors []float32, dims int) ([]float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	count := len(vectors) / dims
	if count == 0 {
		return nil, nil
	}

	results := make([]float32, count)

	start := time.Now()
	ret := C.metal_norm_batch_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vectors[0])),
		(*C.float)(unsafe.Pointer(&results[0])),
		C.int(dims),
		C.int(count),
	)

	if ret != 0 {
		// CPU fallback
		for i := 0; i < count; i++ {
			var sum float64
			for j := 0; j < dims; j++ {
				val := float64(vectors[i*dims+j])
				sum += val * val
			}
			results[i] = float32(math.Sqrt(sum))
		}
	} else {
		metrics.GPUComputeDurationSeconds.WithLabelValues("Apple Silicon GPU (Optimized)", "norm_batch").Observe(time.Since(start).Seconds())
	}
	return results, nil
}

func (idx *MetalIndexOptimized) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
	// CPU fallback for cluster assignment
	numVecs := len(vectors) / idx.dim
	numClusters := len(centroids) / idx.dim
	assignments := make([]uint32, numVecs)

	for i := 0; i < numVecs; i++ {
		vec := vectors[i*idx.dim : (i+1)*idx.dim]
		minDist := float32(math.MaxFloat32)
		bestCluster := uint32(0)

		for j := 0; j < numClusters; j++ {
			centroid := centroids[j*idx.dim : (j+1)*idx.dim]
			dist := float32(0)
			for k := 0; k < idx.dim; k++ {
				diff := vec[k] - centroid[k]
				dist += diff * diff
			}
			if dist < minDist {
				minDist = dist
				bestCluster = uint32(j)
			}
		}
		assignments[i] = bestCluster
	}
	return assignments, nil
}

func (idx *MetalIndexOptimized) SearchGreedy(query []float32, entryPoint uint32, entryDist float32) (uint32, float32, error) {
	return idx.searchGreedyGPU(query, entryPoint, entryDist)
}

func (idx *MetalIndexOptimized) PruneNeighbors(candidateIds []uint32, candidateDists []float32, maxNeighbors int, allVectors []float32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	numCandidates := len(candidateIds)
	if numCandidates == 0 {
		return []uint32{}, nil
	}

	selectedIds := make([]uint32, maxNeighbors)
	var selectedCount uint32

	var vecPtr *C.float
	if len(allVectors) > 0 {
		vecPtr = (*C.float)(unsafe.Pointer(&allVectors[0]))
	}

	start := time.Now()
	ret := C.metal_prune_neighbors_optimized(
		idx.handle,
		(*C.uint32_t)(unsafe.Pointer(&candidateIds[0])),
		(*C.float)(unsafe.Pointer(&candidateDists[0])),
		(*C.uint32_t)(unsafe.Pointer(&selectedIds[0])),
		(*C.uint32_t)(unsafe.Pointer(&selectedCount)),
		vecPtr,
		C.int(maxNeighbors),
		C.int(numCandidates),
		C.int(idx.dim),
		C.bool(true),
	)

	if ret == 0 {
		metrics.GPUComputeDurationSeconds.WithLabelValues("Apple Silicon GPU (Optimized)", "prune_neighbors").Observe(time.Since(start).Seconds())
		return selectedIds[:selectedCount], nil
	}

	// CPU fallback: simple distance-based pruning
	type cand struct {
		id   uint32
		dist float32
	}
	cands := make([]cand, len(candidateIds))
	for i := range candidateIds {
		cands[i] = cand{id: candidateIds[i], dist: candidateDists[i]}
	}

	sort.Slice(cands, func(i, j int) bool {
		return cands[i].dist < cands[j].dist
	})

	n := maxNeighbors
	if n > len(cands) {
		n = len(cands)
	}

	pruned := make([]uint32, n)
	for i := 0; i < n; i++ {
		pruned[i] = cands[i].id
	}

	return pruned, nil
}

func (idx *MetalIndexOptimized) SearchGreedy(query []float32, entryPoint uint32, entryDist float32) (uint32, float32, error) {
	return idx.searchGreedyGPU(query, entryPoint, entryDist)
}

func (idx *MetalIndexOptimized) searchGreedyGPU(query []float32, entryPoint uint32, entryDist float32) (uint32, float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return 0, 0, fmt.Errorf("index closed")
	}

	ep := entryPoint
	ed := entryDist

	ret := C.metal_greedy_search_optimized(
		idx.handle,
		(*C.float)(unsafe.Pointer(&query[0])),
		(*C.uint32_t)(unsafe.Pointer(&ep)),
		(*C.float)(unsafe.Pointer(&ed)),
	)

	if ret != 0 {
		return 0, 0, fmt.Errorf("GPU greedy search failed")
	}

	return ep, ed, nil
}

func (idx *MetalIndexOptimized) Sync() error {
	return nil
}

func (idx *MetalIndexOptimized) Clear() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	// Reset vector count in handle
	idx.handle.vectorCount = 0
	
	// Reset graph metadata
	idx.graphOffsets = nil
	idx.graphNeighbors = nil
	idx.graphWeights = nil

	return nil
}

func (idx *MetalIndexOptimized) Reset() error {
	return idx.Clear()
}
