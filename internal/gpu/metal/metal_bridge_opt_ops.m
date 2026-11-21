//go:build gpu && darwin && arm64 && cgo

#import <Accelerate/Accelerate.h>
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#include <math.h>
#include <string.h>

#include "metal_bridge.h"

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
                                 void* l2, void* l2Paged, void* cosine, void* dot, void* topK,
                                 void* l2Fp16, void* cosineFp16, void* dotFp16,
                                 void* l2C128, void* cosineC128, void* l2C64, void* cosineC64,
                                 void* tq, void* haversine, void* norm, void* prune, void* greedy, void* greedyTQ) {
    handle->device = device;
    handle->commandQueue = queue;
    handle->distanceComputePipeline = l2;
    handle->distancePagedPipeline = l2Paged;
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
    handle->greedyTQSearchPipeline = greedyTQ;

    id<MTLDevice> mtlDevice = (__bridge id<MTLDevice>)device;
    float* trigData = (float*)malloc(256 * 2 * sizeof(float));
    for (int i = 0; i < 256; i++) {
        double theta = ((double)i / 255.0) * 2.0 * M_PI - M_PI;
        trigData[2 * i] = (float)cos(theta);
        trigData[2 * i + 1] = (float)sin(theta);
    }
    id<MTLBuffer> trigBuffer = [mtlDevice newBufferWithBytes:trigData length:256 * 2 * sizeof(float) options:MTLResourceStorageModeShared];
    handle->trigBuffer = (__bridge_retained void*)trigBuffer;
    free(trigData);

    size_t queryBufSize = 1024 * sizeof(float);
    if (handle->dimensions > 1024) {
        queryBufSize = (size_t)handle->dimensions * sizeof(float);
    }
    id<MTLBuffer> qBuf0 = [mtlDevice newBufferWithLength:queryBufSize options:MTLResourceStorageModeShared];
    id<MTLBuffer> qBuf1 = [mtlDevice newBufferWithLength:queryBufSize options:MTLResourceStorageModeShared];
    handle->queryBuffers[0] = (__bridge_retained void*)qBuf0;
    handle->queryBuffers[1] = (__bridge_retained void*)qBuf1;
    handle->currentBufferIdx = 0;
}

int metal_greedy_search_optimized(MetalIndexOptimized* handle, float* query, uint32_t* entryPoint, float* entryDist) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> greedyPipeline = (__bridge id<MTLComputePipelineState>)handle->greedySearchPipeline;

        if (!greedyPipeline || !handle->graphOffsetsBuffer || !handle->graphNeighborsBuffer) {
            return -1;
        }

        id<MTLBuffer> queryBuf = (__bridge id<MTLBuffer>)handle->queryBuffers[handle->currentBufferIdx];
        if (queryBuf) {
            memcpy(queryBuf.contents, query, handle->dimensions * sizeof(float));
        } else {
            queryBuf = [device newBufferWithBytes:query length:handle->dimensions * sizeof(float) options:MTLResourceStorageModeShared];
        }
        handle->currentBufferIdx = (handle->currentBufferIdx + 1) % 2;

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

int metal_greedy_search_tq_optimized(MetalIndexOptimized* handle, float* query, int pow2, int bitsPerAngle, uint32_t* entryPoint, float* entryDist) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> greedyPipeline = (__bridge id<MTLComputePipelineState>)handle->greedyTQSearchPipeline;

        if (!greedyPipeline || !handle->graphOffsetsBuffer || !handle->graphNeighborsBuffer) {
            return -1;
        }

        id<MTLBuffer> queryBuf = (__bridge id<MTLBuffer>)handle->queryBuffers[handle->currentBufferIdx];
        if (queryBuf) {
            memcpy(queryBuf.contents, query, pow2 * sizeof(float));
        } else {
            queryBuf = [device newBufferWithBytes:query length:pow2 * sizeof(float) options:MTLResourceStorageModeShared];
        }
        handle->currentBufferIdx = (handle->currentBufferIdx + 1) % 2;

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
        [encoder setBytes:&pow2 length:sizeof(uint32_t) atIndex:7];
        [encoder setBytes:&bitsPerAngle length:sizeof(uint32_t) atIndex:8];
        [encoder setBuffer:(__bridge id<MTLBuffer>)handle->trigBuffer offset:0 atIndex:9];

        [encoder dispatchThreadgroups:MTLSizeMake(1, 1, 1) threadsPerThreadgroup:MTLSizeMake(1, 1, 1)];
        [encoder endEncoding];
        [cmdBuf commit];
        [cmdBuf waitUntilCompleted];

        *entryPoint = *(uint32_t*)epBuf.contents;
        *entryDist = *(float*)distBuf.contents;

        return 0;
    }
}

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

void metal_set_metric(MetalIndexOptimized* handle, DistanceMetric metric) {
    handle->metric = metric;
}

int metal_add_vectors_optimized(MetalIndexOptimized* handle, float* vectors, int64_t* ids, int count) {
    @autoreleasepool {
        if (!handle || !vectors) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        int requiredCapacity = handle->vectorCount + count;
        int newCapacity = handle->capacity;

        if (requiredCapacity > newCapacity) {
            newCapacity = requiredCapacity > 0 ? requiredCapacity : 1024;
            while (newCapacity < requiredCapacity) {
                newCapacity *= 2;
            }
        }

        size_t bufferSize = (size_t)newCapacity * handle->dimensions * sizeof(float);
        id<MTLBuffer> newVectorBuffer = [device newBufferWithLength:bufferSize
                                                            options:MTLResourceStorageModeShared];

        if (!newVectorBuffer) {
            return -1;
        }

        if (handle->vectorBuffer && handle->vectorCount > 0) {
            id<MTLBuffer> oldBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
            memcpy([newVectorBuffer contents], [oldBuffer contents],
                   (size_t)handle->vectorCount * handle->dimensions * sizeof(float));
            CFRelease(handle->vectorBuffer);
        }

        float* dest = (float*)[newVectorBuffer contents] + (handle->vectorCount * handle->dimensions);
        memcpy(dest, vectors, (size_t)count * handle->dimensions * sizeof(float));

        if (!handle->idBuffer) {
            size_t idBufferSize = (size_t)newCapacity * sizeof(int64_t);
            id<MTLBuffer> idBuf = [device newBufferWithLength:idBufferSize
                                                       options:MTLResourceStorageModeShared];
            if (idBuf) {
                handle->idBuffer = (__bridge_retained void*)idBuf;
            }
        }

        if (handle->idBuffer && ids) {
            id<MTLBuffer> idBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
            int64_t* idDest = (int64_t*)[idBuffer contents] + handle->vectorCount;
            memcpy(idDest, ids, (size_t)count * sizeof(int64_t));
        }

        handle->vectorBuffer = (__bridge_retained void*)newVectorBuffer;
        handle->vectorCount = requiredCapacity;
        handle->capacity = newCapacity;

        return 0;
    }
}

int metal_get_count_optimized(MetalIndexOptimized* handle) {
    return handle ? handle->vectorCount : 0;
}

void metal_cleanup_optimized(MetalIndexOptimized* handle) {
    @autoreleasepool {
        if (handle) {
            if (handle->idBuffer) CFRelease(handle->idBuffer);
            if (handle->vectorBuffer) CFRelease(handle->vectorBuffer);
            if (handle->pqBuffer) CFRelease(handle->pqBuffer);
            if (handle->trigBuffer) CFRelease(handle->trigBuffer);
            if (handle->queryBuffers[0]) CFRelease(handle->queryBuffers[0]);
            if (handle->queryBuffers[1]) CFRelease(handle->queryBuffers[1]);
            free(handle);
        }
    }
}
