//go:build gpu && darwin && arm64 && cgo

#import <Accelerate/Accelerate.h>
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#include <math.h>
#include <string.h>

#include "metal_bridge.h"

int metal_search_optimized(MetalIndexOptimized* handle, float* query, void** page_buffers, int* page_starts, int num_pages, int totalVectors, int k, int64_t* resultIDs, float* resultDistances) {
    @autoreleasepool {
        if (!page_buffers || totalVectors == 0 || !query) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;

        id<MTLComputePipelineState> distancePipeline;
        switch (handle->metric) {
            case METRIC_COSINE:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->cosinePipeline;
                break;
            case METRIC_DOT:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->dotPipeline;
                break;
            case METRIC_L2:
            default:
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->distancePagedPipeline;
                break;
        }

        id<MTLComputePipelineState> topKPipeline = (__bridge id<MTLComputePipelineState>)handle->topKPipeline;

        if (!distancePipeline || !topKPipeline) {
            return -1;
        }

        id<MTLBuffer> queryBuf = (__bridge id<MTLBuffer>)handle->queryBuffers[handle->currentBufferIdx];
        if (queryBuf) {
            memcpy(queryBuf.contents, query, handle->dimensions * sizeof(float));
        } else {
            queryBuf = [device newBufferWithBytes:query length:handle->dimensions * sizeof(float) options:MTLResourceStorageModeShared];
        }
        handle->currentBufferIdx = (handle->currentBufferIdx + 1) % 2;

        id<MTLBuffer> distancesBuf = [device newBufferWithLength:totalVectors * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> topDistBuf = [device newBufferWithLength:k * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> topIdxBuf = [device newBufferWithLength:k * sizeof(int) options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> cmdBuf = [queue commandBuffer];

        id<MTLComputeCommandEncoder> distEncoder = [cmdBuf computeCommandEncoder];
        [distEncoder setComputePipelineState:distancePipeline];
        [distEncoder setBuffer:queryBuf offset:0 atIndex:0];

        id<MTLBuffer> argBuf = [device newBufferWithLength:num_pages * sizeof(uint64_t) options:MTLResourceStorageModeShared];
        uint64_t* ptrs = (uint64_t*)[argBuf contents];

        for (int i = 0; i < num_pages; i++) {
            id<MTLBuffer> pb = (__bridge id<MTLBuffer>)page_buffers[i];
            ptrs[i] = pb.gpuAddress;
            [distEncoder useResource:pb usage:MTLResourceUsageRead];
        }
        [distEncoder setBuffer:argBuf offset:0 atIndex:1];

        id<MTLBuffer> startsBuf = [device newBufferWithBytes:page_starts length:(num_pages+1)*sizeof(uint32_t) options:MTLResourceStorageModeShared];
        [distEncoder setBuffer:distancesBuf offset:0 atIndex:2];
        [distEncoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:3];
        [distEncoder setBytes:&totalVectors length:sizeof(uint32_t) atIndex:4];
        [distEncoder setBuffer:startsBuf offset:0 atIndex:5];
        [distEncoder setBytes:&num_pages length:sizeof(uint32_t) atIndex:6];

        NSUInteger threadGroupSize = distancePipeline.maxTotalThreadsPerThreadgroup;
        if (threadGroupSize > (NSUInteger)totalVectors) threadGroupSize = totalVectors;
        MTLSize threadgroups = MTLSizeMake((totalVectors + threadGroupSize - 1) / threadGroupSize, 1, 1);
        MTLSize threadsPerThreadgroup = MTLSizeMake(threadGroupSize, 1, 1);

        [distEncoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threadsPerThreadgroup];
        [distEncoder endEncoding];

        id<MTLComputeCommandEncoder> topKEncoder = [cmdBuf computeCommandEncoder];
        [topKEncoder setComputePipelineState:topKPipeline];
        [topKEncoder setBuffer:distancesBuf offset:0 atIndex:0];
        [topKEncoder setBuffer:topIdxBuf offset:0 atIndex:1];
        [topKEncoder setBuffer:topDistBuf offset:0 atIndex:2];
        [topKEncoder setBytes:&totalVectors length:sizeof(uint32_t) atIndex:3];
        [topKEncoder setBytes:&k length:sizeof(uint32_t) atIndex:4];

        [topKEncoder dispatchThreadgroups:MTLSizeMake(1, 1, 1) threadsPerThreadgroup:MTLSizeMake(1, 1, 1)];
        [topKEncoder endEncoding];

        [cmdBuf commit];
        [cmdBuf waitUntilCompleted];

        int* topIndices = (int*)[topIdxBuf contents];
        float* topDistances = (float*)[topDistBuf contents];

        id<MTLBuffer> idBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
        int64_t* allIDs = idBuffer ? (int64_t*)[idBuffer contents] : NULL;

        for (int i = 0; i < k; i++) {
            resultDistances[i] = topDistances[i];
            int localIdx = topIndices[i];
            if (localIdx >= 0 && localIdx < totalVectors) {
                resultIDs[i] = allIDs ? allIDs[localIdx] : localIdx;
            } else {
                resultIDs[i] = -1;
            }
        }

        return 0;
    }
}

int metal_search_typed(MetalIndexOptimized* handle, void* query, int k, int64_t* resultIDs, float* resultDistances, VectorTypeGPU vtype) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0 || !query) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;

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

        id<MTLBuffer> queryBuffer = (__bridge id<MTLBuffer>)handle->queryBuffers[handle->currentBufferIdx];
        size_t queryLen = handle->dimensions * ((vtype == VECTOR_F16 || vtype == VECTOR_C64) ? sizeof(uint16_t) : sizeof(float));
        if (queryBuffer && queryBuffer.length >= queryLen) {
            memcpy(queryBuffer.contents, query, queryLen);
        } else {
            queryBuffer = [device newBufferWithBytes:query length:queryLen options:MTLResourceStorageModeShared];
        }
        handle->currentBufferIdx = (handle->currentBufferIdx + 1) % 2;

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

        id<MTLBuffer> queryBuffer = (__bridge id<MTLBuffer>)handle->queryBuffers[handle->currentBufferIdx];
        size_t queryLen = (size_t)pow2 * sizeof(float);
        if (queryBuffer && queryBuffer.length >= queryLen) {
            memcpy(queryBuffer.contents, query, queryLen);
        } else {
            queryBuffer = [device newBufferWithBytes:query length:queryLen options:MTLResourceStorageModeShared];
        }
        handle->currentBufferIdx = (handle->currentBufferIdx + 1) % 2;

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
        [encoder setBuffer:(__bridge id<MTLBuffer>)handle->trigBuffer offset:0 atIndex:7];

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

        for (int i = 0; i < k; i++) {
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
        for (int i = 0; i < k; i++) {
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
