//go:build gpu && darwin && arm64 && cgo

#import <Accelerate/Accelerate.h>
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#include <math.h>
#include <string.h>

#include "metal_bridge.h"

// select_topk_cpu picks the k smallest distances on CPU using partial selection sort.
static inline void select_topk_cpu(
    float* distances,
    const int64_t* ids,
    int n,
    int k,
    int64_t* resultIDs,
    float* resultDistances
) {
    int resultCount = k < n ? k : n;
    for (int i = 0; i < resultCount; i++) {
        int minIdx = -1;
        float minDist = INFINITY;
        for (int j = 0; j < n; j++) {
            if (distances[j] < minDist) {
                minDist = distances[j];
                minIdx = j;
            }
        }
        if (minIdx < 0) {
            resultIDs[i] = -1;
            resultDistances[i] = INFINITY;
            continue;
        }
        resultIDs[i] = ids[minIdx];
        resultDistances[i] = minDist;
        distances[minIdx] = INFINITY;
    }
}

void* create_shared_buffer(void* device, size_t size) {
    @autoreleasepool {
        id<MTLDevice> d = (__bridge id<MTLDevice>)device;
        id<MTLBuffer> buffer = [d newBufferWithLength:size options:MTLResourceStorageModeShared];
        return (__bridge_retained void*)buffer;
    }
}

void metal_get_device_info(MetalIndexHandle* handle, char* name, int maxLen, uint64_t* totalMem) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        NSString* deviceName = [device name];
        strncpy(name, [deviceName UTF8String], maxLen - 1);
        name[maxLen - 1] = '\0';
        *totalMem = (uint64_t)[device recommendedMaxWorkingSetSize];
    }
}

int metal_add_vectors(MetalIndexHandle* handle, float* vectors, int64_t* ids, int count) {
    @autoreleasepool {
        if (!handle->vectorBuffer) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        int requiredCapacity = handle->vectorCount + count;
        if (requiredCapacity > handle->capacity) {
            int newCapacity = handle->capacity;
            while (newCapacity < requiredCapacity) {
                newCapacity *= 2;
            }

            size_t newBufferSize = (size_t)newCapacity * handle->dimensions * sizeof(float);
            id<MTLBuffer> newBuffer = [device newBufferWithLength:newBufferSize
                                                            options:MTLResourceStorageModeShared];
            if (!newBuffer) {
                return -1;
            }

            id<MTLBuffer> oldBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
            memcpy([newBuffer contents], [oldBuffer contents],
                   (size_t)handle->vectorCount * handle->dimensions * sizeof(float));

            CFRelease(handle->vectorBuffer);
            handle->vectorBuffer = (__bridge_retained void*)newBuffer;

            if (handle->idBuffer) {
                size_t newIdBufferSize = (size_t)newCapacity * sizeof(int64_t);
                id<MTLBuffer> newIdBuffer = [device newBufferWithLength:newIdBufferSize
                                                                options:MTLResourceStorageModeShared];
                if (newIdBuffer) {
                    id<MTLBuffer> oldIdBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
                    memcpy([newIdBuffer contents], [oldIdBuffer contents],
                           (size_t)handle->vectorCount * sizeof(int64_t));
                    CFRelease(handle->idBuffer);
                    handle->idBuffer = (__bridge_retained void*)newIdBuffer;
                }
            }

            handle->capacity = newCapacity;
        }

        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        float* dest = (float*)[vectorBuffer contents] + (handle->vectorCount * (long)handle->dimensions);
        memcpy(dest, vectors, count * (long)handle->dimensions * sizeof(float));

        if (!handle->idBuffer) {
            size_t idBufferSize = (size_t)handle->capacity * sizeof(int64_t);
            id<MTLBuffer> idBuf = [device newBufferWithLength:idBufferSize
                                                       options:MTLResourceStorageModeShared];
            if (!idBuf) {
                return -1;
            }
            handle->idBuffer = (__bridge_retained void*)idBuf;
        }

        id<MTLBuffer> idBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
        int64_t* idDest = (int64_t*)[idBuffer contents] + handle->vectorCount;
        memcpy(idDest, ids, (size_t)count * sizeof(int64_t));

        handle->vectorCount += count;

        return 0;
    }
}

int metal_compute_distances_gpu(MetalIndexHandle* handle, float* query, float* distances, int count, int type) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> pipeline = (type == 0)
            ? (__bridge id<MTLComputePipelineState>)handle->l2DistancePipeline
            : (__bridge id<MTLComputePipelineState>)handle->ipDistancePipeline;

        if (!pipeline) return -1;

        id<MTLBuffer> queryBuf = [device newBufferWithBytes:query length:handle->dimensions * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> distBuf = [device newBufferWithLength:count * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> vecBuf = (__bridge id<MTLBuffer>)handle->vectorBuffer;

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:queryBuf offset:0 atIndex:0];
        [encoder setBuffer:vecBuf offset:0 atIndex:1];
        [encoder setBuffer:distBuf offset:0 atIndex:2];
        [encoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:3];
        [encoder setBytes:&count length:sizeof(uint32_t) atIndex:4];

        MTLSize gridSize = MTLSizeMake(count, 1, 1);
        NSUInteger maxThreads = pipeline.maxTotalThreadsPerThreadgroup;
        if (maxThreads > (NSUInteger)count) maxThreads = count;
        MTLSize threadgroupSize = MTLSizeMake(maxThreads, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        memcpy(distances, [distBuf contents], count * sizeof(float));
        return 0;
    }
}

int metal_search(MetalIndexHandle* handle, float* query, int k, int64_t* resultIDs, float* resultDistances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0) {
            return -1;
        }

        float* distances = (float*)malloc(handle->vectorCount * sizeof(float));
        if (!distances) {
            return -1;
        }

        if (metal_compute_distances_gpu(handle, query, distances, handle->vectorCount, 0) != 0) {
            free(distances);
            return -1;
        }

        id<MTLBuffer> idBuffer = (__bridge id<MTLBuffer>)handle->idBuffer;
        int64_t* ids = (int64_t*)[idBuffer contents];
        select_topk_cpu(distances, ids, handle->vectorCount, k, resultIDs, resultDistances);

        free(distances);
        return 0;
    }
}

int metal_get_count(MetalIndexHandle* handle) {
    return handle->vectorCount;
}

int metal_haversine_batch(MetalIndexHandle* handle, float* center, float* points, float* results, float earthRadius, int count) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->haversinePipeline;

        if (!pipeline) return -1;

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

        MTLSize gridSize = MTLSizeMake(count, 1, 1);
        NSUInteger maxThreads = pipeline.maxTotalThreadsPerThreadgroup;
        if (maxThreads > (NSUInteger)count) maxThreads = count;
        MTLSize threadgroupSize = MTLSizeMake(maxThreads, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        memcpy(results, [resBuf contents], count * sizeof(float));
        return 0;
    }
}

int metal_norm_batch_f32(MetalIndexHandle* handle, float* vectors, float* results, int dims, int count) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->normPipeline;

        if (!pipeline) return -1;

        size_t vecSize = (size_t)count * dims * sizeof(float);
        id<MTLBuffer> vecBuf = [device newBufferWithBytes:vectors length:vecSize options:MTLResourceStorageModeShared];
        id<MTLBuffer> resBuf = [device newBufferWithLength:count * sizeof(float) options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:vecBuf offset:0 atIndex:0];
        [encoder setBuffer:resBuf offset:0 atIndex:1];
        [encoder setBytes:&dims length:sizeof(uint32_t) atIndex:2];
        [encoder setBytes:&count length:sizeof(uint32_t) atIndex:3];

        MTLSize gridSize = MTLSizeMake(count, 1, 1);
        NSUInteger maxThreads = pipeline.maxTotalThreadsPerThreadgroup;
        if (maxThreads > (NSUInteger)count) maxThreads = count;
        MTLSize threadgroupSize = MTLSizeMake(maxThreads, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        memcpy(results, [resBuf contents], count * sizeof(float));
        return 0;
    }
}

void metal_cleanup(MetalIndexHandle* handle) {
    @autoreleasepool {
        if (handle->idBuffer) CFRelease(handle->idBuffer);
        if (handle->vectorBuffer) CFRelease(handle->vectorBuffer);
        if (handle->graphOffsets) CFRelease(handle->graphOffsets);
        if (handle->graphNeighbors) CFRelease(handle->graphNeighbors);
        if (handle->graphWeights) CFRelease(handle->graphWeights);
        if (handle->pruneCandIdBuffer) CFRelease(handle->pruneCandIdBuffer);
        if (handle->pruneCandDistBuffer) CFRelease(handle->pruneCandDistBuffer);
        if (handle->pruneSelIdBuffer) CFRelease(handle->pruneSelIdBuffer);
        if (handle->pruneSelCountBuffer) CFRelease(handle->pruneSelCountBuffer);
        free(handle);
    }
}

int metal_search_pq(MetalIndexHandle* handle, float* lookupTable, int m, int k, int64_t* resultIDs, float* resultDistances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0 || !handle->pqPipeline) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> codesBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->pqPipeline;

        size_t distSize = handle->vectorCount * sizeof(float);
        id<MTLBuffer> distBuffer = [device newBufferWithLength:distSize options:MTLResourceStorageModeShared];

        size_t tableSize = (size_t)m * 256 * sizeof(float);
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
        if (threadGroupSize > (NSUInteger)handle->vectorCount) threadGroupSize = handle->vectorCount;
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        float* distances = (float*)[distBuffer contents];
        int64_t* ids = (int64_t*)[(__bridge id<MTLBuffer>)handle->idBuffer contents];
        select_topk_cpu(distances, ids, handle->vectorCount, k, resultIDs, resultDistances);

        return 0;
    }
}

void* metal_get_vector_buffer(MetalIndexHandle* handle) {
    id<MTLBuffer> buffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
    return [buffer contents];
}

void* metal_get_id_buffer(MetalIndexHandle* handle) {
    id<MTLBuffer> buffer = (__bridge id<MTLBuffer>)handle->idBuffer;
    return [buffer contents];
}
