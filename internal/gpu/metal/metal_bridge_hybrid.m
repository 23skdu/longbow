//go:build gpu && darwin && arm64 && cgo

#import <Accelerate/Accelerate.h>
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#include <stdint.h>
#include <string.h>

#include "metal_bridge.h"

extern void goMetalBatchCallback(uintptr_t handleVal);

static const char* const hybridShaderSource =
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
"}\n"
"\n"
"kernel void compute_batch_distances(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device const uint* candidateIDs [[buffer(2)]],\n"
"    device float* distances [[buffer(3)]],\n"
"    constant uint& dim [[buffer(4)]],\n"
"    constant uint& numCandidates [[buffer(5)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numCandidates) return;\n"
"    \n"
"    uint vectorID = candidateIDs[gid];\n"
"    float sum = 0.0f;\n"
"    uint offset = vectorID * dim;\n"
"    \n"
"    for (uint i = 0; i < dim; i++) {\n"
"        float diff = query[i] - vectors[offset + i];\n"
"        sum += diff * diff;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sum);\n"
"}\n"
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
"}\n";

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

        id<MTLFunction> pqFunc = [library newFunctionWithName:@"compute_pq_distances"];
        id<MTLComputePipelineState> pqPipeline = [device newComputePipelineStateWithFunction:pqFunc error:&error];

        id<MTLFunction> batchFunc = [library newFunctionWithName:@"compute_batch_distances"];
        id<MTLComputePipelineState> batchPipeline = batchFunc ? [device newComputePipelineStateWithFunction:batchFunc error:&error] : nil;

        MetalHybridIndex* handle = (MetalHybridIndex*)malloc(sizeof(MetalHybridIndex));
        handle->device = (__bridge_retained void*)device;
        handle->commandQueue = (__bridge_retained void*)queue;
        handle->vectorBuffer = NULL;
        handle->distancePipeline = (__bridge_retained void*)pipeline;
        handle->pqPipeline = pqPipeline ? (__bridge_retained void*)pqPipeline : NULL;
        handle->batchPipeline = batchPipeline ? (__bridge_retained void*)batchPipeline : NULL;
        handle->vectorCount = 0;
        handle->dimensions = dimensions;

        return handle;
    }
}

int metal_hybrid_add(MetalHybridIndex* handle, float* vectors, int count) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;

        size_t bufferSize = (size_t)count * handle->dimensions * sizeof(float);
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
        if (threadGroupSize > (NSUInteger)handle->vectorCount) threadGroupSize = handle->vectorCount;
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        memcpy(distances, [distanceBuffer contents], handle->vectorCount * sizeof(float));

        return 0;
    }
}

int metal_hybrid_compute_pq_distances(MetalHybridIndex* handle, float* lookupTable, int m, float* distances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0 || !handle->pqPipeline) return -1;

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> codesBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->pqPipeline;

        size_t tableSize = (size_t)m * 256 * sizeof(float);
        id<MTLBuffer> tableBuffer = [device newBufferWithBytes:lookupTable
                                                       length:tableSize
                                                      options:MTLResourceStorageModeShared];

        id<MTLBuffer> distBuffer = [device newBufferWithLength:handle->vectorCount * sizeof(float)
                                                       options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:tableBuffer offset:0 atIndex:0];
        [encoder setBuffer:codesBuffer offset:0 atIndex:1];
        [encoder setBuffer:distBuffer offset:0 atIndex:2];
        [encoder setBytes:&m length:sizeof(uint32_t) atIndex:3];
        [encoder setBytes:&handle->vectorCount length:sizeof(uint32_t) atIndex:4];

        MTLSize gridSize = MTLSizeMake(handle->vectorCount, 1, 1);
        NSUInteger threadGroupSize = pipeline.maxTotalThreadsPerThreadgroup;
        if (threadGroupSize > (NSUInteger)handle->vectorCount) threadGroupSize = handle->vectorCount;
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        memcpy(distances, [distBuffer contents], handle->vectorCount * sizeof(float));

        return 0;
    }
}

int metal_hybrid_compute_batch_distances(MetalHybridIndex* handle, float* query, uint32_t* candidateIDs, int numCandidates, float* distances) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0 || !handle->batchPipeline || numCandidates == 0) return -1;

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->batchPipeline;

        id<MTLBuffer> queryBuffer = [device newBufferWithBytes:query
                                                        length:handle->dimensions * sizeof(float)
                                                       options:MTLResourceStorageModeShared];

        id<MTLBuffer> candidateBuffer = [device newBufferWithBytes:candidateIDs
                                                            length:numCandidates * sizeof(uint32_t)
                                                           options:MTLResourceStorageModeShared];

        id<MTLBuffer> distanceBuffer = [device newBufferWithLength:numCandidates * sizeof(float)
                                                            options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:queryBuffer offset:0 atIndex:0];
        [encoder setBuffer:vectorBuffer offset:0 atIndex:1];
        [encoder setBuffer:candidateBuffer offset:0 atIndex:2];
        [encoder setBuffer:distanceBuffer offset:0 atIndex:3];
        [encoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:4];
        [encoder setBytes:&numCandidates length:sizeof(uint32_t) atIndex:5];

        MTLSize gridSize = MTLSizeMake(numCandidates, 1, 1);
        NSUInteger threadGroupSize = pipeline.maxTotalThreadsPerThreadgroup;
        if (threadGroupSize > (NSUInteger)numCandidates) threadGroupSize = numCandidates;
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        memcpy(distances, [distanceBuffer contents], numCandidates * sizeof(float));

        return 0;
    }
}

int metal_hybrid_compute_batch_distances_async(MetalHybridIndex* handle, float* query, uint32_t* candidateIDs, int numCandidates, float* distances, uintptr_t callbackHandle) {
    @autoreleasepool {
        if (!handle->vectorBuffer || handle->vectorCount == 0 || !handle->batchPipeline || numCandidates == 0) return -1;

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLBuffer> vectorBuffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->batchPipeline;

        id<MTLBuffer> queryBuffer = [device newBufferWithBytes:query
                                                        length:handle->dimensions * sizeof(float)
                                                       options:MTLResourceStorageModeShared];

        id<MTLBuffer> candidateBuffer = [device newBufferWithBytes:candidateIDs
                                                            length:numCandidates * sizeof(uint32_t)
                                                           options:MTLResourceStorageModeShared];

        id<MTLBuffer> distanceBuffer = [device newBufferWithLength:numCandidates * sizeof(float)
                                                            options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:queryBuffer offset:0 atIndex:0];
        [encoder setBuffer:vectorBuffer offset:0 atIndex:1];
        [encoder setBuffer:candidateBuffer offset:0 atIndex:2];
        [encoder setBuffer:distanceBuffer offset:0 atIndex:3];
        [encoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:4];
        [encoder setBytes:&numCandidates length:sizeof(uint32_t) atIndex:5];

        MTLSize gridSize = MTLSizeMake(numCandidates, 1, 1);
        NSUInteger threadGroupSize = pipeline.maxTotalThreadsPerThreadgroup;
        if (threadGroupSize > (NSUInteger)numCandidates) threadGroupSize = numCandidates;
        MTLSize threadgroupSize = MTLSizeMake(threadGroupSize, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];

        [commandBuffer addCompletedHandler:^(id<MTLCommandBuffer> cb) {
            memcpy(distances, [distanceBuffer contents], numCandidates * sizeof(float));
            goMetalBatchCallback(callbackHandle);
        }];

        [commandBuffer commit];

        return 0;
    }
}

void metal_hybrid_cleanup(MetalHybridIndex* handle) {
    @autoreleasepool {
        if (handle->vectorBuffer) CFRelease(handle->vectorBuffer);
        if (handle->distancePipeline) CFRelease(handle->distancePipeline);
        if (handle->pqPipeline) CFRelease(handle->pqPipeline);
        if (handle->batchPipeline) CFRelease(handle->batchPipeline);
        if (handle->commandQueue) CFRelease(handle->commandQueue);
        if (handle->device) CFRelease(handle->device);
        free(handle);
    }
}

void* metal_hybrid_get_vector_buffer(MetalHybridIndex* handle) {
    id<MTLBuffer> buffer = (__bridge id<MTLBuffer>)handle->vectorBuffer;
    return [buffer contents];
}
