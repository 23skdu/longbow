//go:build gpu && darwin && arm64 && cgo

#import <Accelerate/Accelerate.h>
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#include <string.h>

#include "metal_bridge.h"

int metal_assign_to_clusters(MetalIndexHandle* handle, float* vectors, float* centroids, uint32_t* assignments, int numVectors, int numCentroids, int dim) {
    @autoreleasepool {
        if (!handle->assignPipeline) {
            return -1;
        }

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->assignPipeline;

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
        if (maxThreads > (NSUInteger)numVectors) maxThreads = numVectors;
        MTLSize threadgroupSize = MTLSizeMake(maxThreads, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        memcpy(assignments, [assignBuf contents], assignSize);
        return 0;
    }
}

int metal_train_kmeans(MetalIndexHandle* handle, float* data, float* centroids, int n, int dim, int k, int maxIter) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> assignPipeline = (__bridge id<MTLComputePipelineState>)handle->assignPipeline;
        id<MTLComputePipelineState> sumPipeline = (__bridge id<MTLComputePipelineState>)handle->sumCentroidsPipeline;
        id<MTLComputePipelineState> finalizePipeline = (__bridge id<MTLComputePipelineState>)handle->finalizeCentroidsPipeline;

        if (!assignPipeline || !sumPipeline || !finalizePipeline) return -1;

        size_t dataSize = (size_t)n * dim * sizeof(float);
        size_t centSize = (size_t)k * dim * sizeof(float);
        size_t assignSize = (size_t)n * sizeof(uint32_t);
        size_t countSize = (size_t)k * sizeof(uint32_t);

        id<MTLBuffer> dataBuf = [device newBufferWithBytes:data length:dataSize options:MTLResourceStorageModeShared];
        id<MTLBuffer> centBuf = [device newBufferWithBytes:centroids length:centSize options:MTLResourceStorageModeShared];
        id<MTLBuffer> assignBuf = [device newBufferWithLength:assignSize options:MTLResourceStorageModeShared];
        id<MTLBuffer> countBuf = [device newBufferWithLength:countSize options:MTLResourceStorageModeShared];

        for (int iter = 0; iter < maxIter; iter++) {
            id<MTLCommandBuffer> cb1 = [queue commandBuffer];
            id<MTLComputeCommandEncoder> enc1 = [cb1 computeCommandEncoder];
            [enc1 setComputePipelineState:assignPipeline];
            [enc1 setBuffer:dataBuf offset:0 atIndex:0];
            [enc1 setBuffer:centBuf offset:0 atIndex:1];
            [enc1 setBuffer:assignBuf offset:0 atIndex:2];
            [enc1 setBytes:&dim length:sizeof(int) atIndex:3];
            [enc1 setBytes:&n length:sizeof(int) atIndex:4];
            [enc1 setBytes:&k length:sizeof(int) atIndex:5];
            [enc1 dispatchThreads:MTLSizeMake(n, 1, 1) threadsPerThreadgroup:MTLSizeMake(MIN(n, 256), 1, 1)];
            [enc1 endEncoding];
            [cb1 commit];
            [cb1 waitUntilCompleted];

            memset([countBuf contents], 0, countSize);
            memset([centBuf contents], 0, centSize);

            id<MTLCommandBuffer> cb2 = [queue commandBuffer];
            id<MTLComputeCommandEncoder> enc2 = [cb2 computeCommandEncoder];
            [enc2 setComputePipelineState:sumPipeline];
            [enc2 setBuffer:dataBuf offset:0 atIndex:0];
            [enc2 setBuffer:assignBuf offset:0 atIndex:1];
            [enc2 setBuffer:centBuf offset:0 atIndex:2];
            [enc2 setBuffer:countBuf offset:0 atIndex:3];
            [enc2 setBytes:&dim length:sizeof(int) atIndex:4];
            [enc2 setBytes:&n length:sizeof(int) atIndex:5];
            [enc2 dispatchThreads:MTLSizeMake(n, 1, 1) threadsPerThreadgroup:MTLSizeMake(MIN(n, 256), 1, 1)];
            [enc2 endEncoding];
            [cb2 commit];
            [cb2 waitUntilCompleted];

            id<MTLCommandBuffer> cb3 = [queue commandBuffer];
            id<MTLComputeCommandEncoder> enc3 = [cb3 computeCommandEncoder];
            [enc3 setComputePipelineState:finalizePipeline];
            [enc3 setBuffer:centBuf offset:0 atIndex:0];
            [enc3 setBuffer:countBuf offset:0 atIndex:1];
            [enc3 setBytes:&dim length:sizeof(int) atIndex:2];
            [enc3 setBytes:&k length:sizeof(int) atIndex:3];
            [enc3 dispatchThreads:MTLSizeMake(k, 1, 1) threadsPerThreadgroup:MTLSizeMake(MIN(k, 256), 1, 1)];
            [enc3 endEncoding];
            [cb3 commit];
            [cb3 waitUntilCompleted];
        }

        memcpy(centroids, [centBuf contents], centSize);
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

int metal_graph_expand(MetalIndexHandle* handle, uint32_t* seeds, int numSeeds, int depth, float alpha, uint32_t* outIDs, float* outScores, int* outCount) {
    @autoreleasepool {
        if (!handle->graphOffsets || !handle->fusedGraphPipeline) return -1;

        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->fusedGraphPipeline;

        int nodeCount = handle->graphNodeCount;

        id<MTLBuffer> activations = [device newBufferWithLength:nodeCount * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> nextActivations = [device newBufferWithLength:nodeCount * sizeof(float) options:MTLResourceStorageModeShared];
        id<MTLBuffer> visited = [device newBufferWithLength:((nodeCount + 31) / 32) * sizeof(uint32_t) options:MTLResourceStorageModeShared];
        id<MTLBuffer> frontier = [device newBufferWithLength:nodeCount * sizeof(uint32_t) options:MTLResourceStorageModeShared];
        id<MTLBuffer> nextFrontier = [device newBufferWithLength:nodeCount * sizeof(uint32_t) options:MTLResourceStorageModeShared];
        id<MTLBuffer> nextFrontierSize = [device newBufferWithLength:sizeof(uint32_t) options:MTLResourceStorageModeShared];

        float* actPtr = (float*)[activations contents];
        uint32_t* frontPtr = (uint32_t*)[frontier contents];
        uint32_t* visPtr = (uint32_t*)[visited contents];
        memset(visPtr, 0, ((nodeCount + 31) / 32) * sizeof(uint32_t));
        memset(actPtr, 0, nodeCount * sizeof(float));

        for (int i = 0; i < numSeeds; i++) {
            actPtr[seeds[i]] = 1.0f;
            frontPtr[i] = seeds[i];
            visPtr[seeds[i] / 32] |= (1 << (seeds[i] % 32));
        }
        int currentFrontierSize = numSeeds;

        for (int d = 0; d < depth; d++) {
            if (currentFrontierSize == 0) break;

            memset([nextActivations contents], 0, nodeCount * sizeof(float));
            memset([nextFrontierSize contents], 0, sizeof(uint32_t));

            id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
            id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

            [encoder setComputePipelineState:pipeline];
            [encoder setBuffer:frontier offset:0 atIndex:0];
            [encoder setBuffer:(__bridge id<MTLBuffer>)handle->graphOffsets offset:0 atIndex:1];
            [encoder setBuffer:(__bridge id<MTLBuffer>)handle->graphNeighbors offset:0 atIndex:2];
            [encoder setBuffer:(__bridge id<MTLBuffer>)handle->graphWeights offset:0 atIndex:3];
            [encoder setBuffer:activations offset:0 atIndex:4];
            [encoder setBuffer:nextActivations offset:0 atIndex:5];
            [encoder setBuffer:nextFrontier offset:0 atIndex:6];
            [encoder setBuffer:nextFrontierSize offset:0 atIndex:7];
            [encoder setBuffer:visited offset:0 atIndex:8];

            [encoder setBytes:&currentFrontierSize length:sizeof(int) atIndex:0];
            [encoder setBytes:&alpha length:sizeof(float) atIndex:1];

            MTLSize gridSize = MTLSizeMake(currentFrontierSize, 1, 1);
            NSUInteger maxThreads = pipeline.maxTotalThreadsPerThreadgroup;
            if (maxThreads > (NSUInteger)currentFrontierSize) maxThreads = currentFrontierSize;
            MTLSize threadgroupSize = MTLSizeMake(maxThreads, 1, 1);

            [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
            [encoder endEncoding];
            [commandBuffer commit];
            [commandBuffer waitUntilCompleted];

            id<MTLBuffer> tmpAct = activations; activations = nextActivations; nextActivations = tmpAct;
            id<MTLBuffer> tmpFront = frontier; frontier = nextFrontier; nextFrontier = tmpFront;
            currentFrontierSize = *(uint32_t*)[nextFrontierSize contents];
        }

        float* finalActs = (float*)[activations contents];
        int count = 0;
        for (int i = 0; i < nodeCount; i++) {
            if (finalActs[i] > 0.01f) {
                outIDs[count] = i;
                outScores[count] = finalActs[i];
                count++;
            }
        }
        *outCount = count;

        return 0;
    }
}

int metal_quantize_sq8(MetalIndexHandle* handle, float* vectors, float* mins, float* maxs, unsigned char* results, int count) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->quantizeSQ8Pipeline;

        if (!pipeline) return -1;

        int totalElements = count * handle->dimensions;
        size_t vecSize = (size_t)totalElements * sizeof(float);
        size_t minMaxSize = (size_t)handle->dimensions * sizeof(float);
        size_t resSize = (size_t)totalElements * sizeof(unsigned char);

        id<MTLBuffer> vecBuf = [device newBufferWithBytes:vectors length:vecSize options:MTLResourceStorageModeShared];
        id<MTLBuffer> minBuf = [device newBufferWithBytes:mins length:minMaxSize options:MTLResourceStorageModeShared];
        id<MTLBuffer> maxBuf = [device newBufferWithBytes:maxs length:minMaxSize options:MTLResourceStorageModeShared];
        id<MTLBuffer> resBuf = [device newBufferWithLength:resSize options:MTLResourceStorageModeShared];

        id<MTLCommandBuffer> commandBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline];
        [encoder setBuffer:vecBuf offset:0 atIndex:0];
        [encoder setBuffer:minBuf offset:0 atIndex:1];
        [encoder setBuffer:maxBuf offset:0 atIndex:2];
        [encoder setBuffer:resBuf offset:0 atIndex:3];
        [encoder setBytes:&handle->dimensions length:sizeof(uint32_t) atIndex:0];

        MTLSize gridSize = MTLSizeMake(totalElements, 1, 1);
        NSUInteger maxThreads = pipeline.maxTotalThreadsPerThreadgroup;
        if (maxThreads > (NSUInteger)totalElements) maxThreads = totalElements;
        MTLSize threadgroupSize = MTLSizeMake(maxThreads, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadgroupSize];
        [encoder endEncoding];
        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        memcpy(results, [resBuf contents], resSize);
        return 0;
    }
}

int metal_prune_neighbors(MetalIndexHandle* handle, uint32_t* candidateIds, float* candidateDists, uint32_t* selectedIds, uint32_t* selectedCount, float* allVectors, int maxNeighbors, int numCandidates, int dim, bool extended) {
    @autoreleasepool {
        id<MTLDevice> device = (__bridge id<MTLDevice>)handle->device;
        id<MTLCommandQueue> queue = (__bridge id<MTLCommandQueue>)handle->commandQueue;
        id<MTLComputePipelineState> pipeline = (__bridge id<MTLComputePipelineState>)handle->prunePipeline;

        if (!pipeline) return -1;

        id<MTLBuffer> candIdBuf;
        id<MTLBuffer> candDistBuf;
        id<MTLBuffer> selIdBuf;
        id<MTLBuffer> selCountBuf;

        if (handle->pruneScratchCapacity >= numCandidates && handle->pruneScratchCapacity >= maxNeighbors) {
            candIdBuf = (__bridge id<MTLBuffer>)handle->pruneCandIdBuffer;
            candDistBuf = (__bridge id<MTLBuffer>)handle->pruneCandDistBuffer;
            selIdBuf = (__bridge id<MTLBuffer>)handle->pruneSelIdBuffer;
            selCountBuf = (__bridge id<MTLBuffer>)handle->pruneSelCountBuffer;

            memcpy([candIdBuf contents], candidateIds, numCandidates * sizeof(uint32_t));
            memcpy([candDistBuf contents], candidateDists, numCandidates * sizeof(float));
        } else {
            candIdBuf = [device newBufferWithBytes:candidateIds length:numCandidates * sizeof(uint32_t) options:MTLResourceStorageModeShared];
            candDistBuf = [device newBufferWithBytes:candidateDists length:numCandidates * sizeof(float) options:MTLResourceStorageModeShared];
            selIdBuf = [device newBufferWithLength:maxNeighbors * sizeof(uint32_t) options:MTLResourceStorageModeShared];
            selCountBuf = [device newBufferWithLength:sizeof(uint32_t) options:MTLResourceStorageModeShared];
        }

        id<MTLBuffer> allVecBuf;
        if (allVectors != NULL) {
            allVecBuf = [device newBufferWithBytes:allVectors length:(size_t)handle->vectorCount * dim * sizeof(float) options:MTLResourceStorageModeShared];
        } else {
            allVecBuf = (__bridge id<MTLBuffer>)handle->vectorBuffer;
        }

        if (!allVecBuf) return -2;

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
