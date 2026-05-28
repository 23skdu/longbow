import re

with open('internal/gpu/metal/metal_gpu_optimized.go', 'r') as f:
    content = f.read()

new_search = """// Search using Metal compute shaders with multiple metrics
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
                distancePipeline = (__bridge id<MTLComputePipelineState>)handle->distanceComputePipeline;
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

        // Argument buffer logic for pages
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
        if (threadGroupSize > totalVectors) threadGroupSize = totalVectors;
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

        // We only return IDs if we actually have them.
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
}"""

content = re.sub(r'// Search using Metal compute shaders with multiple metrics\nint metal_search_optimized\(MetalIndexOptimized\* handle, float\* query, int k, int64_t\* resultIDs, float\* resultDistances\) \{.*?\n\}\n', new_search + '\n\n', content, flags=re.DOTALL)

with open('internal/gpu/metal/metal_gpu_optimized.go', 'w') as f:
    f.write(content)
