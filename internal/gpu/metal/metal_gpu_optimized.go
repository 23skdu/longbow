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

// Metal shader source for optimized L2 distance calculation, top-k selection, and batched queries
const char* metalShaderSource =
"#include <metal_stdlib>\n"
"using namespace metal;\n"
"\n"
"// Optimized L2 distance with SIMD vectorization and loop unrolling\n"
"kernel void compute_l2_distances(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]],\n"
"    uint tid [[thread_index_in_threadgroup]],\n"
"    uint tg_size [[threads_per_threadgroup]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint offset = gid * dim;\n"
"    \n"
"    // Use SIMD vector loads for 4-way parallelism\n"
"    float sum = 0.0f;\n"
"    uint vectorWidth = dim / 4;\n"
"    uint remainder = dim % 4;\n"
"    \n"
"    // Process 4 elements at a time using vector loads\n"
"    for (uint i = 0; i < vectorWidth; i++) {\n"
"        float4 queryVec = float4(query[i*4], query[i*4+1], query[i*4+2], query[i*4+3]);\n"
"        float4 vecVec = float4(vectors[offset + i*4], vectors[offset + i*4+1], \n"
"                              vectors[offset + i*4+2], vectors[offset + i*4+3]);\n"
"        float4 diff = queryVec - vecVec;\n"
"        sum += dot(diff, diff);\n"
"    }\n"
"    \n"
"    // Handle remainder\n"
"    for (uint i = 0; i < remainder; i++) {\n"
"        float diff = query[vectorWidth * 4 + i] - vectors[offset + vectorWidth * 4 + i];\n"
"        sum += diff * diff;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sum);\n"
"}\n"
"\n"
"// Optimized L2 distance - simple version for small dimensions\n"
"kernel void compute_l2_distances_simple(\n"
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
"    // Unrolled loop for better instruction-level parallelism\n"
"    for (uint i = 0; i < dim; i++) {\n"
"        float diff = query[i] - vectors[offset + i];\n"
"        sum += diff * diff;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sum);\n"
"}\n"
"\n"
"// Cosine similarity computation\n"
"kernel void compute_cosine_similarity(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* similarities [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    float dotProd = 0.0f;\n"
"    float queryMag = 0.0f;\n"
"    float vecMag = 0.0f;\n"
"    uint offset = gid * dim;\n"
"    \n"
"    for (uint i = 0; i < dim; i++) {\n"
"        float q = query[i];\n"
"        float v = vectors[offset + i];\n"
"        dotProd += q * v;\n"
"        queryMag += q * q;\n"
"        vecMag += v * v;\n"
"    }\n"
"    \n"
"    float denom = sqrt(queryMag) * sqrt(vecMag);\n"
"    similarities[gid] = (denom > 1e-10f) ? (dotProd / denom) : 0.0f;\n"
"}\n"
"\n"
"// Dot product computation\n"
"kernel void compute_dot_product(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* products [[buffer(2)]],\n"
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
"        sum += query[i] * vectors[offset + i];\n"
"    }\n"
"    \n"
"    products[gid] = sum;\n"
"}\n"
"\n"
"// Asymmetric PQ distance computation\n"
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
"        sum += lookupTable[i * 256 + codes[offset + i]];\n"
"    }\n"
"    \n"
"    distances[gid] = sum;\n"
"}\n"
"\n"
"// Parallel reduction for top-k using threadgroups\n"
"kernel void find_top_k_parallel(\n"
"    device const float* distances [[buffer(0)]],\n"
"    device int* indices [[buffer(1)]],\n"
"    device float* topDistances [[buffer(2)]],\n"
"    constant uint& numVectors [[buffer(3)]],\n"
"    constant uint& k [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]],\n"
"    uint tid [[thread_index_in_threadgroup]],\n"
"    uint tg_size [[threads_per_threadgroup]])\n"
"{\n"
"    // Each threadgroup finds local top-k\n"
"    threadgroup float localDists[256];\n"
"    threadgroup uint localIndices[256];\n"
"    \n"
"    // Load into local memory with bounds checking\n"
"    if (gid < numVectors) {\n"
"        localDists[tid] = distances[gid];\n"
"        localIndices[tid] = gid;\n"
"    } else {\n"
"        localDists[tid] = INFINITY;\n"
"        localIndices[tid] = UINT_MAX;\n"
"    }\n"
"    \n"
"    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"    \n"
"    // Parallel selection within threadgroup\n"
"    for (uint i = 0; i < k && i < tg_size; i++) {\n"
"        if (tid == i) {\n"
"            // Find minimum in remaining\n"
"            float minDist = localDists[tid];\n"
"            uint minIdx = tid;\n"
"            \n"
"            for (uint j = tid + 1; j < tg_size; j++) {\n"
"                if (localDists[j] < minDist) {\n"
"                    minDist = localDists[j];\n"
"                    minIdx = j;\n"
"                }\n"
"            }\n"
"            \n"
"            // Store to global if within top-k\n"
"            if (i < k) {\n"
"                topDistances[i] = minDist;\n"
"                indices[i] = localIndices[minIdx];\n"
"            }\n"
"            \n"
"            // Mark as used\n"
"            localDists[minIdx] = INFINITY;\n"
"        }\n"
"        \n"
"        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"    }\n"
"}\n"
"\n"
"// Fast top-k using heap-based selection (better for large k)\n"
"kernel void find_top_k_heap(\n"
"    device const float* distances [[buffer(0)]],\n"
"    device int* indices [[buffer(1)]],\n"
"    device float* topDistances [[buffer(2)]],\n"
"    constant uint& numVectors [[buffer(3)]],\n"
"    constant uint& k [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    // Single-threaded heap construction\n"
"    if (gid == 0) {\n"
"        uint heapSize = min(k, numVectors);\n"
"        \n"
"        // Initialize heap with default values\n"
"        for (uint i = 0; i < heapSize; i++) {\n"
"            topDistances[i] = INFINITY;\n"
"            indices[i] = -1;\n"
"        }\n"
"        \n"
"        // Iterate all elements and maintain max-heap\n"
"        for (uint i = 0; i < numVectors; i++) {\n"
"            float currentDist = distances[i];\n"
"            if (currentDist < topDistances[0]) {\n"
"                // Replace root\n"
"                float parentDist = currentDist;\n"
"                int parentIdx = i;\n"
"                topDistances[0] = parentDist;\n"
"                indices[0] = parentIdx;\n"
"                \n"
"                // Heapify down\n"
"                uint parent = 0;\n"
"                while (true) {\n"
"                    uint child = 2 * parent + 1;\n"
"                    if (child >= heapSize) break;\n"
"                    \n"
"                    if (child + 1 < heapSize && topDistances[child + 1] > topDistances[child]) {\n"
"                        child++;\n"
"                    }\n"
"                    \n"
"                    if (parentDist >= topDistances[child]) break;\n"
"                    \n"
"                    topDistances[parent] = topDistances[child];\n"
"                    indices[parent] = indices[child];\n"
"                    \n"
"                    parent = child;\n"
"                }\n"
"                \n"
"                topDistances[parent] = parentDist;\n"
"                indices[parent] = parentIdx;\n"
"            }\n"
"        }\n"
"        \n"
"        // Sort final heap (ascending for top-k)\n"
"        for (uint i = heapSize - 1; i > 0; i--) {\n"
"            float tempDist = topDistances[0];\n"
"            int tempIdx = indices[0];\n"
"            topDistances[0] = topDistances[i];\n"
"            indices[0] = indices[i];\n"
"            topDistances[i] = tempDist;\n"
"            indices[i] = tempIdx;\n"
"            \n"
"            // Heapify\n"
"            uint parent = 0;\n"
"            uint child = 1;\n"
"            while (child < i) {\n"
"                if (child + 1 < i && topDistances[child + 1] > topDistances[child]) {\n"
"                    child++;\n"
"                }\n"
"                if (topDistances[parent] >= topDistances[child]) break;\n"
"                \n"
"                float swapDist = topDistances[parent];\n"
"                int swapIdx = indices[parent];\n"
"                topDistances[parent] = topDistances[child];\n"
"                indices[parent] = indices[child];\n"
"                topDistances[child] = swapDist;\n"
"                indices[child] = swapIdx;\n"
"                \n"
"                parent = child;\n"
"                child = 2 * parent + 1;\n"
"            }\n"
"        }\n"
"    }\n"
"}\n"
"\n"
"// ============================================================================\n"
"// FP16 (half-precision) Kernels - Memory bandwidth optimization\n"
"// ============================================================================\n"
"\n"
"// FP16 L2 distance with half storage and float32 accumulation\n"
"// Uses half4 vector loads for 4-way parallelism (Metal only supports up to half4)\n"
"kernel void compute_l2_distances_fp16(\n"
"    device const half* query [[buffer(0)]],\n"
"    device const half* vectors [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint offset = gid * dim;\n"
"    \n"
"    // Use half4 vector loads for 4-way parallelism with half -> float conversion\n"
"    float sum = 0.0f;\n"
"    uint vectorWidth = dim / 4;\n"
"    uint remainder = dim % 4;\n"
"    \n"
"    // Process 4 elements at a time using half4 vector loads\n"
"    for (uint i = 0; i < vectorWidth; i++) {\n"
"        half4 queryVec = half4(query[i*4], query[i*4+1], query[i*4+2], query[i*4+3]);\n"
"        half4 vecVec = half4(vectors[offset + i*4], vectors[offset + i*4+1],\n"
"                            vectors[offset + i*4+2], vectors[offset + i*4+3]);\n"
"        float4 diff = float4(queryVec) - float4(vecVec);\n"
"        sum += dot(diff, diff);\n"
"    }\n"
"    \n"
"    // Handle remainder\n"
"    for (uint i = 0; i < remainder; i++) {\n"
"        float diff = float(query[vectorWidth * 4 + i]) - float(vectors[offset + vectorWidth * 4 + i]);\n"
"        sum += diff * diff;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sum);\n"
"}\n"
"\n"
"// FP16 cosine similarity with half storage and float32 accumulation\n"
"kernel void compute_cosine_similarity_fp16(\n"
"    device const half* query [[buffer(0)]],\n"
"    device const half* vectors [[buffer(1)]],\n"
"    device float* similarities [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    float dotProd = 0.0f;\n"
"    float queryMag = 0.0f;\n"
"    float vecMag = 0.0f;\n"
"    uint offset = gid * dim;\n"
"    \n"
"    // Process 4 elements at a time using half4\n"
"    uint vectorWidth = dim / 4;\n"
"    uint remainder = dim % 4;\n"
"    \n"
"    for (uint i = 0; i < vectorWidth; i++) {\n"
"        half4 q4 = half4(query[i*4], query[i*4+1], query[i*4+2], query[i*4+3]);\n"
"        half4 v4 = half4(vectors[offset + i*4], vectors[offset + i*4+1],\n"
"                        vectors[offset + i*4+2], vectors[offset + i*4+3]);\n"
"        float4 q = float4(q4);\n"
"        float4 v = float4(v4);\n"
"        dotProd += dot(q, v);\n"
"        queryMag += dot(q, q);\n"
"        vecMag += dot(v, v);\n"
"    }\n"
"    \n"
"    // Handle remainder\n"
"    for (uint i = 0; i < remainder; i++) {\n"
"        float q = float(query[vectorWidth * 4 + i]);\n"
"        float v = float(vectors[offset + vectorWidth * 4 + i]);\n"
"        dotProd += q * v;\n"
"        queryMag += q * q;\n"
"        vecMag += v * v;\n"
"    }\n"
"    \n"
"    float denom = sqrt(queryMag) * sqrt(vecMag);\n"
"    similarities[gid] = (denom > 1e-10f) ? (dotProd / denom) : 0.0f;\n"
"}\n"
"\n"
"// FP16 dot product with half storage and float32 accumulation\n"
"kernel void compute_dot_product_fp16(\n"
"    device const half* query [[buffer(0)]],\n"
"    device const half* vectors [[buffer(1)]],\n"
"    device float* products [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    float sum = 0.0f;\n"
"    uint offset = gid * dim;\n"
"    \n"
"    // Process 4 elements at a time using half4\n"
"    uint vectorWidth = dim / 4;\n"
"    uint remainder = dim % 4;\n"
"    \n"
"    for (uint i = 0; i < vectorWidth; i++) {\n"
"        half4 q4 = half4(query[i*4], query[i*4+1], query[i*4+2], query[i*4+3]);\n"
"        half4 v4 = half4(vectors[offset + i*4], vectors[offset + i*4+1],\n"
"                        vectors[offset + i*4+2], vectors[offset + i*4+3]);\n"
"        sum += dot(float4(q4), float4(v4));\n"
"    }\n"
"    \n"
"    // Handle remainder\n"
"    for (uint i = 0; i < remainder; i++) {\n"
"        sum += float(query[vectorWidth * 4 + i]) * float(vectors[offset + vectorWidth * 4 + i]);\n"
"    }\n"
"    \n"
"    products[gid] = sum;\n"
"}\n"
"\n"
"// ============================================================================\n"
"// Complex64 / Complex128 Kernels - Phasor & signal processing\n"
"// ============================================================================\n"
"\n"
"// complex128 L2 distance: treat real/imag pairs as 2D vectors\n"
"kernel void compute_l2_distances_complex128(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint vecDim = dim / 2;  // logical complex dims\n"
"    uint offset = gid * dim;\n"
"    \n"
"    float sumReal = 0.0f;\n"
"    float sumImag = 0.0f;\n"
"    \n"
"    // Process pairs: [real0, imag0, real1, imag1, ...]\n"
"    for (uint i = 0; i < vecDim; i++) {\n"
"        float2 q2 = float2(query[i*2], query[i*2+1]);\n"
"        float2 v2 = float2(vectors[offset + i*2], vectors[offset + i*2+1]);\n"
"        float2 diff = q2 - v2;\n"
"        sumReal += diff.x * diff.x;\n"
"        sumImag += diff.y * diff.y;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sumReal + sumImag);\n"
"}\n"
"\n"
"// complex128 cosine similarity\n"
"kernel void compute_cosine_similarity_complex128(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* similarities [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint vecDim = dim / 2;\n"
"    uint offset = gid * dim;\n"
"    \n"
"    float dotReal = 0.0f;\n"
"    float dotImag = 0.0f;\n"
"    float qMag = 0.0f;\n"
"    float vMag = 0.0f;\n"
"    \n"
"    for (uint i = 0; i < vecDim; i++) {\n"
"        float qRe = query[i*2];\n"
"        float qIm = query[i*2+1];\n"
"        float vRe = vectors[offset + i*2];\n"
"        float vIm = vectors[offset + i*2+1];\n"
"        dotReal += qRe*vRe + qIm*vIm;\n"
"        dotImag += qRe*vIm - qIm*vRe;\n"
"        qMag += qRe*qRe + qIm*qIm;\n"
"        vMag += vRe*vRe + vIm*vIm;\n"
"    }\n"
"    \n"
"    float dotMod = sqrt(dotReal*dotReal + dotImag*dotImag);\n"
"    float denom = sqrt(qMag) * sqrt(vMag);\n"
"    similarities[gid] = (denom > 1e-10f) ? (dotMod / denom) : 0.0f;\n"
"}\n"
"\n"
"// complex128 dot product = (a_r*b_r - a_i*b_i) + (a_r*b_i + a_i*b_r)i\n"
"kernel void compute_dot_product_complex128(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* realOut [[buffer(2)]],\n"
"    device float* imagOut [[buffer(3)]],\n"
"    constant uint& dim [[buffer(4)]],\n"
"    constant uint& numVectors [[buffer(5)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint vecDim = dim / 2;\n"
"    uint offset = gid * dim;\n"
"    \n"
"    float dotReal = 0.0f;\n"
"    float dotImag = 0.0f;\n"
"    \n"
"    for (uint i = 0; i < vecDim; i++) {\n"
"        float qRe = query[i*2];\n"
"        float qIm = query[i*2+1];\n"
"        float vRe = vectors[offset + i*2];\n"
"        float vIm = vectors[offset + i*2+1];\n"
"        dotReal += qRe*vRe - qIm*vIm;\n"
"        dotImag += qRe*vIm + qIm*vRe;\n"
"    }\n"
"    \n"
"    realOut[gid] = dotReal;\n"
"    imagOut[gid] = dotImag;\n"
"}\n"
"\n"
"// complex64 L2 distance (half storage, float32 accumulation)\n"
"kernel void compute_l2_distances_complex64(\n"
"    device const half* query [[buffer(0)]],\n"
"    device const half* vectors [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint vecDim = dim / 2;\n"
"    uint offset = gid * dim;\n"
"    \n"
"    float sumReal = 0.0f;\n"
"    float sumImag = 0.0f;\n"
"    \n"
"    for (uint i = 0; i < vecDim; i++) {\n"
"        half2 q2 = half2(query[i*2], query[i*2+1]);\n"
"        half2 v2 = half2(vectors[offset + i*2], vectors[offset + i*2+1]);\n"
"        float2 q = float2(q2);\n"
"        float2 v = float2(v2);\n"
"        float2 diff = q - v;\n"
"        sumReal += diff.x * diff.x;\n"
"        sumImag += diff.y * diff.y;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sumReal + sumImag);\n"
"}\n"
"\n"
"// complex64 cosine similarity\n"
"kernel void compute_cosine_similarity_complex64(\n"
"    device const half* query [[buffer(0)]],\n"
"    device const half* vectors [[buffer(1)]],\n"
"    device float* similarities [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint vecDim = dim / 2;\n"
"    uint offset = gid * dim;\n"
"    \n"
"    float dotReal = 0.0f;\n"
"    float dotImag = 0.0f;\n"
"    float qMag = 0.0f;\n"
"    float vMag = 0.0f;\n"
"    \n"
"    for (uint i = 0; i < vecDim; i++) {\n"
"        half2 qh = half2(query[i*2], query[i*2+1]);\n"
"        half2 vh = half2(vectors[offset + i*2], vectors[offset + i*2+1]);\n"
"        float2 q = float2(qh);\n"
"        float2 v = float2(vh);\n"
"        dotReal += q.x*v.x + q.y*v.y;\n"
"        dotImag += q.x*v.y - q.y*v.x;\n"
"        qMag += dot(q,q);\n"
"        vMag += dot(v,v);\n"
"    }\n"
"    \n"
"    float dotMod = sqrt(dotReal*dotReal + dotImag*dotImag);\n"
"    float denom = sqrt(qMag) * sqrt(vMag);\n"
"    similarities[gid] = (denom > 1e-10f) ? (dotMod / denom) : 0.0f;\n"
"}\n"
"\n"
"// complex64 dot product\n"
"kernel void compute_dot_product_complex64(\n"
"    device const half* query [[buffer(0)]],\n"
"    device const half* vectors [[buffer(1)]],\n"
"    device float* realOut [[buffer(2)]],\n"
"    device float* imagOut [[buffer(3)]],\n"
"    constant uint& dim [[buffer(4)]],\n"
"    constant uint& numVectors [[buffer(5)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint vecDim = dim / 2;\n"
"    uint offset = gid * dim;\n"
"    \n"
"    float dotReal = 0.0f;\n"
"    float dotImag = 0.0f;\n"
"    \n"
"    for (uint i = 0; i < vecDim; i++) {\n"
"        half2 qh = half2(query[i*2], query[i*2+1]);\n"
"        half2 vh = half2(vectors[offset + i*2], vectors[offset + i*2+1]);\n"
"        float2 q = float2(qh);\n"
"        float2 v = float2(vh);\n"
"        dotReal += q.x*v.x - q.y*v.y;\n"
"        dotImag += q.x*v.y + q.y*v.x;\n"
"    }\n"
"    \n"
"    realOut[gid] = dotReal;\n"
"    imagOut[gid] = dotImag;\n"
"}\n"
"\n"
"// ============================================================================\n"
"// SIMD/Warp-level Reduction Kernels - Optimized for Apple Silicon\n"
"// ============================================================================\n"
"\n"
"// L2 distance kernel optimized for Apple Silicon (each thread computes one vector)\n"
"kernel void compute_l2_distances_warp(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint offset = gid * dim;\n"
"    \n"
"    // Each thread computes its distance using float4 for vectorization\n"
"    float sum = 0.0f;\n"
"    uint vectorWidth = dim / 4;\n"
"    uint remainder = dim % 4;\n"
"    \n"
"    for (uint i = 0; i < vectorWidth; i++) {\n"
"        float4 queryVec = float4(query[i*4], query[i*4+1], query[i*4+2], query[i*4+3]);\n"
"        float4 vecVec = float4(vectors[offset + i*4], vectors[offset + i*4+1],\n"
"                              vectors[offset + i*4+2], vectors[offset + i*4+3]);\n"
"        float4 diff = queryVec - vecVec;\n"
"        sum += dot(diff, diff);\n"
"    }\n"
"    \n"
"    // Handle remainder\n"
"    for (uint i = 0; i < remainder; i++) {\n"
"        float diff = query[vectorWidth * 4 + i] - vectors[offset + vectorWidth * 4 + i];\n"
"        sum += diff * diff;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sum);\n"
"}\n"
"\n"
"// Fused distance computation with local reduction\n"
"kernel void compute_l2_and_topk_warp(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    device int* indices [[buffer(3)]],\n"
"    constant uint& dim [[buffer(4)]],\n"
"    constant uint& numVectors [[buffer(5)]],\n"
"    constant uint& k [[buffer(6)]],\n"
"    uint gid [[thread_position_in_grid]],\n"
"    uint tid [[thread_index_in_threadgroup]],\n"
"    uint tg_size [[threads_per_threadgroup]])\n"
"{\n"
"    // Each thread computes one distance\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    uint offset = gid * dim;\n"
"    \n"
"    // Compute L2 distance using float4\n"
"    float sum = 0.0f;\n"
"    uint vectorWidth = dim / 4;\n"
"    uint remainder = dim % 4;\n"
"    \n"
"    for (uint i = 0; i < vectorWidth; i++) {\n"
"        float4 queryVec = float4(query[i*4], query[i*4+1], query[i*4+2], query[i*4+3]);\n"
"        float4 vecVec = float4(vectors[offset + i*4], vectors[offset + i*4+1],\n"
"                              vectors[offset + i*4+2], vectors[offset + i*4+3]);\n"
"        float4 diff = queryVec - vecVec;\n"
"        sum += dot(diff, diff);\n"
"    }\n"
"    \n"
"    // Handle remainder\n"
"    for (uint i = 0; i < remainder; i++) {\n"
"        float diff = query[vectorWidth * 4 + i] - vectors[offset + vectorWidth * 4 + i];\n"
"        sum += diff * diff;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sum);\n"
"    indices[gid] = int(gid);\n"
"}\n"
"\n"
"// Batched distance computation - multiple queries at once\n"
"kernel void compute_l2_distances_batch(\n"
"    device const float* queries [[buffer(0)]],\n"
"    device const float* vectors [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& numVectors [[buffer(4)]],\n"
"    constant uint& numQueries [[buffer(5)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    // gid = queryIdx * numVectors + vectorIdx\n"
"    uint queryIdx = gid / numVectors;\n"
"    uint vectorIdx = gid % numVectors;\n"
"    \n"
"    if (queryIdx >= numQueries || vectorIdx >= numVectors) return;\n"
"    \n"
"    uint queryOffset = queryIdx * dim;\n"
"    uint vectorOffset = vectorIdx * dim;\n"
"    \n"
"    float sum = 0.0f;\n"
"    for (uint i = 0; i < dim; i++) {\n"
"        float diff = queries[queryOffset + i] - vectors[vectorOffset + i];\n"
"        sum += diff * diff;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sum);\n"
"}\n"
"\n"
"// Batched top-k for multiple queries\n"
"kernel void find_top_k_batch(\n"
"    device const float* distances [[buffer(0)]],\n"
"    device int* indices [[buffer(1)]],\n"
"    device float* topDistances [[buffer(2)]],\n"
"    constant uint& numVectors [[buffer(3)]],\n"
"    constant uint& numQueries [[buffer(4)]],\n"
"    constant uint& k [[buffer(5)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    // Process one query per threadgroup\n"
"    uint queryIdx = gid;\n"
"    if (queryIdx >= numQueries) return;\n"
"    \n"
"    uint distOffset = queryIdx * numVectors;\n"
"    \n"
"    // Simple selection for each query\n"
"    for (uint i = 0; i < k && i < numVectors; i++) {\n"
"        float minDist = INFINITY;\n"
"        int minIdx = -1;\n"
"        \n"
"        for (uint j = i; j < numVectors; j++) {\n"
"            float d = distances[distOffset + j];\n"
"            if (d < minDist) {\n"
"                minDist = d;\n"
"                minIdx = j;\n"
"            }\n"
"        }\n"
"        \n"
"        topDistances[queryIdx * k + i] = minDist;\n"
"        indices[queryIdx * k + i] = minIdx;\n"
"    }\n"
"}\n"
"\n"
"// TurboQuant distance computation for high-dimensional vectors (768d+)\n"
"kernel void compute_tq_distances(\n"
"    device const float* query [[buffer(0)]],\n"
"    device const uchar* tqData [[buffer(1)]],\n"
"    device float* distances [[buffer(2)]],\n"
"    constant uint& dim [[buffer(3)]],\n"
"    constant uint& pow2 [[buffer(4)]],\n"
"    constant uint& bitsPerAngle [[buffer(5)]],\n"
"    constant uint& numVectors [[buffer(6)]],\n"
"    uint gid [[thread_position_in_grid]])\n"
"{\n"
"    if (gid >= numVectors) return;\n"
"    \n"
"    // Format: [Radius (4B)][Packed Angles][QJL Bits]\n"
"    uint angleCount = pow2 - 1;\n"
"    uint angleBytes = (angleCount * bitsPerAngle + 7) / 8;\n"
"    uint bitBytes = (pow2 + 7) / 8;\n"
"    uint stride = 4 + angleBytes + bitBytes;\n"
"    \n"
"    device const uchar* data = tqData + (gid * stride);\n"
"    \n"
"    float radius = *(device const float*)data;\n"
"    device const uchar* packedAngles = data + 4;\n"
"    device const uchar* qjlBits = data + 4 + angleBytes;\n"
"    \n"
"    // Iterative Polar Reconstruction (using local registers/stack)\n"
"    // Max supported dimension in this kernel is 1024 (pow2)\n"
"    float recon[1024];\n"
"    \n"
"    recon[0] = radius;\n"
"    uint currentLevelSize = 1;\n"
"    uint angleOffset = angleCount;\n"
"    \n"
"    while (currentLevelSize < pow2) {\n"
"        angleOffset -= currentLevelSize;\n"
"        for (int i = (int)currentLevelSize - 1; i >= 0; i--) {\n"
"            float r = recon[i];\n"
"            \n"
"            uint bitStart = (angleOffset + i) * bitsPerAngle;\n"
"            uint q = 0;\n"
"            for (uint k = 0; k < bitsPerAngle; k++) {\n"
"                uint bitIdx = bitStart + k;\n"
"                if ((packedAngles[bitIdx / 8] >> (bitIdx % 8)) & 1) {\n"
"                    q |= (1 << k);\n"
"                }\n"
"            }\n"
"            float theta = (float(q) / ((1 << bitsPerAngle) - 1)) * 2.0f * 3.14159265f - 3.14159265f;\n"
"            \n"
"            float s, c;\n"
"            s = sincos(theta, c);\n"
"            recon[2*i] = r * c;\n"
"            recon[2*i+1] = r * s;\n"
"        }\n"
"        currentLevelSize *= 2;\n"
"    }\n"
"    \n"
"    // QJL Correction & Distance\n"
"    float sum = 0.0f;\n"
"    float correctionFactor = radius / sqrt((float)pow2) * 0.1f;\n"
"    \n"
"    for (uint i = 0; i < dim; i++) {\n"
"        float val = recon[i];\n"
"        if ((qjlBits[i / 8] >> (i % 8)) & 1) {\n"
"            val += correctionFactor;\n"
"        } else {\n"
"            val -= 0.1f;\n"
"        }\n"
"        \n"
"        float diff = query[i] - val;\n"
"        sum += diff * diff;\n"
"    }\n"
"    \n"
"    distances[gid] = sqrt(sum);\n"
"}\n";

// Distance metric type
typedef enum {
    METRIC_L2 = 0,
    METRIC_COSINE = 1,
    METRIC_DOT = 2
} DistanceMetric;

// MetalIndexOptimized wraps Metal GPU resources with compute shaders
typedef struct {
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
    int vectorCount;
    int dimensions;
    int capacity;
    DistanceMetric metric;
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

        // Create compute pipelines for all distance metrics
        id<MTLFunction> l2DistanceFunc = [library newFunctionWithName:@"compute_l2_distances"];
        id<MTLFunction> cosineFunc = [library newFunctionWithName:@"compute_cosine_similarity"];
        id<MTLFunction> dotFunc = [library newFunctionWithName:@"compute_dot_product"];
        id<MTLFunction> topKFunc = [library newFunctionWithName:@"find_top_k_heap"];

        // FP16 pipelines
        id<MTLFunction> l2Fp16Func = [library newFunctionWithName:@"compute_l2_distances_fp16"];
        id<MTLFunction> cosineFp16Func = [library newFunctionWithName:@"compute_cosine_similarity_fp16"];
        id<MTLFunction> dotFp16Func = [library newFunctionWithName:@"compute_dot_product_fp16"];

        // Complex pipelines
        id<MTLFunction> l2C128Func = [library newFunctionWithName:@"compute_l2_distances_complex128"];
        id<MTLFunction> cosineC128Func = [library newFunctionWithName:@"compute_cosine_similarity_complex128"];
        id<MTLFunction> l2C64Func = [library newFunctionWithName:@"compute_l2_distances_complex64"];
        id<MTLFunction> cosineC64Func = [library newFunctionWithName:@"compute_cosine_similarity_complex64"];
        id<MTLFunction> tqFunc = [library newFunctionWithName:@"compute_tq_distances"];

        id<MTLComputePipelineState> l2Pipeline = nil;
        id<MTLComputePipelineState> cosinePipeline = nil;
        id<MTLComputePipelineState> dotPipeline = nil;
        id<MTLComputePipelineState> topKPipeline = nil;
        id<MTLComputePipelineState> l2Fp16Pipeline = nil;
        id<MTLComputePipelineState> cosineFp16Pipeline = nil;
        id<MTLComputePipelineState> dotFp16Pipeline = nil;
        id<MTLComputePipelineState> l2C128Pipeline = nil;
        id<MTLComputePipelineState> cosineC128Pipeline = nil;
        id<MTLComputePipelineState> tqPipeline = nil;
        id<MTLComputePipelineState> l2C64Pipeline = nil;
        id<MTLComputePipelineState> cosineC64Pipeline = nil;
        id<MTLComputePipelineState> pqPipeline = nil;
        
        id<MTLFunction> pqFunc = [library newFunctionWithName:@"compute_pq_distances"];
        if (pqFunc) {
            pqPipeline = [device newComputePipelineStateWithFunction:pqFunc error:&error];
        }

        if (l2DistanceFunc) {
            l2Pipeline = [device newComputePipelineStateWithFunction:l2DistanceFunc error:&error];
        }
        if (cosineFunc) {
            cosinePipeline = [device newComputePipelineStateWithFunction:cosineFunc error:&error];
        }
        if (dotFunc) {
            dotPipeline = [device newComputePipelineStateWithFunction:dotFunc error:&error];
        }
        if (topKFunc) {
            topKPipeline = [device newComputePipelineStateWithFunction:topKFunc error:&error];
        }
        if (l2Fp16Func) {
            l2Fp16Pipeline = [device newComputePipelineStateWithFunction:l2Fp16Func error:&error];
        }
        if (cosineFp16Func) {
            cosineFp16Pipeline = [device newComputePipelineStateWithFunction:cosineFp16Func error:&error];
        }
        if (dotFp16Func) {
            dotFp16Pipeline = [device newComputePipelineStateWithFunction:dotFp16Func error:&error];
        }
        if (l2C128Func) {
            l2C128Pipeline = [device newComputePipelineStateWithFunction:l2C128Func error:&error];
        }
        if (cosineC128Func) {
            cosineC128Pipeline = [device newComputePipelineStateWithFunction:cosineC128Func error:&error];
        }
        if (l2C64Func) {
            l2C64Pipeline = [device newComputePipelineStateWithFunction:l2C64Func error:&error];
        }
        if (cosineC64Func) {
            cosineC64Pipeline = [device newComputePipelineStateWithFunction:cosineC64Func error:&error];
        }
        if (tqFunc) {
            tqPipeline = [device newComputePipelineStateWithFunction:tqFunc error:&error];
        }

        if (!l2Pipeline || !topKPipeline) {
            NSLog(@"Failed to create compute pipelines: %@", error);
            return NULL;
        }

        MetalIndexOptimized* handle = (MetalIndexOptimized*)malloc(sizeof(MetalIndexOptimized));
        handle->device = (__bridge_retained void*)device;
        handle->commandQueue = (__bridge_retained void*)queue;
        handle->vectorBuffer = NULL;
        handle->idBuffer = NULL;
        handle->distanceComputePipeline = (__bridge_retained void*)l2Pipeline;
        handle->cosinePipeline = (__bridge_retained void*)cosinePipeline;
        handle->dotPipeline = (__bridge_retained void*)dotPipeline;
        handle->topKPipeline = (__bridge_retained void*)topKPipeline;
        handle->l2Fp16Pipeline = (__bridge_retained void*)l2Fp16Pipeline;
        handle->cosineFp16Pipeline = (__bridge_retained void*)cosineFp16Pipeline;
        handle->dotFp16Pipeline = (__bridge_retained void*)dotFp16Pipeline;
        handle->l2C128Pipeline = (__bridge_retained void*)l2C128Pipeline;
        handle->cosineC128Pipeline = (__bridge_retained void*)cosineC128Pipeline;
        handle->l2C64Pipeline = (__bridge_retained void*)l2C64Pipeline;
        handle->cosineC64Pipeline = (__bridge_retained void*)cosineC64Pipeline;
        handle->tqPipeline = (__bridge_retained void*)tqPipeline;
        handle->pqPipeline = (__bridge_retained void*)pqPipeline;
        handle->pqBuffer = NULL;
        handle->vectorCount = 0;
        handle->dimensions = dimensions;
        handle->capacity = 0;
        handle->metric = METRIC_L2;

        return handle;
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
            if (handle->idBuffer) {
                CFRelease(handle->idBuffer);
            }
            if (handle->vectorBuffer) {
                CFRelease(handle->vectorBuffer);
            }
            if (handle->topKPipeline) {
                CFRelease(handle->topKPipeline);
            }
            if (handle->tqPipeline) {
                CFRelease(handle->tqPipeline);
            }
            if (handle->cosinePipeline) {
                CFRelease(handle->cosinePipeline);
            }
            if (handle->dotPipeline) {
                CFRelease(handle->dotPipeline);
            }
            if (handle->distanceComputePipeline) {
                CFRelease(handle->distanceComputePipeline);
            }
            if (handle->l2Fp16Pipeline) {
                CFRelease(handle->l2Fp16Pipeline);
            }
            if (handle->cosineFp16Pipeline) {
                CFRelease(handle->cosineFp16Pipeline);
            }
            if (handle->dotFp16Pipeline) {
                CFRelease(handle->dotFp16Pipeline);
            }
            if (handle->l2C128Pipeline) {
                CFRelease(handle->l2C128Pipeline);
            }
            if (handle->cosineC128Pipeline) {
                CFRelease(handle->cosineC128Pipeline);
            }
            if (handle->l2C64Pipeline) {
                CFRelease(handle->l2C64Pipeline);
            }
            if (handle->cosineC64Pipeline) {
                CFRelease(handle->cosineC64Pipeline);
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

    int metal_search_pq(MetalIndexOptimized* handle, const float* lookupTable, int m, int k, int64_t* resultIDs, float* resultDistances) {
        @autoreleasepool {
            if (!handle->pqBuffer || handle->vectorCount == 0) return -1;
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
}
*/
import "C"
import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/pq"
)

// MetalIndexOptimized implements GPU-accelerated vector search using Metal compute shaders
type MetalIndexOptimized struct {
	handle *C.MetalIndexOptimized
	dim    int
	mu     sync.RWMutex
	closed bool
	pqEncoder *pq.PQEncoder
}

// NewMetalIndexOptimized creates an optimized Metal-based GPU index with compute shaders
func NewMetalIndexOptimized(cfg types.GPUConfig) (types.Index, error) {
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

	// Batch search: currently implemented as sequential calls
	// Future: use compute_l2_distances_batch kernel for true parallelism
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

func (idx *MetalIndexOptimized) DeviceID() int {
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

	ret := C.metal_search_pq(idx.handle, (*C.float)(unsafe.Pointer(&lookupTable[0])), C.int(m), C.int(k), (*C.int64_t)(unsafe.Pointer(&resIDs[0])), (*C.float)(unsafe.Pointer(&resDists[0])))
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

