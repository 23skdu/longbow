#include <metal_stdlib>
using namespace metal;

// ===========================================================================
// Distance Kernels (Standard Float)
// ===========================================================================

kernel void vector_distance_l2(
    const device float* query [[buffer(0)]],
    const device float* vectors [[buffer(1)]],
    device float* results [[buffer(2)]],
    uint vectorIdx [[thread_position_in_grid]],
    constant uint& dims [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]]
) {
    if (vectorIdx >= numVectors) return;
    
    float sum = 0.0f;
    uint offset = vectorIdx * dims;
    for (uint i = 0; i < dims; i++) {
        float diff = query[i] - vectors[offset + i];
        sum += diff * diff;
    }
    results[vectorIdx] = sqrt(sum);
}

kernel void vector_distance_ip(
    const device float* query [[buffer(0)]],
    const device float* vectors [[buffer(1)]],
    device float* results [[buffer(2)]],
    uint vectorIdx [[thread_position_in_grid]],
    constant uint& dims [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]]
) {
    if (vectorIdx >= numVectors) return;
    
    float dotSum = 0.0f;
    uint offset = vectorIdx * dims;
    for (uint i = 0; i < dims; i++) {
        dotSum += query[i] * vectors[offset + i];
    }
    results[vectorIdx] = dotSum;
}

kernel void compute_cosine_similarity(
    device const float* query [[buffer(0)]],
    device const float* vectors [[buffer(1)]],
    device float* similarities [[buffer(2)]],
    constant uint& dim [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    
    float dotProd = 0.0f;
    float queryMag = 0.0f;
    float vecMag = 0.0f;
    uint offset = gid * dim;
    
    for (uint i = 0; i < dim; i++) {
        float q = query[i];
        float v = vectors[offset + i];
        dotProd += q * v;
        queryMag += q * q;
        vecMag += v * v;
    }
    
    float denom = sqrt(queryMag) * sqrt(vecMag);
    similarities[gid] = (denom > 1e-10f) ? (dotProd / denom) : 0.0f;
}

// ===========================================================================
// Optimized Distance Kernels (SIMD/Unrolled)
// ===========================================================================

kernel void compute_l2_distances_optimized(
    device const float* query [[buffer(0)]],
    device const float* vectors [[buffer(1)]],
    device float* distances [[buffer(2)]],
    constant uint& dim [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    
    uint offset = gid * dim;
    float sum = 0.0f;
    uint vectorWidth = dim / 4;
    uint remainder = dim % 4;
    
    for (uint i = 0; i < vectorWidth; i++) {
        float4 q = *(device const float4*)(query + i * 4);
        float4 v = *(device const float4*)(vectors + offset + i * 4);
        float4 diff = q - v;
        sum += dot(diff, diff);
    }
    
    for (uint i = 0; i < remainder; i++) {
        float diff = query[vectorWidth * 4 + i] - vectors[offset + vectorWidth * 4 + i];
        sum += diff * diff;
    }
    
    distances[gid] = sqrt(sum);
}

// ===========================================================================
// Complex Type Kernels
// ===========================================================================

kernel void compute_l2_distances_complex128(
    device const float* query [[buffer(0)]],
    device const float* vectors [[buffer(1)]],
    device float* distances [[buffer(2)]],
    constant uint& dim [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    uint vecDim = dim / 2;
    uint offset = gid * dim;
    float sumReal = 0.0f;
    float sumImag = 0.0f;
    for (uint i = 0; i < vecDim; i++) {
        float2 q2 = float2(query[i*2], query[i*2+1]);
        float2 v2 = float2(vectors[offset + i*2], vectors[offset + i*2+1]);
        float2 diff = q2 - v2;
        sumReal += diff.x * diff.x;
        sumImag += diff.y * diff.y;
    }
    distances[gid] = sqrt(sumReal + sumImag);
}

// ===========================================================================
// Quantization & Packing Kernels
// ===========================================================================

kernel void compute_pq_distances(
    device const float* lookupTable [[buffer(0)]],
    device const uchar* codes [[buffer(1)]],
    device float* distances [[buffer(2)]],
    constant uint& m [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    float sum = 0.0f;
    uint offset = gid * m;
    for (uint i = 0; i < m; i++) {
        sum += lookupTable[i * 256 + codes[offset + i]];
    }
    distances[gid] = sum;
}

kernel void quantize_sq8(
    const device float* vectors [[buffer(0)]],
    const device float* mins [[buffer(1)]],
    const device float* maxs [[buffer(2)]],
    device uchar* results [[buffer(3)]],
    uint elementIdx [[thread_position_in_grid]],
    constant uint& dims [[buffer(4)]]
) {
    uint dimIdx = elementIdx % dims;
    float val = vectors[elementIdx];
    float min = mins[dimIdx];
    float max = maxs[dimIdx];
    float scaled = (val - min) / (max - min) * 255.0f;
    results[elementIdx] = (uchar)clamp(scaled, 0.0f, 255.0f);
}

// ===========================================================================
// TurboQuant Kernels
// ===========================================================================

kernel void compute_tq_distances(
    device const float* query [[buffer(0)]],
    device const uchar* tqData [[buffer(1)]],
    device float* distances [[buffer(2)]],
    constant uint& dim [[buffer(3)]],
    constant uint& pow2 [[buffer(4)]],
    constant uint& bitsPerAngle [[buffer(5)]],
    constant uint& numVectors [[buffer(6)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    uint angleCount = pow2 - 1;
    uint angleBytes = (angleCount * bitsPerAngle + 7) / 8;
    uint bitBytes = (pow2 + 7) / 8;
    uint stride = 4 + angleBytes + bitBytes;
    device const uchar* data = tqData + (gid * stride);
    float radius = *(device const float*)data;
    // ... Simplified for now, will implement full NEON/Metal unrolling in Step 3
    distances[gid] = radius; // Placeholder
}

// ===========================================================================
// Graph & Specialized Kernels
// ===========================================================================

kernel void graph_rag_fused(
    const device uint32_t* frontier [[buffer(0)]],
    const device uint32_t* offsets [[buffer(1)]],
    const device uint32_t* neighbors [[buffer(2)]],
    const device float* weights [[buffer(3)]],
    const device float* currentActivations [[buffer(4)]],
    device atomic_float* nextActivations [[buffer(5)]],
    device uint32_t* nextFrontier [[buffer(6)]],
    device atomic_uint* nextFrontierSize [[buffer(7)]],
    device atomic_uint* visited [[buffer(8)]],
    uint idx [[thread_position_in_grid]],
    constant uint& frontierSize [[buffer(9)]],
    constant float& alpha [[buffer(10)]]
) {
    if (idx >= frontierSize) return;
    uint32_t nodeID = frontier[idx];
    float parentScore = currentActivations[nodeID];
    uint32_t start = offsets[nodeID];
    uint32_t end = offsets[nodeID + 1];
    for (uint32_t i = start; i < end; i++) {
        uint32_t neighborID = neighbors[i];
        float edgeWeight = (weights != nullptr) ? weights[i] : 1.0f;
        float scoreToPass = parentScore * alpha * edgeWeight;
        device atomic_float& target = nextActivations[neighborID];
        float expected = atomic_load_explicit(&target, memory_order_relaxed);
        while (!atomic_compare_exchange_weak_explicit(&target, &expected, expected + scoreToPass, memory_order_relaxed, memory_order_relaxed));
        uint wordIdx = neighborID / 32;
        uint bitMask = 1 << (neighborID % 32);
        uint oldVisited = atomic_fetch_or_explicit(&visited[wordIdx], bitMask, memory_order_relaxed);
        if (!(oldVisited & bitMask)) {
            uint pos = atomic_fetch_add_explicit(nextFrontierSize, 1, memory_order_relaxed);
            nextFrontier[pos] = neighborID;
        }
    }
}

kernel void haversine_batch(
    const device float* center [[buffer(0)]],
    const device float* points [[buffer(1)]],
    device float* results [[buffer(2)]],
    constant float& earthRadius [[buffer(3)]],
    constant uint& numPoints [[buffer(4)]],
    uint idx [[thread_position_in_grid]]
) {
    if (idx >= numPoints) return;
    float lat1 = center[0] * M_PI_F / 180.0f;
    float lon1 = center[1] * M_PI_F / 180.0f;
    float lat2 = points[idx * 2] * M_PI_F / 180.0f;
    float lon2 = points[idx * 2 + 1] * M_PI_F / 180.0f;
    float dLat = lat2 - lat1;
    float dLon = lon2 - lon1;
    float a = sin(dLat / 2.0f) * sin(dLat / 2.0f) + cos(lat1) * cos(lat2) * sin(dLon / 2.0f) * sin(dLon / 2.0f);
    float c = 2.0f * atan2(sqrt(a), sqrt(1.0f - a));
    results[idx] = earthRadius * c;
}

kernel void norm_batch_f32(
    const device float* vectors [[buffer(0)]],
    device float* results [[buffer(1)]],
    constant uint& dims [[buffer(2)]],
    constant uint& numVectors [[buffer(3)]],
    uint idx [[thread_position_in_grid]]
) {
    if (idx >= numVectors) return;
    float sum = 0.0f;
    uint base = idx * dims;
    for (uint i = 0; i < dims; i++) {
        float v = vectors[base + i];
        sum += v * v;
    }
    results[idx] = sum;
}

kernel void sigmoid_f32(
    device const float* src [[buffer(0)]],
    device float* dst [[buffer(1)]],
    constant uint& n [[buffer(2)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= n) return;
    dst[gid] = 1.0f / (1.0f + exp(-src[gid]));
}

kernel void assign_to_clusters(
    device const float* vectors [[buffer(0)]],
    device const float* centroids [[buffer(1)]],
    device uint* assignments [[buffer(2)]],
    constant uint& dim [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]],
    constant uint& numCentroids [[buffer(5)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    float minDist = 1e38f;
    uint bestCent = 0;
    uint vecOffset = gid * dim;
    for (uint c = 0; c < numCentroids; c++) {
        float dist = 0.0f;
        uint centOffset = c * dim;
        for (uint i = 0; i < dim; i++) {
            float diff = vectors[vecOffset + i] - centroids[centOffset + i];
            dist += diff * diff;
        }
        if (dist < minDist) {
            minDist = dist;
            bestCent = c;
        }
    }
    assignments[gid] = bestCent;
}
