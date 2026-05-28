#include <metal_stdlib>
using namespace metal;

// ===========================================================================
// Distance Kernels (Standard Float)
// ===========================================================================

struct PageArgBuffer {
    device const float* pages[1024];
};
struct PageArgBufferHalf {
    device const half* pages[1024];
};

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

// Optimized versions with SIMD
kernel void compute_l2_distances(
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

kernel void compute_dot_product(
    device const float* query [[buffer(0)]],
    device const float* vectors [[buffer(1)]],
    device float* products [[buffer(2)]],
    constant uint& dim [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    
    float sum = 0.0f;
    uint offset = gid * dim;
    uint vectorWidth = dim / 4;
    uint remainder = dim % 4;

    for (uint i = 0; i < vectorWidth; i++) {
        float4 q = *(device const float4*)(query + i * 4);
        float4 v = *(device const float4*)(vectors + offset + i * 4);
        sum += dot(q, v);
    }
    
    for (uint i = 0; i < remainder; i++) {
        sum += query[vectorWidth * 4 + i] * vectors[offset + vectorWidth * 4 + i];
    }
    
    products[gid] = sum;
}

// ===========================================================================
// FP16 (half-precision) Kernels
// ===========================================================================

kernel void compute_l2_distances_fp16(
    device const half* query [[buffer(0)]],
    device const half* vectors [[buffer(1)]],
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
        half4 q4 = *(device const half4*)(query + i * 4);
        half4 v4 = *(device const half4*)(vectors + offset + i * 4);
        float4 diff = float4(q4) - float4(v4);
        sum += dot(diff, diff);
    }
    for (uint i = 0; i < remainder; i++) {
        float diff = float(query[vectorWidth * 4 + i]) - float(vectors[offset + vectorWidth * 4 + i]);
        sum += diff * diff;
    }
    distances[gid] = sqrt(sum);
}

kernel void compute_cosine_similarity_fp16(
    device const half* query [[buffer(0)]],
    device const half* vectors [[buffer(1)]],
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
    uint vectorWidth = dim / 4;
    uint remainder = dim % 4;
    for (uint i = 0; i < vectorWidth; i++) {
        half4 q4 = *(device const half4*)(query + i * 4);
        half4 v4 = *(device const half4*)(vectors + offset + i * 4);
        float4 q = float4(q4);
        float4 v = float4(v4);
        dotProd += dot(q, v);
        queryMag += dot(q, q);
        vecMag += dot(v, v);
    }
    for (uint i = 0; i < remainder; i++) {
        float q = float(query[vectorWidth * 4 + i]);
        float v = float(vectors[offset + vectorWidth * 4 + i]);
        dotProd += q * v;
        queryMag += q * q;
        vecMag += v * v;
    }
    float denom = sqrt(queryMag) * sqrt(vecMag);
    similarities[gid] = (denom > 1e-10f) ? (dotProd / denom) : 0.0f;
}

kernel void compute_dot_product_fp16(
    device const half* query [[buffer(0)]],
    device const half* vectors [[buffer(1)]],
    device float* products [[buffer(2)]],
    constant uint& dim [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    float sum = 0.0f;
    uint offset = gid * dim;
    uint vectorWidth = dim / 4;
    uint remainder = dim % 4;
    for (uint i = 0; i < vectorWidth; i++) {
        half4 q4 = *(device const half4*)(query + i * 4);
        half4 v4 = *(device const half4*)(vectors + offset + i * 4);
        sum += dot(float4(q4), float4(v4));
    }
    for (uint i = 0; i < remainder; i++) {
        sum += float(query[vectorWidth * 4 + i]) * float(vectors[offset + vectorWidth * 4 + i]);
    }
    products[gid] = sum;
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

kernel void compute_cosine_similarity_complex128(
    device const float* query [[buffer(0)]],
    device const float* vectors [[buffer(1)]],
    device float* similarities [[buffer(2)]],
    constant uint& dim [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    uint vecDim = dim / 2;
    uint offset = gid * dim;
    float dotReal = 0.0f;
    float dotImag = 0.0f;
    float qMag = 0.0f;
    float vMag = 0.0f;
    for (uint i = 0; i < vecDim; i++) {
        float qRe = query[i*2];
        float qIm = query[i*2+1];
        float vRe = vectors[offset + i*2];
        float vIm = vectors[offset + i*2+1];
        dotReal += qRe*vRe + qIm*vIm;
        dotImag += qRe*vIm - qIm*vRe;
        qMag += qRe*qRe + qIm*qIm;
        vMag += vRe*vRe + vIm*vIm;
    }
    float dotMod = sqrt(dotReal*dotReal + dotImag*dotImag);
    float denom = sqrt(qMag) * sqrt(vMag);
    similarities[gid] = (denom > 1e-10f) ? (dotMod / denom) : 0.0f;
}

kernel void compute_dot_product_complex128(
    device const float* query [[buffer(0)]],
    device const float* vectors [[buffer(1)]],
    device float* realOut [[buffer(2)]],
    device float* imagOut [[buffer(3)]],
    constant uint& dim [[buffer(4)]],
    constant uint& numVectors [[buffer(5)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    uint vecDim = dim / 2;
    uint offset = gid * dim;
    float dotReal = 0.0f;
    float dotImag = 0.0f;
    for (uint i = 0; i < vecDim; i++) {
        float qRe = query[i*2];
        float qIm = query[i*2+1];
        float vRe = vectors[offset + i*2];
        float vIm = vectors[offset + i*2+1];
        dotReal += qRe*vRe - qIm*vIm;
        dotImag += qRe*vIm + qIm*vRe;
    }
    realOut[gid] = dotReal;
    imagOut[gid] = dotImag;
}

kernel void compute_l2_distances_complex64(
    device const half* query [[buffer(0)]],
    device const half* vectors [[buffer(1)]],
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
        half2 q2 = half2(query[i*2], query[i*2+1]);
        half2 v2 = half2(vectors[offset + i*2], vectors[offset + i*2+1]);
        float2 q = float2(q2);
        float2 v = float2(v2);
        float2 diff = q - v;
        sumReal += diff.x * diff.x;
        sumImag += diff.y * diff.y;
    }
    distances[gid] = sqrt(sumReal + sumImag);
}

kernel void compute_cosine_similarity_complex64(
    device const half* query [[buffer(0)]],
    device const half* vectors [[buffer(1)]],
    device float* similarities [[buffer(2)]],
    constant uint& dim [[buffer(3)]],
    constant uint& numVectors [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    uint vecDim = dim / 2;
    uint offset = gid * dim;
    float dotReal = 0.0f;
    float dotImag = 0.0f;
    float qMag = 0.0f;
    float vMag = 0.0f;
    for (uint i = 0; i < vecDim; i++) {
        half2 qh = half2(query[i*2], query[i*2+1]);
        half2 vh = half2(vectors[offset + i*2], vectors[offset + i*2+1]);
        float2 q = float2(qh);
        float2 v = float2(vh);
        dotReal += q.x*v.x + q.y*v.y;
        dotImag += q.x*v.y - q.y*v.x;
        qMag += dot(q,q);
        vMag += dot(v,v);
    }
    float dotMod = sqrt(dotReal*dotReal + dotImag*dotImag);
    float denom = sqrt(qMag) * sqrt(vMag);
    similarities[gid] = (denom > 1e-10f) ? (dotMod / denom) : 0.0f;
}

kernel void compute_dot_product_complex64(
    device const half* query [[buffer(0)]],
    device const half* vectors [[buffer(1)]],
    device float* realOut [[buffer(2)]],
    device float* imagOut [[buffer(3)]],
    constant uint& dim [[buffer(4)]],
    constant uint& numVectors [[buffer(5)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    uint vecDim = dim / 2;
    uint offset = gid * dim;
    float dotReal = 0.0f;
    float dotImag = 0.0f;
    for (uint i = 0; i < vecDim; i++) {
        half2 qh = half2(query[i*2], query[i*2+1]);
        half2 vh = half2(vectors[offset + i*2], vectors[offset + i*2+1]);
        float2 q = float2(qh);
        float2 v = float2(vh);
        dotReal += q.x*v.x - q.y*v.y;
        dotImag += q.x*v.y + q.y*v.x;
    }
    realOut[gid] = dotReal;
    imagOut[gid] = dotImag;
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
    device const float* trigTable [[buffer(7)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    
    // TurboQuant format: [Radius (4B)][Packed Angles (Variable)][QJL Bits (Variable)]
    uint angleCount = pow2 - 1;
    uint angleBytes = (angleCount * bitsPerAngle + 7) / 8;
    uint bitBytes = (pow2 + 7) / 8;
    uint stride = (4 + angleBytes + bitBytes + 3) & ~3;
    
    device const uchar* data = tqData + (gid * stride);
    float radius = *(device const float*)data;
    device const uchar* packedAngles = data + 4;
    device const uchar* qjlBits = data + 4 + angleBytes;
    
    float invMaxVal = 1.0f / ((1 << bitsPerAngle) - 1);
    float correctionFactor = radius / sqrt((float)pow2) * 0.1f;
    
    // Use a small local array for folding. Max dimension supported in one pass is 256.
    float work[256];
    if (pow2 > 256) {
        distances[gid] = 1e38f;
        return;
    }
    
    float normSq = 0.0f;
    for (uint i = 0; i < pow2; i++) {
        float q_i = (i < dim) ? query[i] : 0.0f;
        float c_i = ((qjlBits[i / 8] >> (i % 8)) & 1) ? correctionFactor : -0.1f;
        float x_prime = q_i - c_i;
        work[i] = x_prime;
        normSq += x_prime * x_prime;
    }
    
    // Folding bottom-up: converts query-space to polar-space coefficients
    uint currentLevelSize = pow2;
    uint angleOffset = 0;
    
    while (currentLevelSize > 1) {
        uint nextLevelSize = currentLevelSize / 2;
        for (uint i = 0; i < nextLevelSize; i++) {
            uint bitStart = (angleOffset + i) * bitsPerAngle;
            uint q = 0;
            for (uint k = 0; k < bitsPerAngle; k++) {
                uint bitIdx = bitStart + k;
                if ((packedAngles[bitIdx / 8] >> (bitIdx % 8)) & 1) {
                    q |= (1 << k);
                }
            }
            
            // Look up sin/cos from table
            uint q_mapped = (q * 255) / ((1 << bitsPerAngle) - 1);
            float c = trigTable[2 * q_mapped];
            float s = trigTable[2 * q_mapped + 1];
            
            // Reverse polar step: fold two components into one
            work[i] = work[2*i] * c + work[2*i+1] * s;
        }
        angleOffset += nextLevelSize;
        currentLevelSize = nextLevelSize;
    }
    
    // Squared L2 Distance = ||query - corrections||^2 + radius^2 - 2 * radius * folded_query[0]
    float distSq = normSq + radius * radius - 2.0f * radius * work[0];
    distances[gid] = sqrt(max(0.0f, distSq));
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
    results[idx] = sqrt(sum);
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

kernel void sum_centroids(
    device const float* vectors [[buffer(0)]],
    device const uint* assignments [[buffer(1)]],
    device atomic_float* centroids [[buffer(2)]],
    device atomic_uint* counts [[buffer(3)]],
    constant uint& dim [[buffer(4)]],
    constant uint& numVectors [[buffer(5)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numVectors) return;
    uint clusterID = assignments[gid];
    uint vecOffset = gid * dim;
    uint centOffset = clusterID * dim;
    
    atomic_fetch_add_explicit(&counts[clusterID], 1, memory_order_relaxed);
    
    for (uint i = 0; i < dim; i++) {
        device atomic_float& target = centroids[centOffset + i];
        float val = vectors[vecOffset + i];
        
        float expected = atomic_load_explicit(&target, memory_order_relaxed);
        while (!atomic_compare_exchange_weak_explicit(&target, &expected, expected + val, memory_order_relaxed, memory_order_relaxed));
    }
}

kernel void finalize_centroids(
    device float* centroids [[buffer(0)]],
    device const uint* counts [[buffer(1)]],
    constant uint& dim [[buffer(2)]],
    constant uint& numCentroids [[buffer(3)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= numCentroids) return;
    uint count = counts[gid];
    if (count == 0) return;
    
    float invCount = 1.0f / (float)count;
    uint offset = gid * dim;
    for (uint i = 0; i < dim; i++) {
        centroids[offset + i] *= invCount;
    }
}

// ===========================================================================
// Top-K & Selection Kernels
// ===========================================================================

kernel void find_top_k_heap(
    device const float* distances [[buffer(0)]],
    device int* indices [[buffer(1)]],
    device float* topDistances [[buffer(2)]],
    constant uint& numVectors [[buffer(3)]],
    constant uint& k [[buffer(4)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid == 0) {
        uint heapSize = min(k, numVectors);
        for (uint i = 0; i < heapSize; i++) {
            topDistances[i] = INFINITY;
            indices[i] = -1;
        }
        for (uint i = 0; i < numVectors; i++) {
            float currentDist = distances[i];
            if (currentDist < topDistances[0]) {
                float parentDist = currentDist;
                int parentIdx = i;
                topDistances[0] = parentDist;
                indices[0] = parentIdx;
                uint parent = 0;
                while (true) {
                    uint child = 2 * parent + 1;
                    if (child >= heapSize) break;
                    if (child + 1 < heapSize && topDistances[child + 1] > topDistances[child]) {
                        child++;
                    }
                    if (parentDist >= topDistances[child]) break;
                    topDistances[parent] = topDistances[child];
                    indices[parent] = indices[child];
                    parent = child;
                }
                topDistances[parent] = parentDist;
                indices[parent] = parentIdx;
            }
        }
        for (uint i = heapSize - 1; i > 0; i--) {
            float tempDist = topDistances[0];
            int tempIdx = indices[0];
            topDistances[0] = topDistances[i];
            indices[0] = indices[i];
            topDistances[i] = tempDist;
            indices[i] = tempIdx;
            uint parent = 0;
            uint child = 1;
            while (child < i) {
                if (child + 1 < i && topDistances[child + 1] > topDistances[child]) {
                    child++;
                }
                if (topDistances[parent] >= topDistances[child]) break;
                float swapDist = topDistances[parent];
                int swapIdx = indices[parent];
                topDistances[parent] = topDistances[child];
                indices[parent] = indices[child];
                topDistances[child] = swapDist;
                indices[child] = swapIdx;
                parent = child;
                child = 2 * parent + 1;
            }
        }
    }
}
// ===========================================================================
// HNSW & Graph Maintenance Kernels
// ===========================================================================

kernel void hnsw_prune_neighbors(
    device const uint* candidateIds [[buffer(0)]],
    device const float* candidateDists [[buffer(1)]],
    device uint* selectedIds [[buffer(2)]],
    device uint* selectedCount [[buffer(3)]],
    device const float* allVectors [[buffer(4)]],
    constant uint& maxNeighbors [[buffer(5)]],
    constant uint& numCandidates [[buffer(6)]],
    constant uint& dim [[buffer(7)]],
    constant bool& extendedHeuristic [[buffer(8)]],
    uint tid [[thread_index_in_threadgroup]],
    uint gid [[threadgroup_position_in_grid]]
) {
    // One threadgroup per pruning task. Currently we assume one task at a time for simplicity.
    if (gid > 0) return;

    threadgroup uint sharedSelectedIds[256];
    threadgroup uint sharedCount;
    threadgroup bool sharedDiverse;

    if (tid == 0) sharedCount = 0;
    threadgroup_barrier(mem_flags::mem_threadgroup);

    for (uint i = 0; i < numCandidates; i++) {
        if (sharedCount >= maxNeighbors) break;

        uint currId = candidateIds[i];
        float currDist = candidateDists[i];

        if (tid == 0) sharedDiverse = true;
        threadgroup_barrier(mem_flags::mem_threadgroup);

        for (uint j = 0; j < sharedCount; j++) {
            uint selId = sharedSelectedIds[j];
            
            float partialDist = 0.0f;
            uint off1 = currId * dim;
            uint off2 = selId * dim;
            // Parallel distance computation across threadgroup
            for (uint k = tid; k < dim; k += 32) {
                float d = allVectors[off1 + k] - allVectors[off2 + k];
                partialDist += d * d;
            }
            
            // Warp-level reduction
            float distSum = simd_sum(partialDist);
            
            if (tid == 0) {
                if (sqrt(distSum) < currDist) {
                    sharedDiverse = false;
                }
            }
            threadgroup_barrier(mem_flags::mem_threadgroup);
            if (!sharedDiverse) break;
        }

        if (tid == 0 && sharedDiverse) {
            sharedSelectedIds[sharedCount++] = currId;
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }

    if (tid == 0) {
        *selectedCount = sharedCount;
        for (uint i = 0; i < sharedCount; i++) {
            selectedIds[i] = sharedSelectedIds[i];
        }
    }
}

kernel void hnsw_greedy_search(
    device const float* query [[buffer(0)]],
    device const float* vectors [[buffer(1)]],
    device const uint* graphOffsets [[buffer(2)]],
    device const uint* graphNeighbors [[buffer(3)]],
    device uint* entryPoint [[buffer(4)]],
    device float* entryDist [[buffer(5)]],
    constant uint& dim [[buffer(6)]],
    constant uint& numNodes [[buffer(7)]],
    uint gid [[thread_position_in_grid]],
    uint tid [[thread_index_in_threadgroup]]
) {
    if (gid >= 1) return;

    threadgroup float scratchDists[32];
    threadgroup uint scratchIds[32];
    threadgroup bool sharedImproved;

    uint currId = *entryPoint;
    float currDist = *entryDist;
    bool improved = true;

    while (improved) {
        if (tid == 0) sharedImproved = false;
        threadgroup_barrier(mem_flags::mem_threadgroup);

        uint start = graphOffsets[currId];
        uint end = graphOffsets[currId + 1];
        uint numNeighbors = end - start;

        uint bestId = currId;
        float bestDist = currDist;

        for (uint i = tid; i < numNeighbors; i += 32) {
            uint neighborId = graphNeighbors[start + i];
            float distSq = 0.0f;
            uint off1 = neighborId * dim;
            for (uint k = 0; k < dim; k++) {
                float d = query[k] - vectors[off1 + k];
                distSq += d * d;
            }
            float dist = sqrt(distSq);

            if (dist < bestDist) {
                bestDist = dist;
                bestId = neighborId;
            }
        }

        scratchDists[tid] = bestDist;
        scratchIds[tid] = bestId;
        threadgroup_barrier(mem_flags::mem_threadgroup);

        if (tid == 0) {
            for (uint i = 1; i < 32; i++) {
                if (scratchDists[i] < bestDist) {
                    bestDist = scratchDists[i];
                    bestId = scratchIds[i];
                }
            }
            if (bestDist < currDist) {
                currDist = bestDist;
                currId = bestId;
                sharedImproved = true;
            }
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        improved = sharedImproved;
    }

    if (tid == 0) {
        *entryPoint = currId;
        *entryDist = currDist;
    }
}

kernel void hnsw_greedy_search_tq(
    device const float* query [[buffer(0)]],
    device const uchar* tqData [[buffer(1)]],
    device const uint* graphOffsets [[buffer(2)]],
    device const uint* graphNeighbors [[buffer(3)]],
    device uint* entryPoint [[buffer(4)]],
    device float* entryDist [[buffer(5)]],
    constant uint& dim [[buffer(6)]],
    constant uint& pow2 [[buffer(7)]],
    constant uint& bitsPerAngle [[buffer(8)]],
    device const float* trigTable [[buffer(9)]],
    uint gid [[thread_position_in_grid]],
    uint tid [[thread_index_in_threadgroup]]
) {
    if (gid >= 1) return;

    threadgroup float scratchDists[32];
    threadgroup uint scratchIds[32];
    threadgroup bool sharedImproved;
    
    threadgroup_barrier(mem_flags::mem_threadgroup);

    uint currId = *entryPoint;
    float currDist = *entryDist;
    bool improved = true;

    // Stride for TQ data
    uint angleCount = pow2 - 1;
    uint angleBytes = (angleCount * bitsPerAngle + 7) / 8;
    uint bitBytes = (pow2 + 7) / 8;
    uint stride = (4 + angleBytes + bitBytes + 3) & ~3;
    float invMaxVal = 1.0f / ((1 << bitsPerAngle) - 1);

    while (improved) {
        if (tid == 0) {
            for (uint i = 0; i < 32; i++) {
                scratchDists[i] = 1e38f;
                scratchIds[i] = 0xFFFFFFFF;
            }
            sharedImproved = false;
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);

        uint start = graphOffsets[currId];
        uint end = graphOffsets[currId + 1];
        uint numNeighbors = end - start;

        uint bestId = currId;
        float bestDist = currDist;

        for (uint i = tid; i < numNeighbors; i += 32) {
            uint neighborId = graphNeighbors[start + i];
            
            // Fused TQ Distance logic
            device const uchar* data = tqData + (neighborId * stride);
            float radius = *(device const float*)data;
            device const uchar* packedAngles = data + 4;
            device const uchar* qjlBits = data + 4 + angleBytes;
            
            float correctionFactor = radius / sqrt((float)pow2) * 0.1f;
            float normSq = 0.0f;
            float work[256]; // Note: stack usage per thread. Limit to 256.
            if (pow2 > 256) return; // Safety

            for (uint k = 0; k < pow2; k++) {
                float q_k = (k < dim) ? query[k] : 0.0f;
                float c_k = ((qjlBits[k / 8] >> (k % 8)) & 1) ? correctionFactor : -0.1f;
                float x_prime = q_k - c_k;
                work[k] = x_prime;
                normSq += x_prime * x_prime;
            }

            uint currentLevelSize = pow2;
            uint angleOffset = 0;
            while (currentLevelSize > 1) {
                uint nextLevelSize = currentLevelSize / 2;
                for (uint k = 0; k < nextLevelSize; k++) {
                    uint bitStart = (angleOffset + k) * bitsPerAngle;
                    uint q = 0;
                    for (uint b = 0; b < bitsPerAngle; b++) {
                        uint bitIdx = bitStart + b;
                        if ((packedAngles[bitIdx / 8] >> (bitIdx % 8)) & 1) q |= (1 << b);
                    }
                    
                    uint q_mapped = (q * 255) / ((1 << bitsPerAngle) - 1);
                    float c = trigTable[2 * q_mapped];
                    float s = trigTable[2 * q_mapped + 1];
                    work[k] = work[2*k] * c + work[2*k+1] * s;
                }
                angleOffset += nextLevelSize;
                currentLevelSize = nextLevelSize;
            }

            float distSq = normSq + radius * radius - 2.0f * radius * work[0];
            float dist = sqrt(max(0.0f, distSq));

            if (dist < bestDist) {
                bestDist = dist;
                bestId = neighborId;
            }
        }

        scratchDists[tid] = bestDist;
        scratchIds[tid] = bestId;
        threadgroup_barrier(mem_flags::mem_threadgroup);

        if (tid == 0) {
            for (uint i = 1; i < 32; i++) {
                if (scratchDists[i] < bestDist) {
                    bestDist = scratchDists[i];
                    bestId = scratchIds[i];
                }
            }
            if (bestDist < currDist) {
                currDist = bestDist;
                currId = bestId;
                sharedImproved = true;
            }
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        improved = sharedImproved;
    }

    if (tid == 0) {
        *entryPoint = currId;
        *entryDist = currDist;
    }
}
