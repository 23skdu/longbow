#include <metal_stdlib>
using namespace metal;

// Graph BFS Expansion Kernel
kernel void graph_bfs_expand(
    const device uint32_t* frontier [[buffer(0)]],
    const device uint32_t* offsets [[buffer(1)]],
    const device uint32_t* neighbors [[buffer(2)]],
    device atomic_uint* visited [[buffer(3)]],
    device uint32_t* nextFrontier [[buffer(4)]],
    device atomic_uint* nextFrontierSize [[buffer(5)]],
    uint idx [[thread_position_in_grid]],
    uint frontierSize [[constant(0)]]
) {
    if (idx >= frontierSize) return;

    uint32_t nodeID = frontier[idx];
    uint32_t start = offsets[nodeID];
    uint32_t end = offsets[nodeID + 1];

    for (uint32_t i = start; i < end; i++) {
        uint32_t neighborID = neighbors[i];
        
        // Atomic bitset check and set
        uint wordIdx = neighborID / 32;
        uint bitMask = 1 << (neighborID % 32);
        
        uint old = atomic_fetch_or_explicit(&visited[wordIdx], bitMask, memory_order_relaxed);
        
        if (!(old & bitMask)) {
            // Newly discovered node
            uint pos = atomic_fetch_add_explicit(nextFrontierSize, 1, memory_order_relaxed);
            nextFrontier[pos] = neighborID;
        }
    }
}

// Graph Activation Propagation Kernel
kernel void graph_activation_propagate(
    const device float* activations [[buffer(0)]],
    device atomic_float* newActivations [[buffer(1)]],
    const device uint32_t* frontier [[buffer(2)]],
    const device uint32_t* offsets [[buffer(3)]],
    const device uint32_t* neighbors [[buffer(4)]],
    const device float* weights [[buffer(5)]],
    uint idx [[thread_position_in_grid]],
    uint frontierSize [[constant(0)]],
    float alpha [[constant(1)]]
) {
    if (idx >= frontierSize) return;

    uint32_t nodeID = frontier[idx];
    float parentScore = activations[nodeID];
    uint32_t start = offsets[nodeID];
    uint32_t end = offsets[nodeID + 1];

    for (uint32_t i = start; i < end; i++) {
        uint32_t neighborID = neighbors[i];
        float edgeWeight = (weights != nullptr) ? weights[i] : 1.0f;
        float scoreToPass = parentScore * alpha * edgeWeight;
        
        // Note: atomic_float requires Metal 3.0 or specialized implementation
        // For compatibility, we'd use a loop with atomic_compare_exchange
        // but here we assume modern Metal support or will implement fallback.
        
        device atomic_float& target = newActivations[neighborID];
        float expected = atomic_load_explicit(&target, memory_order_relaxed);
        while (!atomic_compare_exchange_weak_explicit(&target, &expected, expected + scoreToPass, memory_order_relaxed, memory_order_relaxed));
    }
}

// Fused GraphRAG Kernel: Combines traversal and activation propagation
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
    uint frontierSize [[constant(0)]],
    float alpha [[constant(1)]]
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
        
        // 1. Update Activation (Atomic)
        device atomic_float& target = nextActivations[neighborID];
        float expected = atomic_load_explicit(&target, memory_order_relaxed);
        while (!atomic_compare_exchange_weak_explicit(&target, &expected, expected + scoreToPass, memory_order_relaxed, memory_order_relaxed));

        // 2. Traversal: Add to next frontier if not visited
        uint wordIdx = neighborID / 32;
        uint bitMask = 1 << (neighborID % 32);
        uint oldVisited = atomic_fetch_or_explicit(&visited[wordIdx], bitMask, memory_order_relaxed);
        
        if (!(oldVisited & bitMask)) {
            uint pos = atomic_fetch_add_explicit(nextFrontierSize, 1, memory_order_relaxed);
            nextFrontier[pos] = neighborID;
        }
    }
}

// L2 Distance Kernel (Vector vs Batch)
kernel void vector_distance_l2(
    const device float* query [[buffer(0)]],
    const device float* vectors [[buffer(1)]],
    device float* results [[buffer(2)]],
    uint vectorIdx [[thread_position_in_grid]],
    uint dims [[constant(0)]],
    uint numVectors [[constant(1)]]
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

// Inner Product Kernel
kernel void vector_distance_ip(
    const device float* query [[buffer(0)]],
    const device float* vectors [[buffer(1)]],
    device float* results [[buffer(2)]],
    uint vectorIdx [[thread_position_in_grid]],
    uint dims [[constant(0)]],
    uint numVectors [[constant(1)]]
) {
    if (vectorIdx >= numVectors) return;
    
    float dot = 0.0f;
    uint offset = vectorIdx * dims;
    for (uint i = 0; i < dims; i++) {
        dot += query[i] * vectors[offset + i];
    }
    results[vectorIdx] = dot;
}

// Batch SQ8 Encoding Kernel
kernel void quantize_sq8(
    const device float* vectors [[buffer(0)]],
    const device float* mins [[buffer(1)]],
    const device float* maxs [[buffer(2)]],
    device uchar* results [[buffer(3)]],
    uint elementIdx [[thread_position_in_grid]],
    uint dims [[constant(0)]]
) {
    uint vectorIdx = elementIdx / dims;
    uint dimIdx = elementIdx % dims;
    
    float val = vectors[elementIdx];
    float min = mins[dimIdx];
    float max = maxs[dimIdx];
    
    float scaled = (val - min) / (max - min) * 255.0f;
    results[elementIdx] = (uchar)clamp(scaled, 0.0f, 255.0f);
}

// Euclidean Distance Kernel for Large Dimensions
kernel void euclidean_distance_f32_large(
    const device float* query [[buffer(0)]],
    const device float* vectors [[buffer(1)]],
    device float* results [[buffer(2)]],
    uint vectorIdx [[thread_position_in_grid]],
    constant uint& dims [[buffer(3)]]
) {
    float distSq = 0.0f;
    uint base = vectorIdx * dims;
    
    // Process in blocks of 4 for better SIMD utilization on GPU
    uint i = 0;
    for (; i + 3 < dims; i += 4) {
        float4 q = *(const device float4*)(query + i);
        float4 v = *(const device float4*)(vectors + base + i);
        float4 diff = q - v;
        distSq += dot(diff, diff);
    }
    
    // Tail loop
    for (; i < dims; i++) {
        float diff = query[i] - vectors[base + i];
        distSq += diff * diff;
    }
    
    results[vectorIdx] = sqrt(distSq);
}

// Inner Product (Cosine-ready) Kernel for Large Dimensions
kernel void dot_product_f32_large(
    const device float* query [[buffer(0)]],
    const device float* vectors [[buffer(1)]],
    device float* results [[buffer(2)]],
    uint vectorIdx [[thread_position_in_grid]],
    constant uint& dims [[buffer(3)]]
) {
    float dotSum = 0.0f;
    uint base = vectorIdx * dims;
    
    uint i = 0;
    for (; i + 3 < dims; i += 4) {
        float4 q = *(const device float4*)(query + i);
        float4 v = *(const device float4*)(vectors + base + i);
        dotSum += dot(q, v);
    }
    
    for (; i < dims; i++) {
        dotSum += query[i] * vectors[base + i];
    }
    
    results[vectorIdx] = dotSum;
}
