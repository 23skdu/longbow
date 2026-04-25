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
