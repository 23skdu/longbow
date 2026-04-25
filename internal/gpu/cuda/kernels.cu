#include <cuda_runtime.h>
#include <device_launch_parameters.h>
#include <cuda_fp16.h>
#include <math.h>
#include <float.h>

// Top-K implementation using shared memory heap
#define MAX_K 1024

typedef struct {
    float dist;
    int64_t id;
} ResultPair;

__device__ void heap_push(ResultPair* heap, int* size, float dist, int64_t id, int k) {
    if (*size < k) {
        heap[*size] = {dist, id};
        int curr = *size;
        while (curr > 0) {
            int parent = (curr - 1) / 2;
            if (heap[curr].dist > heap[parent].dist) {
                ResultPair tmp = heap[curr];
                heap[curr] = heap[parent];
                heap[parent] = tmp;
                curr = parent;
            } else break;
        }
        (*size)++;
    } else if (dist < heap[0].dist) {
        heap[0] = {dist, id};
        int curr = 0;
        while (true) {
            int left = 2 * curr + 1;
            int right = 2 * curr + 2;
            int largest = curr;
            if (left < k && heap[left].dist > heap[largest].dist) largest = left;
            if (right < k && heap[right].dist > heap[largest].dist) largest = right;
            if (largest != curr) {
                ResultPair tmp = heap[curr];
                heap[curr] = heap[largest];
                heap[largest] = tmp;
                curr = largest;
            } else break;
        }
    }
}

__global__ void select_topk_kernel(const float* distances, const int64_t* ids, int n, int k, float* outDistances, int64_t* outIDs) {
    // Each thread maintains its own small heap if K is very small, 
    // but here we'll use a block-level approach with shared memory.
    // For simplicity and correctness in this phase, each thread will process a portion
    // and we'll use a single-threaded merge at the end of the block, or better, 
    // use atomic operations if we had a global heap.
    
    // Improved: Each thread maintains a local heap in registers (if K is small)
    // or we use a shared memory heap with a lock (slow).
    // Let's use a simpler approach: Each thread block produces one Top-K list.
    
    extern __shared__ char s_mem[];
    ResultPair* blockHeap = (ResultPair*)s_mem; // Shared by block
    __shared__ int blockHeapSize;
    __shared__ int lock;

    if (threadIdx.x == 0) {
        blockHeapSize = 0;
        lock = 0;
    }
    __syncthreads();

    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    int stride = blockDim.x * gridDim.x;

    for (int i = idx; i < n; i += stride) {
        float d = distances[i];
        int64_t id = ids[i];
        
        // Critical section for block-level heap
        bool done = false;
        while (!done) {
            if (atomicCAS(&lock, 0, 1) == 0) {
                heap_push(blockHeap, &blockHeapSize, d, id, k);
                atomicExch(&lock, 0);
                done = true;
            }
        }
    }

    __syncthreads();

    if (threadIdx.x == 0) {
        for (int i = 0; i < blockHeapSize; i++) {
            outDistances[blockIdx.x * k + i] = blockHeap[i].dist;
            outIDs[blockIdx.x * k + i] = blockHeap[i].id;
        }
    }
}

extern "C" {

// L2 Distance Kernel (FP32)
__global__ void l2_distance_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        float sum = 0.0f;
        const float* vec = vectors + idx * dimensions;
        for (int i = 0; i < dimensions; i++) {
            float diff = vec[i] - query[i];
            sum += diff * diff;
        }
        distances[idx] = sqrtf(sum);
    }
}

// PQ Distance Kernel (Asymmetric Distance Computation)
__global__ void pq_distance_kernel(const float* lookupTable, const unsigned char* codes, float* distances, int m, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        float sum = 0.0f;
        const unsigned char* vectorCodes = codes + idx * m;
        for (int i = 0; i < m; i++) {
            sum += lookupTable[i * 256 + vectorCodes[i]];
        }
        distances[idx] = sum;
    }
}

// Optimized FP16 L2 Distance Kernel
// Uses shared memory to cache the query vector and warp-level reduction
__global__ void l2_distance_fp16_kernel_optimized(const __half* vectors, const __half* query, float* distances, int dimensions, int count) {
    extern __shared__ __half s_query[];
    
    // Load query into shared memory (cooperative)
    for (int i = threadIdx.x; i < dimensions; i += blockDim.x) {
        s_query[i] = query[i];
    }
    __syncthreads();

    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        float sum = 0.0f;
        const __half* vec = vectors + idx * dimensions;
        
        int i = 0;
        const __half2* vec2 = (const __half2*)vec;
        const __half2* query2 = (const __half2*)s_query;
        int n2 = dimensions / 2;
        
        #pragma unroll 4
        for (; i < n2; i++) {
            __half2 diff = __hsub2(vec2[i], query2[i]);
            __half2 sq = __hmul2(diff, diff);
            sum += __half2float(__hadd(sq.x, sq.y));
        }
        
        if (dimensions % 2 != 0) {
            float diff = __half2float(vec[dimensions-1]) - __half2float(s_query[dimensions-1]);
            sum += diff * diff;
        }
        
        distances[idx] = sqrtf(sum);
    }
}

// Optimized FP16 Dot Product Kernel
__global__ void dot_distance_fp16_kernel_optimized(const __half* vectors, const __half* query, float* distances, int dimensions, int count) {
    extern __shared__ __half s_query[];
    
    for (int i = threadIdx.x; i < dimensions; i += blockDim.x) {
        s_query[i] = query[i];
    }
    __syncthreads();

    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        float sum = 0.0f;
        const __half* vec = vectors + idx * dimensions;
        
        int i = 0;
        const __half2* vec2 = (const __half2*)vec;
        const __half2* query2 = (const __half2*)s_query;
        int n2 = dimensions / 2;
        
        #pragma unroll 4
        for (; i < n2; i++) {
            __half2 prod = __hmul2(vec2[i], query2[i]);
            sum += __half2float(__hadd(prod.x, prod.y));
        }
        
        if (dimensions % 2 != 0) {
            sum += __half2float(__hmul(vec[dimensions-1], s_query[dimensions-1]));
        }
        
        distances[idx] = sum;
    }
}

// Dispatchers

void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    l2_distance_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(vectors, query, distances, dimensions, count);
}

void launch_l2_distance_fp16_kernel(const __half* vectors, const __half* query, float* distances, int dimensions, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    size_t sharedMemSize = dimensions * sizeof(__half);
    l2_distance_fp16_kernel_optimized<<<blocksPerGrid, threadsPerBlock, sharedMemSize, stream>>>(vectors, query, distances, dimensions, count);
}

void launch_dot_distance_fp16_kernel(const __half* vectors, const __half* query, float* distances, int dimensions, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    size_t sharedMemSize = dimensions * sizeof(__half);
    dot_distance_fp16_kernel_optimized<<<blocksPerGrid, threadsPerBlock, sharedMemSize, stream>>>(vectors, query, distances, dimensions, count);
}

void launch_pq_distance_kernel(const float* lookupTable, const unsigned char* codes, float* distances, int m, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    pq_distance_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(lookupTable, codes, distances, m, count);
}

// TurboQuant Distance Kernel
__global__ void turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        int angleCount = pow2 - 1;
        int angleBytes = (angleCount * bitsPerAngle + 7) / 8;
        int bitBytes = (pow2 + 7) / 8;
        int stride = 4 + angleBytes + bitBytes;
        
        const unsigned char* data = tqData + (idx * stride);
        float radius = *(const float*)data;
        const unsigned char* packedAngles = data + 4;
        const unsigned char* qjlBits = data + 4 + angleBytes;
        
        float recon[1024]; // Max supported for now
        recon[0] = radius;
        int currentLevelSize = 1;
        int angleOffset = angleCount;
        
        while (currentLevelSize < pow2) {
            angleOffset -= currentLevelSize;
            for (int i = currentLevelSize - 1; i >= 0; i--) {
                float r = recon[i];
                int bitStart = (angleOffset + i) * bitsPerAngle;
                unsigned int q = 0;
                for (int k = 0; k < bitsPerAngle; k++) {
                    int bitIdx = bitStart + k;
                    if ((packedAngles[bitIdx / 8] >> (bitIdx % 8)) & 1) {
                        q |= (1 << k);
                    }
                }
                float theta = (float(q) / ((1 << bitsPerAngle) - 1)) * 2.0f * 3.14159265f - 3.14159265f;
                float s, c;
                sincosf(theta, &s, &c);
                recon[2*i] = r * c;
                recon[2*i+1] = r * s;
            }
            currentLevelSize *= 2;
        }
        
        float sum = 0.0f;
        float correctionFactor = radius / sqrtf((float)pow2) * 0.1f;
        for (int i = 0; i < dim; i++) {
            float val = recon[i];
            if ((qjlBits[i / 8] >> (i % 8)) & 1) val += correctionFactor;
            else val -= 0.1f;
            
            float diff = query[i] - val;
            sum += diff * diff;
        }
        distances[idx] = sqrtf(sum);
    }
}

// Fused Filtered L2 Distance Kernel
__global__ void l2_distance_filtered_kernel(const float* vectors, const float* query, float* distances, const unsigned long long* bitset, int dimensions, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        // Check if filtered out
        if (bitset && !((bitset[idx / 64] >> (idx % 64)) & 1)) {
            distances[idx] = 1e30f; // Max distance for filtered out
            return;
        }

        float sum = 0.0f;
        const float* vec = vectors + idx * dimensions;
        for (int i = 0; i < dimensions; i++) {
            float diff = vec[i] - query[i];
            sum += diff * diff;
        }
        distances[idx] = sqrtf(sum);
    }
}

void launch_l2_distance_filtered_kernel(const float* vectors, const float* query, float* distances, const unsigned long long* bitset, int dimensions, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    l2_distance_filtered_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(vectors, query, distances, bitset, dimensions, count);
}

void launch_turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    turboquant_distance_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(query, tqData, distances, dim, pow2, bitsPerAngle, count);
}

void launch_topk_kernel(const float* distances, const int64_t* ids, int n, int k, float* outDistances, int64_t* outIDs, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = 1; // For now, single block merge for simplicity, or 2-pass
    size_t sharedMemSize = k * sizeof(ResultPair);
    select_topk_kernel<<<blocksPerGrid, threadsPerBlock, sharedMemSize, stream>>>(distances, ids, n, k, outDistances, outIDs);
}

// Graph BFS Expansion Kernel
// frontier: list of node IDs to expand from
// frontierSize: number of nodes in frontier
// offsets: CSR offsets array (node i's neighbors start at offsets[i])
// neighbors: CSR neighbors array
// visited: Bitset to avoid cycles/redundant work
// nextFrontier: Output list of discovered neighbors
// nextFrontierSize: Output counter for nextFrontier
__global__ void graph_bfs_expand_kernel(
    const uint32_t* frontier, 
    int frontierSize,
    const uint32_t* offsets,
    const uint32_t* neighbors,
    unsigned long long* visited,
    uint32_t* nextFrontier,
    int* nextFrontierSize
) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < frontierSize) {
        uint32_t nodeID = frontier[idx];
        uint32_t start = offsets[nodeID];
        uint32_t end = offsets[nodeID + 1];
        
        for (uint32_t i = start; i < end; i++) {
            uint32_t neighborID = neighbors[i];
            
            // Atomic bitset check and set
            unsigned long long mask = 1ULL << (neighborID % 64);
            unsigned long long old = atomicOr(&visited[neighborID / 64], mask);
            
            if (!(old & mask)) {
                // Newly discovered node
                int pos = atomicAdd(nextFrontierSize, 1);
                nextFrontier[pos] = neighborID;
            }
        }
    }
}

// Graph Activation Propagation Kernel
// activations: Current activation scores per node
// newActivations: Output activation scores
// alpha: Decay factor
// weights: Edge weights (optional)
__global__ void graph_activation_propagate_kernel(
    const float* activations,
    float* newActivations,
    const uint32_t* frontier,
    int frontierSize,
    const uint32_t* offsets,
    const uint32_t* neighbors,
    const float* weights,
    float alpha
) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < frontierSize) {
        uint32_t nodeID = frontier[idx];
        float parentScore = activations[nodeID];
        uint32_t start = offsets[nodeID];
        uint32_t end = offsets[nodeID + 1];
        
        for (uint32_t i = start; i < end; i++) {
            uint32_t neighborID = neighbors[i];
            float edgeWeight = weights ? weights[i] : 1.0f;
            float scoreToPass = parentScore * alpha * edgeWeight;
            
            atomicAdd(&newActivations[neighborID], scoreToPass);
        }
    }
}

void launch_graph_bfs_expand_kernel(
    const uint32_t* frontier, 
    int frontierSize,
    const uint32_t* offsets,
    const uint32_t* neighbors,
    unsigned long long* visited,
    uint32_t* nextFrontier,
    int* nextFrontierSize,
    cudaStream_t stream
) {
    if (frontierSize == 0) return;
    int threadsPerBlock = 256;
    int blocksPerGrid = (frontierSize + threadsPerBlock - 1) / threadsPerBlock;
    graph_bfs_expand_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(
        frontier, frontierSize, offsets, neighbors, visited, nextFrontier, nextFrontierSize
    );
}

void launch_graph_activation_propagate_kernel(
    const float* activations,
    float* newActivations,
    const uint32_t* frontier,
    int frontierSize,
    const uint32_t* offsets,
    const uint32_t* neighbors,
    const float* weights,
    float alpha,
    cudaStream_t stream
) {
    if (frontierSize == 0) return;
    int threadsPerBlock = 256;
    int blocksPerGrid = (frontierSize + threadsPerBlock - 1) / threadsPerBlock;
    graph_activation_propagate_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(
        activations, newActivations, frontier, frontierSize, offsets, neighbors, weights, alpha
    );
}

}
