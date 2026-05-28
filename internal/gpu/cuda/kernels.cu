#include <cuda_runtime.h>
#include <device_launch_parameters.h>
#include <cuda_fp16.h>
#include <math.h>
#include <stdint.h>

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
    // Thread 0 of the single block processes all elements sequentially to avoid warp-divergence deadlocks
    extern __shared__ char s_mem[];
    ResultPair* blockHeap = (ResultPair*)s_mem; // Shared by block
    __shared__ int blockHeapSize;

    if (threadIdx.x == 0) {
        blockHeapSize = 0;
        
        // Single thread sequentially builds the heap to bypass expensive warp lock serialization
        for (int i = 0; i < n; i++) {
            heap_push(blockHeap, &blockHeapSize, distances[i], ids[i], k);
        }
        
        // Heapsort: repeatedly extract the max element and place it at the end of the heap range
        int originalSize = blockHeapSize;
        while (blockHeapSize > 1) {
            // Swap the root (largest) with the last element
            ResultPair maxVal = blockHeap[0];
            blockHeap[0] = blockHeap[blockHeapSize - 1];
            blockHeap[blockHeapSize - 1] = maxVal;
            
            // Decrease heap size
            blockHeapSize--;
            
            // Re-heapify from root
            int curr = 0;
            while (true) {
                int left = 2 * curr + 1;
                int right = 2 * curr + 2;
                int largest = curr;
                if (left < blockHeapSize && blockHeap[left].dist > blockHeap[largest].dist) largest = left;
                if (right < blockHeapSize && blockHeap[right].dist > blockHeap[largest].dist) largest = right;
                if (largest != curr) {
                    ResultPair tmp = blockHeap[curr];
                    blockHeap[curr] = blockHeap[largest];
                    blockHeap[largest] = tmp;
                    curr = largest;
                } else break;
            }
        }
        
        // Now the blockHeap elements are sorted in ascending order (smallest first) from index 0 to originalSize - 1!
        // Write out final sorted results
        for (int i = 0; i < originalSize; i++) {
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

// Optimized L2 Distance Kernel (FP32) with coalesced global memory access.
// Each warp processes one vector; lanes within a warp read consecutive dimension
// elements from global memory (fully coalesced). Query is cached in shared memory.
// Uses extern shared memory sized to dim*sizeof(float).
#define WARP_SZ 32
__global__ void l2_distance_kernel_v2(const float* vectors, const float* query, float* distances, int dim, int count) {
    extern __shared__ float s_query_f32[];

    // Cooperative query load into shared memory
    if (threadIdx.x < dim) {
        s_query_f32[threadIdx.x] = query[threadIdx.x];
    }
    __syncthreads();

    int warp_id = threadIdx.x / WARP_SZ;
    int lane_id = threadIdx.x % WARP_SZ;
    int warps_per_block = blockDim.x / WARP_SZ;
    int vec_idx = blockIdx.x * warps_per_block + warp_id;

    if (vec_idx >= count) return;

    const float* vec = vectors + (int64_t)vec_idx * dim;
    float sum = 0.0f;

    // Each lane processes elements spaced WARP_SZ apart.
    // Together, all lanes in the warp access consecutive elements in each iteration -> fully coalesced.
    for (int d = lane_id; d < dim; d += WARP_SZ) {
        float diff = vec[d] - s_query_f32[d];
        sum += diff * diff;
    }

    // Warp-level reduction
    for (int offset = WARP_SZ / 2; offset > 0; offset >>= 1) {
        sum += __shfl_xor_sync(0xffffffff, sum, offset);
    }

    if (lane_id == 0) {
        distances[vec_idx] = sqrtf(sum);
    }
}

// Dispatchers

void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    l2_distance_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(vectors, query, distances, dimensions, count);
}

void launch_l2_distance_kernel_v2(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream) {
    int warps_per_block = 8; // 256 threads total
    int vectors_per_block = warps_per_block; // one vector per warp
    int blocks = (count + vectors_per_block - 1) / vectors_per_block;
    size_t shared_mem = dim * sizeof(float);
    l2_distance_kernel_v2<<<blocks, warps_per_block * WARP_SZ, shared_mem, stream>>>(vectors, query, distances, dim, count);
}

// Batched variant: processes vectors from multiple pages in a single launch.
// page_ptrs: device array of GPU page pointers
// page_starts: device array of cumulative vector counts (num_pages+1 elements, page_starts[num_pages]=total)
__global__ void l2_distance_kernel_v2_batched(
    const float** page_ptrs, const int* page_starts,
    const float* query, float* distances,
    int dim, int num_pages
) {
    extern __shared__ float s_query_batch[];
    if (threadIdx.x < dim) {
        s_query_batch[threadIdx.x] = query[threadIdx.x];
    }
    __syncthreads();

    int warp_id = threadIdx.x / WARP_SZ;
    int lane_id = threadIdx.x % WARP_SZ;
    int warps_per_block = blockDim.x / WARP_SZ;
    int global_vec = blockIdx.x * warps_per_block + warp_id;

    int total_count = page_starts[num_pages];
    if (global_vec >= total_count) return;

    int lo = 0, hi = num_pages;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (global_vec < page_starts[mid]) hi = mid;
        else lo = mid + 1;
    }
    int page = lo - 1;
    int local_vec = global_vec - page_starts[page];

    const float* vec = page_ptrs[page] + (int64_t)local_vec * dim;
    float sum = 0.0f;
    for (int d = lane_id; d < dim; d += WARP_SZ) {
        float diff = vec[d] - s_query_batch[d];
        sum += diff * diff;
    }
    for (int offset = WARP_SZ / 2; offset > 0; offset >>= 1) {
        sum += __shfl_xor_sync(0xffffffff, sum, offset);
    }
    if (lane_id == 0) {
        distances[global_vec] = sqrtf(sum);
    }
}

void launch_l2_distance_kernel_v2_batched(
    const float** page_ptrs, const int* page_starts,
    const float* query, float* distances,
    int dim, int total_count, int num_pages,
    cudaStream_t stream
) {
    int warps_per_block = 8;
    int vecs_per_block = warps_per_block;
    int blocks = (total_count + vecs_per_block - 1) / vecs_per_block;
    size_t shared_mem = dim * sizeof(float);
    l2_distance_kernel_v2_batched<<<blocks, warps_per_block * WARP_SZ, shared_mem, stream>>>(
        page_ptrs, page_starts, query, distances, dim, num_pages
    );
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

// Original TurboQuant Distance Kernel (per-thread stack allocation — kept for backward compat)
__global__ void turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count) {
    extern __shared__ float s_query_orig[];
    for (int i = threadIdx.x; i < dim; i += blockDim.x) {
        s_query_orig[i] = query[i];
    }
    __syncthreads();
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    int angleCount = pow2 - 1;
    int angleBytes = (angleCount * bitsPerAngle + 7) / 8;
    int rawStride = 4 + angleBytes + ((pow2 + 7) / 8);
    int stride = (rawStride + 3) & ~3;
    const unsigned char* data = tqData + (idx * stride);
    float radius = *(const float*)data;
    const unsigned char* packedAngles = data + 4;
    const unsigned char* qjlBits = data + 4 + angleBytes;
    float recon[1024];
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
                if ((packedAngles[bitIdx / 8] >> (bitIdx % 8)) & 1) q |= (1 << k);
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
        float diff = s_query_orig[i] - val;
        sum += diff * diff;
    }
    distances[idx] = sqrtf(sum);
}

// TurboQuant Distance Kernel v2 (per-block reconstruction with __shared__ buffer, no per-thread stack overflow)
// Each block processes one vector; reconstruction buffer lives in shared memory.
__global__ void turboquant_distance_kernel_v2(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count) {
    extern __shared__ float s_recon[];

    int vec_idx = blockIdx.x;
    if (vec_idx >= count) return;

    int angleCount = pow2 - 1;
    int angleBytes = (angleCount * bitsPerAngle + 7) / 8;
    int rawStride = 4 + angleBytes + ((pow2 + 7) / 8);
    int stride = (rawStride + 3) & ~3;

    const unsigned char* data = tqData + (vec_idx * stride);
    float radius = *(const float*)data;
    const unsigned char* packedAngles = data + 4;
    const unsigned char* qjlBits = data + 4 + angleBytes;

    // Single thread handles the hierarchical reconstruction (sequential dependency chain)
    if (threadIdx.x == 0) {
        s_recon[0] = radius;
        int currentLevelSize = 1;
        int angleOffset = angleCount;

        while (currentLevelSize < pow2) {
            angleOffset -= currentLevelSize;
            for (int i = currentLevelSize - 1; i >= 0; i--) {
                float r = s_recon[i];
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
                s_recon[2 * i] = r * c;
                s_recon[2 * i + 1] = r * s;
            }
            currentLevelSize *= 2;
        }
    }
    __syncthreads();

    // Coalesced distance computation: warp-per-vector, lane-stride access to s_recon
    int warp_id = threadIdx.x / WARP_SZ;
    int lane_id = threadIdx.x % WARP_SZ;
    int warps_per_block = blockDim.x / WARP_SZ;

    float sum = 0.0f;
    float correctionFactor = radius / sqrtf((float)pow2) * 0.1f;

    for (int d = lane_id; d < dim; d += WARP_SZ) {
        float val = s_recon[d];
        if ((qjlBits[d / 8] >> (d % 8)) & 1) {
            val += correctionFactor;
        } else {
            val -= 0.1f;
        }
        float diff = query[d] - val;
        sum += diff * diff;
    }

    for (int offset = WARP_SZ / 2; offset > 0; offset >>= 1) {
        sum += __shfl_xor_sync(0xffffffff, sum, offset);
    }

    __shared__ float warp_sums[8];
    if (lane_id == 0) {
        warp_sums[warp_id] = sum;
    }
    __syncthreads();

    if (threadIdx.x == 0) {
        float total = 0.0f;
        for (int w = 0; w < warps_per_block; w++) {
            total += warp_sums[w];
        }
        distances[vec_idx] = sqrtf(total);
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
    size_t sharedMemSize = dim * sizeof(float);
    turboquant_distance_kernel<<<blocksPerGrid, threadsPerBlock, sharedMemSize, stream>>>(query, tqData, distances, dim, pow2, bitsPerAngle, count);
}

void launch_turboquant_distance_kernel_v2(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream) {
    // One block per vector; shared memory sized to pow2 floats for reconstruction buffer
    int blocks = count;
    int threads = 256;
    size_t shared_mem = pow2 * sizeof(float);
    turboquant_distance_kernel_v2<<<blocks, threads, shared_mem, stream>>>(query, tqData, distances, dim, pow2, bitsPerAngle, count);
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


// Coalesced Euclidean Distance Kernel for Large Dimensions (dim > 1024).
// Uses the same warp-per-vector pattern as l2_distance_kernel_v2 with float4 vectorized loads.
__global__ void l2_distance_kernel_large_v2(const float* vectors, const float* query, float* distances, int dim, int count) {
    extern __shared__ float s_query_large[];

    if (threadIdx.x < dim) {
        s_query_large[threadIdx.x] = query[threadIdx.x];
    }
    __syncthreads();

    int warp_id = threadIdx.x / WARP_SZ;
    int lane_id = threadIdx.x % WARP_SZ;
    int warps_per_block = blockDim.x / WARP_SZ;
    int vec_idx = blockIdx.x * warps_per_block + warp_id;

    if (vec_idx >= count) return;

    const float* vec = vectors + (int64_t)vec_idx * dim;
    float sum = 0.0f;

    // float4 vectorized: each lane processes one float4 per iteration
    // Consecutive lanes read consecutive float4s -> fully coalesced (128B per warp iteration)
    const float4* vec4 = (const float4*)vec;
    const float4* query4 = (const float4*)s_query_large;
    int n4 = dim / 4;

    for (int i = 0; i < n4; i += WARP_SZ) {
        float4 v = vec4[i + lane_id];
        float4 q = query4[i + lane_id];
        float dx = v.x - q.x;
        float dy = v.y - q.y;
        float dz = v.z - q.z;
        float dw = v.w - q.w;
        sum += dx*dx + dy*dy + dz*dz + dw*dw;
    }

    // Remainder elements (dim % 4)
    int rem = dim & 3;
    if (rem > 0 && lane_id < rem) {
        float diff = vec[dim - rem + lane_id] - s_query_large[dim - rem + lane_id];
        sum += diff * diff;
    }

    for (int offset = WARP_SZ / 2; offset > 0; offset >>= 1) {
        sum += __shfl_xor_sync(0xffffffff, sum, offset);
    }

    if (lane_id == 0) {
        distances[vec_idx] = sqrtf(sum);
    }
}

// Dot Product Kernel for Large Dimensions (Vectorized with float4)
__global__ void dot_product_kernel_large(const float* vectors, const float* query, float* distances, int dimensions, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        float sum = 0.0f;
        const float* vec = vectors + idx * dimensions;
        
        int i = 0;
        if (((uintptr_t)vec & 0xF) == 0 && ((uintptr_t)query & 0xF) == 0) {
            const float4* vec4 = (const float4*)vec;
            const float4* query4 = (const float4*)query;
            int n4 = dimensions / 4;
            
            #pragma unroll 4
            for (; i < n4; i++) {
                float4 v = vec4[i];
                float4 q = query4[i];
                sum += v.x*q.x + v.y*q.y + v.z*q.z + v.w*q.w;
            }
            i *= 4;
        }
        
        for (; i < dimensions; i++) {
            sum += vec[i] * query[i];
        }
        distances[idx] = sum;
    }
}

void launch_l2_distance_large_kernel_v2(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream) {
    int warps_per_block = 8;
    int vectors_per_block = warps_per_block;
    int blocks = (count + vectors_per_block - 1) / vectors_per_block;
    size_t shared_mem = dim * sizeof(float);
    l2_distance_kernel_large_v2<<<blocks, warps_per_block * WARP_SZ, shared_mem, stream>>>(vectors, query, distances, dim, count);
}

// Batched variant for large dimensions (dim > 1024)
__global__ void l2_distance_kernel_large_v2_batched(
    const float** page_ptrs, const int* page_starts,
    const float* query, float* distances,
    int dim, int num_pages
) {
    extern __shared__ float s_query_large_batch[];
    if (threadIdx.x < dim) {
        s_query_large_batch[threadIdx.x] = query[threadIdx.x];
    }
    __syncthreads();

    int warp_id = threadIdx.x / WARP_SZ;
    int lane_id = threadIdx.x % WARP_SZ;
    int warps_per_block = blockDim.x / WARP_SZ;
    int global_vec = blockIdx.x * warps_per_block + warp_id;

    int total_count = page_starts[num_pages];
    if (global_vec >= total_count) return;

    int lo = 0, hi = num_pages;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (global_vec < page_starts[mid]) hi = mid;
        else lo = mid + 1;
    }
    int page = lo - 1;
    int local_vec = global_vec - page_starts[page];

    const float* vec = page_ptrs[page] + (int64_t)local_vec * dim;
    float sum = 0.0f;

    const float4* vec4 = (const float4*)vec;
    const float4* query4 = (const float4*)s_query_large_batch;
    int n4 = dim / 4;

    for (int i = 0; i < n4; i += WARP_SZ) {
        float4 v = vec4[i + lane_id];
        float4 q = query4[i + lane_id];
        float dx = v.x - q.x;
        float dy = v.y - q.y;
        float dz = v.z - q.z;
        float dw = v.w - q.w;
        sum += dx*dx + dy*dy + dz*dz + dw*dw;
    }

    int rem = dim & 3;
    if (rem > 0 && lane_id < rem) {
        float diff = vec[dim - rem + lane_id] - s_query_large_batch[dim - rem + lane_id];
        sum += diff * diff;
    }

    for (int offset = WARP_SZ / 2; offset > 0; offset >>= 1) {
        sum += __shfl_xor_sync(0xffffffff, sum, offset);
    }
    if (lane_id == 0) {
        distances[global_vec] = sqrtf(sum);
    }
}

void launch_l2_distance_kernel_large_v2_batched(
    const float** page_ptrs, const int* page_starts,
    const float* query, float* distances,
    int dim, int total_count, int num_pages,
    cudaStream_t stream
) {
    int warps_per_block = 8;
    int vecs_per_block = warps_per_block;
    int blocks = (total_count + vecs_per_block - 1) / vecs_per_block;
    size_t shared_mem = dim * sizeof(float);
    l2_distance_kernel_large_v2_batched<<<blocks, warps_per_block * WARP_SZ, shared_mem, stream>>>(
        page_ptrs, page_starts, query, distances, dim, num_pages
    );
}

void launch_dot_product_large_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    dot_product_kernel_large<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(vectors, query, distances, dimensions, count);
}

// Haversine Distance Kernel
__global__ void haversine_distance_kernel(const float* center, const float* points, float* distances, float earthRadius, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        float lat1 = center[0] * 3.14159265f / 180.0f;
        float lon1 = center[1] * 3.14159265f / 180.0f;
        float lat2 = points[idx * 2] * 3.14159265f / 180.0f;
        float lon2 = points[idx * 2 + 1] * 3.14159265f / 180.0f;
        
        float dLat = lat2 - lat1;
        float dLon = lon2 - lon1;
        
        float a = sinf(dLat / 2.0f) * sinf(dLat / 2.0f) + 
                  cosf(lat1) * cosf(lat2) * 
                  sinf(dLon / 2.0f) * sinf(dLon / 2.0f);
        float c = 2.0f * atan2f(sqrtf(a), sqrtf(1.0f - a));
        distances[idx] = earthRadius * c;
    }
}

// Norm Squared Kernel
__global__ void l2_squared_kernel(const float* vectors, float* results, int dimensions, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < count) {
        float sum = 0.0f;
        const float* vec = vectors + idx * dimensions;
        for (int i = 0; i < dimensions; i++) {
            float v = vec[i];
            sum += v * v;
        }
        results[idx] = sum;
    }
}

void launch_haversine_distance_kernel(const float* center, const float* points, float* distances, float earthRadius, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    haversine_distance_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(center, points, distances, earthRadius, count);
}

void launch_l2_squared_kernel(const float* vectors, float* results, int dimensions, int count, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (count + threadsPerBlock - 1) / threadsPerBlock;
    l2_squared_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(vectors, results, dimensions, count);
}

// K-Means E-step: Assign vectors to nearest clusters
__global__ void assign_to_clusters_kernel(
    const float* vectors,
    const float* centroids,
    uint32_t* assignments,
    int dimensions,
    int numVectors,
    int numCentroids
) {
    int gid = blockIdx.x * blockDim.x + threadIdx.x;
    if (gid >= numVectors) return;

    float minDist = 1e38f;
    uint32_t bestCent = 0;
    const float* vec = vectors + gid * dimensions;

    for (uint32_t c = 0; c < numCentroids; c++) {
        float dist = 0.0f;
        const float* cent = centroids + c * dimensions;
        for (int i = 0; i < dimensions; i++) {
            float diff = vec[i] - cent[i];
            dist += diff * diff;
        }
        if (dist < minDist) {
            minDist = dist;
            bestCent = c;
        }
    }
    assignments[gid] = bestCent;
}

// K-Means M-step: Sum vectors in each cluster
__global__ void sum_centroids_kernel(
    const float* vectors,
    const uint32_t* assignments,
    float* centroids,
    uint32_t* counts,
    int dimensions,
    int numVectors
) {
    int gid = blockIdx.x * blockDim.x + threadIdx.x;
    if (gid >= numVectors) return;

    uint32_t clusterID = assignments[gid];
    const float* vec = vectors + gid * dimensions;
    float* cent = centroids + clusterID * dimensions;

    atomicAdd(&counts[clusterID], 1);
    for (int i = 0; i < dimensions; i++) {
        atomicAdd(&cent[i], vec[i]);
    }
}

// K-Means M-step: Finalize centroids by dividing by counts
__global__ void finalize_centroids_kernel(
    float* centroids,
    const uint32_t* counts,
    int dimensions,
    int numCentroids
) {
    int gid = blockIdx.x * blockDim.x + threadIdx.x;
    if (gid >= numCentroids) return;

    uint32_t count = counts[gid];
    if (count == 0) return;

    float invCount = 1.0f / (float)count;
    float* cent = centroids + gid * dimensions;
    for (int i = 0; i < dimensions; i++) {
        cent[i] *= invCount;
    }
}

void launch_assign_to_clusters(const float* vectors, const float* centroids, uint32_t* assignments, int dim, int numVectors, int numCentroids, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (numVectors + threadsPerBlock - 1) / threadsPerBlock;
    assign_to_clusters_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(vectors, centroids, assignments, dim, numVectors, numCentroids);
}

void launch_sum_centroids(const float* vectors, const uint32_t* assignments, float* centroids, uint32_t* counts, int dim, int numVectors, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (numVectors + threadsPerBlock - 1) / threadsPerBlock;
    sum_centroids_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(vectors, assignments, centroids, counts, dim, numVectors);
}

void launch_finalize_centroids(float* centroids, const uint32_t* counts, int dim, int numCentroids, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (numCentroids + threadsPerBlock - 1) / threadsPerBlock;
    finalize_centroids_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(centroids, counts, dim, numCentroids);
}
// PQ Encoding: Assign subspace segments of vectors to nearest codebook centroids
__global__ void pq_encode_kernel(
    const float* vectors,
    const float* codebooks, // [m][256][subDim]
    unsigned char* codes,   // [numVectors][m]
    int dimensions,
    int numVectors,
    int m,
    int subDim
) {
    int gid = blockIdx.x * blockDim.x + threadIdx.x;
    if (gid >= numVectors) return;

    for (int sub = 0; sub < m; sub++) {
        const float* vecSub = vectors + (size_t)gid * dimensions + (size_t)sub * subDim;
        const float* cb = codebooks + (size_t)sub * 256 * subDim;
        
        float minDist = 1e38f;
        unsigned char bestIdx = 0;
        
        for (int c = 0; c < 256; c++) {
            float dist = 0.0f;
            const float* cent = cb + (size_t)c * subDim;
            for (int i = 0; i < subDim; i++) {
                float diff = vecSub[i] - cent[i];
                dist += diff * diff;
            }
            if (dist < minDist) {
                minDist = dist;
                bestIdx = (unsigned char)c;
            }
        }
        codes[(size_t)gid * m + sub] = bestIdx;
    }
}

void launch_pq_encode_kernel(
    const float* vectors,
    const float* codebooks,
    unsigned char* codes,
    int dimensions,
    int numVectors,
    int m,
    int subDim,
    cudaStream_t stream
) {
    int threadsPerBlock = 256;
    int blocksPerGrid = (numVectors + threadsPerBlock - 1) / threadsPerBlock;
    pq_encode_kernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(
        vectors, codebooks, codes, dimensions, numVectors, m, subDim
    );
}

int cuda_pq_encode(
    void* handle,
    float* h_vectors,
    float* h_codebooks,
    unsigned char* h_codes,
    int numVectors,
    int m,
    int subDim
) {
    float *d_vectors, *d_codebooks;
    unsigned char *d_codes;
    
    size_t vecSize = (size_t)numVectors * m * subDim * sizeof(float);
    size_t cbSize = (size_t)m * 256 * subDim * sizeof(float);
    size_t codeSize = (size_t)numVectors * m * sizeof(unsigned char);
    
    if (cudaMalloc(&d_vectors, vecSize) != cudaSuccess) return -1;
    if (cudaMalloc(&d_codebooks, cbSize) != cudaSuccess) {
        cudaFree(d_vectors);
        return -1;
    }
    if (cudaMalloc(&d_codes, codeSize) != cudaSuccess) {
        cudaFree(d_vectors);
        cudaFree(d_codebooks);
        return -1;
    }
    
    cudaMemcpy(d_vectors, h_vectors, vecSize, cudaMemcpyHostToDevice);
    cudaMemcpy(d_codebooks, h_codebooks, cbSize, cudaMemcpyHostToDevice);
    
    launch_pq_encode_kernel(d_vectors, d_codebooks, d_codes, m * subDim, numVectors, m, subDim, 0);
    
    cudaMemcpy(h_codes, d_codes, codeSize, cudaMemcpyDeviceToHost);
    
    cudaFree(d_vectors);
    cudaFree(d_codebooks);
    cudaFree(d_codes);
    
    return 0;
}

} // extern "C"

// HNSW Neighbor Pruning Kernel (CUDA)
__global__ void hnsw_prune_neighbors_kernel(
    const uint32_t* candidateIds,
    const float* candidateDists,
    uint32_t* selectedIds,
    uint32_t* selectedCount,
    const float** page_ptrs,
    const int* page_starts,
    int maxNeighbors,
    int numCandidates,
    int dim,
    int total_count,
    int num_pages,
    bool extendedHeuristic
) {
    if (blockIdx.x > 0 || threadIdx.x > 0) return;

    int count = 0;
    for (int i = 0; i < numCandidates && count < maxNeighbors; i++) {
        uint32_t currId = candidateIds[i];
        float currDist = candidateDists[i];
        bool good = true;

        if (currId >= total_count) continue;

        int currPage = 0;
        int currLocal = 0;
        for (int p = 0; p < num_pages; p++) {
            if (currId < page_starts[p+1]) {
                currPage = p;
                currLocal = currId - page_starts[p];
                break;
            }
        }
        const float* v1 = page_ptrs[currPage] + (size_t)currLocal * dim;

        for (int j = 0; j < count; j++) {
            uint32_t selId = selectedIds[j];
            if (selId >= total_count) continue;
            
            int selPage = 0;
            int selLocal = 0;
            for (int p = 0; p < num_pages; p++) {
                if (selId < page_starts[p+1]) {
                    selPage = p;
                    selLocal = selId - page_starts[p];
                    break;
                }
            }
            const float* v2 = page_ptrs[selPage] + (size_t)selLocal * dim;
            
            float distBetween = 0.0f;
            for (int k = 0; k < dim; k++) {
                float d = v1[k] - v2[k];
                distBetween += d * d;
            }
            distBetween = sqrtf(distBetween);

            if (distBetween < currDist) {
                good = false;
                break;
            }
        }

        if (good) {
            selectedIds[count++] = currId;
        }
    }
    *selectedCount = (uint32_t)count;
}

extern "C" {
void launch_hnsw_prune_neighbors_kernel(
    const uint32_t* candidateIds,
    const float* candidateDists,
    uint32_t* selectedIds,
    uint32_t* selectedCount,
    const float* allVectors,
    int maxNeighbors,
    int numCandidates,
    int dim,
    bool extendedHeuristic,
    cudaStream_t stream
) {
    hnsw_prune_neighbors_kernel<<<1, 1, 0, stream>>>(
        candidateIds, candidateDists, selectedIds, selectedCount, allVectors,
        maxNeighbors, numCandidates, dim, extendedHeuristic
    );
}
}
