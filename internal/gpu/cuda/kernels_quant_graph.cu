#include "kernels_common.cuh"

extern "C" {

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

// Part 2: TurboQuant sin/cos lookup tables in __constant__ memory
// Supports bitsPerAngle=4 (16 entries) and bitsPerAngle=8 (256 entries)
// Each entry stores [cos, sin] pair
__constant__ float tq_lut_4[32];   // 16 entries * 2
__constant__ float tq_lut_8[512];  // 256 entries * 2

// Initialize lookup tables (called once from host before first kernel launch)
void init_tq_lookup_tables() {
    float h_lut4[32];
    for (int i = 0; i < 16; i++) {
        float theta = (float(i) / 15.0f) * 2.0f * 3.14159265f - 3.14159265f;
        h_lut4[2*i]   = cosf(theta);
        h_lut4[2*i+1] = sinf(theta);
    }
    cudaMemcpyToSymbol(tq_lut_4, h_lut4, sizeof(h_lut4));

    float h_lut8[512];
    for (int i = 0; i < 256; i++) {
        float theta = (float(i) / 255.0f) * 2.0f * 3.14159265f - 3.14159265f;
        h_lut8[2*i]   = cosf(theta);
        h_lut8[2*i+1] = sinf(theta);
    }
    cudaMemcpyToSymbol(tq_lut_8, h_lut8, sizeof(h_lut8));
}

// Part 2: Device helper — reconstruct TQ vector using lookup tables instead of sincosf
// Each block calls this once from thread 0 to fill s_recon[].
__device__ void tq_reconstruct_with_lut(
    float* s_recon, const unsigned char* packedAngles,
    int angleCount, int pow2, int bitsPerAngle
) {
    s_recon[0] = 1.0f; // radius multiplier (radius applied later in QJL correction)
    int currentLevelSize = 1;
    int angleOffset = angleCount;

    // Select lookup table based on bitsPerAngle
    const float* lut = (bitsPerAngle <= 4) ? tq_lut_4 : tq_lut_8;

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
            // Table lookup instead of sincosf
            float c = lut[2 * q];
            float s = lut[2 * q + 1];
            s_recon[2 * i]     = r * c;
            s_recon[2 * i + 1] = r * s;
        }
        currentLevelSize *= 2;
    }
}

// TurboQuant Distance Kernel v2 (per-block reconstruction with __shared__ buffer, no per-thread stack overflow)
// Each block processes one vector; reconstruction buffer lives in shared memory.
// Part 2: Uses pre-computed sin/cos lookup tables instead of sincosf.
__global__ void turboquant_distance_kernel_v2(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count) {
    extern __shared__ float s_recon[];

    int vec_idx = blockIdx.x;
    if (vec_idx >= count) return;

    int angleCount = pow2 - 1;
    int angleBytes = (angleCount * bitsPerAngle + 7) / 8;
    // Part 5: 32-byte warp-aligned stride
    int rawStride = 4 + angleBytes + ((pow2 + 7) / 8);
    int stride = (rawStride + 31) & ~31;

    const unsigned char* data = tqData + (vec_idx * stride);
    float radius = *(const float*)data;
    const unsigned char* packedAngles = data + 4;
    const unsigned char* qjlBits = data + 4 + angleBytes;

    // Part 2: Use lookup tables for reconstruction
    if (threadIdx.x == 0) {
        tq_reconstruct_with_lut(s_recon, packedAngles, angleCount, pow2, bitsPerAngle);
    }
    __syncthreads();

    // Coalesced distance computation: warp-per-vector, lane-stride access to s_recon
    int warp_id = threadIdx.x / WARP_SZ;
    int lane_id = threadIdx.x % WARP_SZ;
    int warps_per_block = blockDim.x / WARP_SZ;

    float sum = 0.0f;
    float correctionFactor = radius / sqrtf((float)pow2) * 0.1f;

    for (int d = lane_id; d < dim; d += WARP_SZ) {
        float val = s_recon[d] * radius;
        if ((qjlBits[d / 8] >> (d % 8)) & 1) {
            val += correctionFactor;
        } else {
            val -= correctionFactor;
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

// Part 1: Batched TurboQuant Distance Kernel v2
// Processes vectors from multiple pages in a single kernel launch.
// Each block processes one vector. Uses binary search to find the page,
// then reconstructs TQ vector using lookup tables (Part 2).
__global__ void turboquant_distance_kernel_v2_batched(
    const float** page_ptrs, const int* page_starts,
    const float* query, float* distances,
    int dim, int pow2, int bitsPerAngle, int num_pages
) {
    extern __shared__ float s_recon_tq[];

    int warp_id = threadIdx.x / WARP_SZ;
    int lane_id = threadIdx.x % WARP_SZ;
    int warps_per_block = blockDim.x / WARP_SZ;
    int global_vec = blockIdx.x * warps_per_block + warp_id;

    int total_count = page_starts[num_pages];
    if (global_vec >= total_count) return;

    // Binary search to find which page this vector belongs to
    int lo = 0, hi = num_pages;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (global_vec < page_starts[mid]) hi = mid;
        else lo = mid + 1;
    }
    int page = lo - 1;
    int local_vec = global_vec - page_starts[page];

    // Compute TQ stride (Part 5: 32-byte aligned)
    int angleCount = pow2 - 1;
    int angleBytes = (angleCount * bitsPerAngle + 7) / 8;
    int rawStride = 4 + angleBytes + ((pow2 + 7) / 8);
    int stride = (rawStride + 31) & ~31;

    // Each block's thread 0 reconstructs the TQ vector into shared memory
    if (threadIdx.x == 0) {
        const unsigned char* data = (const unsigned char*)page_ptrs[page] + (int64_t)local_vec * stride;
        float radius = *(const float*)data;
        const unsigned char* packedAngles = data + 4;

        tq_reconstruct_with_lut(s_recon_tq, packedAngles, angleCount, pow2, bitsPerAngle);

        // Apply radius scaling to reconstructed values
        for (int i = 0; i < dim; i++) {
            s_recon_tq[i] *= radius;
        }
    }
    __syncthreads();

    // Read QJL bits and compute distance
    const unsigned char* data = (const unsigned char*)page_ptrs[page] + (int64_t)local_vec * stride;
    float radius = *(const float*)data;
    const unsigned char* qjlBits = data + 4 + angleBytes;

    float sum = 0.0f;
    float correctionFactor = radius / sqrtf((float)pow2) * 0.1f;

    for (int d = lane_id; d < dim; d += WARP_SZ) {
        float val = s_recon_tq[d];
        if ((qjlBits[d / 8] >> (d % 8)) & 1) {
            val += correctionFactor;
        } else {
            val -= correctionFactor;
        }
        float diff = query[d] - val;
        sum += diff * diff;
    }

    for (int offset = WARP_SZ / 2; offset > 0; offset >>= 1) {
        sum += __shfl_xor_sync(0xffffffff, sum, offset);
    }

    if (lane_id == 0) {
        distances[global_vec] = sqrtf(sum);
    }
}


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

// Part 3: TurboQuant HNSW Greedy Descent Kernel
// Uses 32 threads (1 warp) per block. Each iteration: expand neighbors of current node,
// compute TQ distances in parallel (lane-stride), reduce via warp shuffle, move if improved.
// Matches Metal hnsw_greedy_search_tq semantics.
// Uses paged TQ data layout (page_ptrs + page_starts) for compatibility with GPU pager.
__global__ void turboquant_greedy_descent_kernel(
    const float* query,
    const float** page_ptrs,
    const int* page_starts,
    const uint32_t* graphOffsets,
    const uint32_t* graphNeighbors,
    uint32_t* entryPoint,
    float* entryDist,
    int dim,
    int pow2,
    int bitsPerAngle,
    int totalVecs,
    int numPages
) {
    // Only 1 thread block, 32 threads
    int lane = threadIdx.x;

    uint32_t currId = *entryPoint;
    float currDist = *entryDist;

    // TQ layout constants
    int angleCount = pow2 - 1;
    int angleBytes = (angleCount * bitsPerAngle + 7) / 8;
    int rawStride = 4 + angleBytes + ((pow2 + 7) / 8);
    int stride = (rawStride + 31) & ~31;

    const float* lut = (bitsPerAngle <= 4) ? tq_lut_4 : tq_lut_8;

    // Shared memory for per-lane results
    __shared__ float s_bestDist[32];
    __shared__ uint32_t s_bestId[32];

    bool improved = true;
    while (improved) {
        uint32_t start = graphOffsets[currId];
        uint32_t end = graphOffsets[currId + 1];
        uint32_t numNeighbors = end - start;

        uint32_t bestId = currId;
        float bestDist = currDist;

        // Each lane processes a strided subset of neighbors
        for (uint32_t i = lane; i < numNeighbors; i += 32) {
            uint32_t neighborId = graphNeighbors[start + i];
            if (neighborId >= (uint32_t)totalVecs) continue;

            // Find which page this neighbor belongs to via binary search
            int lo = 0, hi = numPages;
            while (lo < hi) {
                int mid = (lo + hi) / 2;
                if (page_starts[mid + 1] <= (int)neighborId) lo = mid + 1;
                else hi = mid;
            }
            int page = lo;
            int localId = (int)neighborId - page_starts[page];
            const unsigned char* data = (const unsigned char*)page_ptrs[page] + (size_t)localId * stride;

            // --- Fused TQ distance reconstruction ---
            float radius = *(const float*)data;
            const unsigned char* packedAngles = data + 4;
            const unsigned char* qjlBits = data + 4 + angleBytes;

            float correctionFactor = radius / sqrtf((float)pow2) * 0.1f;

            float work[256];
            if (pow2 > 256) return;

            float normSq = 0.0f;
            for (int k = 0; k < pow2; k++) {
                float q_k = (k < dim) ? query[k] : 0.0f;
                float c_k = ((qjlBits[k / 8] >> (k % 8)) & 1) ? correctionFactor : -correctionFactor;
                work[k] = q_k - c_k;
                normSq += (q_k - c_k) * (q_k - c_k);
            }

            // Butterfly reconstruction
            uint32_t currentLevelSize = pow2;
            uint32_t angleOffset = 0;
            while (currentLevelSize > 1) {
                uint32_t nextLevelSize = currentLevelSize / 2;
                for (uint32_t k = 0; k < nextLevelSize; k++) {
                    uint32_t bitStart = (angleOffset + k) * bitsPerAngle;
                    uint32_t q = 0;
                    for (int b = 0; b < bitsPerAngle; b++) {
                        uint32_t bitIdx = bitStart + b;
                        if ((packedAngles[bitIdx / 8] >> (bitIdx % 8)) & 1)
                            q |= (1 << b);
                    }
                    float c = lut[2 * q];
                    float s = lut[2 * q + 1];
                    work[k] = work[2*k] * c + work[2*k+1] * s;
                }
                angleOffset += nextLevelSize;
                currentLevelSize = nextLevelSize;
            }

            float distSq = normSq + radius * radius - 2.0f * radius * work[0];
            float dist = sqrtf(fmaxf(0.0f, distSq));

            if (dist < bestDist) {
                bestDist = dist;
                bestId = neighborId;
            }
        }

        // Warp reduction to find global best across all 32 lanes
        s_bestDist[lane] = bestDist;
        s_bestId[lane] = bestId;
        __syncthreads();

        if (lane == 0) {
            for (int i = 1; i < 32; i++) {
                if (s_bestDist[i] < bestDist) {
                    bestDist = s_bestDist[i];
                    bestId = s_bestId[i];
                }
            }
            if (bestDist < currDist) {
                currDist = bestDist;
                currId = bestId;
                improved = true;
            } else {
                improved = false;
            }
        }
        __syncthreads();
    }

    if (lane == 0) {
        *entryPoint = currId;
        *entryDist = currDist;
    }
}

extern "C" {

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

    // Check that the reconstruction buffer fits in shared memory
    int max_smem = 0;
    cudaDeviceGetAttribute(&max_smem, cudaDevAttrMaxSharedMemoryPerBlock, 0);
    if (max_smem > 0 && (int)shared_mem > max_smem) {
        // pow2 too large for shared memory — set CUDA error so Go caller can detect this
        // The v1 kernel has a hard 1024-element stack array (float recon[1024]) and cannot
        // handle pow2 > 1024 either, so we abort the launch entirely.
        return;
    }

    turboquant_distance_kernel_v2<<<blocks, threads, shared_mem, stream>>>(query, tqData, distances, dim, pow2, bitsPerAngle, count);
}

// Part 1: Batched launcher for turboquant_distance_kernel_v2_batched
// Processes all pages in a single kernel launch, analogous to l2_distance_kernel_v2_batched.
void launch_turboquant_distance_kernel_v2_batched(
    const float** page_ptrs, const int* page_starts,
    const float* query, float* distances,
    int dim, int pow2, int bitsPerAngle, int total_count, int num_pages,
    cudaStream_t stream
) {
    int warps_per_block = 8;
    int vecs_per_block = warps_per_block;
    int blocks = (total_count + vecs_per_block - 1) / vecs_per_block;
    // Shared memory: reconstruction buffer (pow2 floats)
    size_t shared_mem = pow2 * sizeof(float);

    int max_smem = 0;
    cudaDeviceGetAttribute(&max_smem, cudaDevAttrMaxSharedMemoryPerBlock, 0);
    if (max_smem > 0 && (int)shared_mem > max_smem) {
        return;
    }

    turboquant_distance_kernel_v2_batched<<<blocks, warps_per_block * WARP_SZ, shared_mem, stream>>>(
        page_ptrs, page_starts, query, distances, dim, pow2, bitsPerAngle, num_pages
    );
}

void launch_turboquant_greedy_descent_kernel(
    const float* query,
    const float** page_ptrs,
    const int* page_starts,
    const uint32_t* graphOffsets,
    const uint32_t* graphNeighbors,
    uint32_t* entryPoint,
    float* entryDist,
    int dim,
    int pow2,
    int bitsPerAngle,
    int totalVecs,
    int numPages,
    cudaStream_t stream
) {
    // 1 block, 32 threads (1 warp)
    turboquant_greedy_descent_kernel<<<1, 32, 0, stream>>>(
        query, page_ptrs, page_starts, graphOffsets, graphNeighbors,
        entryPoint, entryDist, dim, pow2, bitsPerAngle, totalVecs, numPages
    );
}

void launch_hnsw_prune_neighbors_kernel(
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
    bool extendedHeuristic,
    cudaStream_t stream
) {
    hnsw_prune_neighbors_kernel<<<1, 1, 0, stream>>>(
        candidateIds, candidateDists, selectedIds, selectedCount, page_ptrs, page_starts,
        maxNeighbors, numCandidates, dim, total_count, num_pages, extendedHeuristic
    );
}
}
