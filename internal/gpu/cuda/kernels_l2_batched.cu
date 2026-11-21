#include "kernels_common.cuh"

extern "C" {

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

__global__ void l2_distance_candidates_kernel(
    const float** page_ptrs,
    const uint32_t* candidate_ids,
    const float* query,
    float* distances,
    int dim,
    int count,
    int total_vecs
) {
    extern __shared__ float s_query_cand[];
    if (dim <= 1024) {
        for (int i = threadIdx.x; i < dim; i += blockDim.x) {
            s_query_cand[i] = query[i];
        }
    }
    __syncthreads();

    int warp_id = threadIdx.x / WARP_SZ;
    int lane_id = threadIdx.x % WARP_SZ;
    int warps_per_block = blockDim.x / WARP_SZ;
    int cand_idx = blockIdx.x * warps_per_block + warp_id;

    if (cand_idx >= count) return;

    uint32_t cid = candidate_ids[cand_idx];
    if (cid >= (uint32_t)total_vecs) {
        if (lane_id == 0) {
            distances[cand_idx] = 1e38f;
        }
        return;
    }

    int page = cid >> 10;
    int local_vec = cid & 1023;
    const float* vec = page_ptrs[page] + (int64_t)local_vec * dim;

    float sum = 0.0f;
    for (int d = lane_id; d < dim; d += WARP_SZ) {
        float q = (dim <= 1024) ? s_query_cand[d] : query[d];
        float diff = vec[d] - q;
        sum += diff * diff;
    }
    for (int offset = WARP_SZ / 2; offset > 0; offset >>= 1) {
        sum += __shfl_xor_sync(0xffffffff, sum, offset);
    }
    if (lane_id == 0) {
        distances[cand_idx] = sqrtf(sum);
    }
}

void launch_l2_distance_candidates(
    const float** page_ptrs,
    const uint32_t* candidate_ids,
    const float* query,
    float* distances,
    int dim,
    int count,
    int total_vecs,
    cudaStream_t stream
) {
    if (count <= 0) return;
    int warps_per_block = 8;
    int vecs_per_block = warps_per_block;
    int blocks = (count + vecs_per_block - 1) / vecs_per_block;
    size_t shared_mem = (dim <= 1024) ? dim * sizeof(float) : 0;
    l2_distance_candidates_kernel<<<blocks, warps_per_block * WARP_SZ, shared_mem, stream>>>(
        page_ptrs, candidate_ids, query, distances, dim, count, total_vecs
    );
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

// Graph BFS Expansion Kernel
// frontier: list of node IDs to expand from
// frontierSize: number of nodes in frontier
// offsets: CSR offsets array (node i's neighbors start at offsets[i])
// neighbors: CSR neighbors array
// visited: Bitset to avoid cycles/redundant work
// nextFrontier: Output list of discovered neighbors
// nextFrontierSize: Output counter for nextFrontier

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


} // extern "C"
