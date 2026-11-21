#include "kernels_common.cuh"

extern "C" {

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
    float* d_vectors,
    float* d_codebooks,
    unsigned char* d_codes,
    float* h_vectors,
    float* h_codebooks,
    unsigned char* h_codes,
    int numVectors,
    int m,
    int subDim
) {
    size_t vecSize = (size_t)numVectors * m * subDim * sizeof(float);
    size_t cbSize = (size_t)m * 256 * subDim * sizeof(float);
    size_t codeSize = (size_t)numVectors * m * sizeof(unsigned char);
    
    cudaMemcpy(d_vectors, h_vectors, vecSize, cudaMemcpyHostToDevice);
    cudaMemcpy(d_codebooks, h_codebooks, cbSize, cudaMemcpyHostToDevice);
    
    launch_pq_encode_kernel(d_vectors, d_codebooks, d_codes, m * subDim, numVectors, m, subDim, 0);
    
    cudaMemcpy(h_codes, d_codes, codeSize, cudaMemcpyDeviceToHost);
    
    return 0;
}

// ===========================================================================
// Complex Type Kernels (Part 8)
// ===========================================================================
// complex128: stored as interleaved float64 pairs [re, im] in float32 arrays.
// The caller passes the dimension as 2*N where N is the number of complex elements.
// complex64: stored as interleaved float32 pairs [re, im].

// Complex128 L2 Distance: |q - v|^2 = sum((q.re - v.re)^2 + (q.im - v.im)^2)
// Inputs are float32 pairs; accumulate in float (not double) — FP64 math runs
// ~1/64 rate on consumer GPUs (roadmap.md §4).
__global__ void l2_distance_complex128_kernel(const float* vectors, const float* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const float* vec = vectors + (int64_t)idx * dim;
    int i = 0;
    // Vectorized path: dim is even (2*N); when dim%4==0 rows are float4-aligned.
    if ((dim & 3) == 0) {
        const float4* q4 = reinterpret_cast<const float4*>(query);
        const float4* v4 = reinterpret_cast<const float4*>(vec);
        const int n4 = dim / 4;
        for (; i < n4; i++) {
            const float4 q = q4[i];
            const float4 v = v4[i];
            const float d0 = q.x - v.x;
            const float d1 = q.y - v.y;
            const float d2 = q.z - v.z;
            const float d3 = q.w - v.w;
            sum += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3;
        }
        i = dim;
    } else {
        for (; i < dim; i += 2) {
            const float dRe = query[i] - vec[i];
            const float dIm = query[i + 1] - vec[i + 1];
            sum += dRe * dRe + dIm * dIm;
        }
    }
    distances[idx] = sqrtf(sum);
}

void launch_l2_distance_complex128_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_complex128_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

__global__ void l2_distance_complex128_kernel_v2_batched(
    const float** page_ptrs, const int* page_starts,
    const float* query, float* distances,
    int dim, int num_pages
) {
    extern __shared__ float s_query_c128[];
    if (dim <= 1024) {
        for (int i = threadIdx.x; i < dim; i += blockDim.x) {
            s_query_c128[i] = query[i];
        }
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
        float q = (dim <= 1024) ? s_query_c128[d] : query[d];
        float diff = vec[d] - q;
        sum += diff * diff;
    }
    for (int offset = WARP_SZ / 2; offset > 0; offset >>= 1) {
        sum += __shfl_xor_sync(0xffffffff, sum, offset);
    }
    if (lane_id == 0) {
        distances[global_vec] = sqrtf(sum);
    }
}

void launch_l2_distance_complex128_kernel_v2_batched(
    const float** page_ptrs, const int* page_starts,
    const float* query, float* distances,
    int dim, int total_count, int num_pages,
    cudaStream_t stream
) {
    if (total_count <= 0 || num_pages <= 0) return;
    int warps_per_block = 8;
    int vecs_per_block = warps_per_block;
    int blocks = (total_count + vecs_per_block - 1) / vecs_per_block;
    size_t shared_mem = (dim <= 1024) ? dim * sizeof(float) : 0;
    l2_distance_complex128_kernel_v2_batched<<<blocks, warps_per_block * WARP_SZ, shared_mem, stream>>>(
        page_ptrs, page_starts, query, distances, dim, num_pages
    );
}

// Complex128 Dot Product: q . v* = sum(q.re*v.re + q.im*vIm, j*(q.im*v.re - q.re*v.im))
// Returns only the real part (magnitude of the dot product) for distance ranking.
// float accumulation on float32 inputs (roadmap.md §4).
__global__ void dot_product_complex128_kernel(const float* vectors, const float* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const float* vec = vectors + (int64_t)idx * dim;
    int i = 0;
    if ((dim & 3) == 0) {
        const float4* q4 = reinterpret_cast<const float4*>(query);
        const float4* v4 = reinterpret_cast<const float4*>(vec);
        const int n4 = dim / 4;
        for (; i < n4; i++) {
            const float4 q = q4[i];
            const float4 v = v4[i];
            sum += q.x * v.x + q.y * v.y + q.z * v.z + q.w * v.w;
        }
        i = dim;
    } else {
        for (; i < dim; i += 2) {
            sum += query[i] * vec[i] + query[i + 1] * vec[i + 1];
        }
    }
    distances[idx] = sum;
}

void launch_dot_product_complex128_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_complex128_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// Complex128 Cosine Similarity: cos(theta) = |q . v*| / (|q| * |v|)
// float accumulation on float32 inputs (roadmap.md §4).
__global__ void cosine_similarity_complex128_kernel(const float* vectors, const float* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float dotRe = 0.0f, dotIm = 0.0f;
    float qMag = 0.0f, vMag = 0.0f;
    const float* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i += 2) {
        const float qRe = query[i];
        const float qIm = query[i + 1];
        const float vRe = vec[i];
        const float vIm = vec[i + 1];
        dotRe += qRe * vRe + qIm * vIm;
        dotIm += qIm * vRe - qRe * vIm;
        qMag += qRe * qRe + qIm * qIm;
        vMag += vRe * vRe + vIm * vIm;
    }
    const float dotMod = sqrtf(dotRe * dotRe + dotIm * dotIm);
    const float denom = sqrtf(qMag) * sqrtf(vMag);
    distances[idx] = (denom > 1e-10f) ? (dotMod / denom) : 0.0f;
}

void launch_cosine_similarity_complex128_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    cosine_similarity_complex128_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// Complex64 L2 Distance: uses float arithmetic for speed
__global__ void l2_distance_complex64_kernel(const float* vectors, const float* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const float* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i += 2) {
        float dRe = query[i] - vec[i];
        float dIm = query[i + 1] - vec[i + 1];
        sum += dRe * dRe + dIm * dIm;
    }
    distances[idx] = sqrtf(sum);
}

void launch_l2_distance_complex64_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_complex64_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// Complex64 Dot Product
__global__ void dot_product_complex64_kernel(const float* vectors, const float* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const float* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i += 2) {
        sum += query[i] * vec[i] + query[i + 1] * vec[i + 1];
    }
    distances[idx] = sum;
}

void launch_dot_product_complex64_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_complex64_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// Complex64 Cosine Similarity
__global__ void cosine_similarity_complex64_kernel(const float* vectors, const float* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float dotRe = 0.0f, dotIm = 0.0f;
    float qMag = 0.0f, vMag = 0.0f;
    const float* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i += 2) {
        float qRe = query[i];
        float qIm = query[i + 1];
        float vRe = vec[i];
        float vIm = vec[i + 1];
        dotRe += qRe * vRe + qIm * vIm;
        dotIm += qIm * vRe - qRe * vIm;
        qMag += qRe * qRe + qIm * qIm;
        vMag += vRe * vRe + vIm * vIm;
    }
    float dotMod = sqrtf(dotRe * dotRe + dotIm * dotIm);
    float denom = sqrtf(qMag) * sqrtf(vMag);
    distances[idx] = (denom > 1e-10f) ? (dotMod / denom) : 0.0f;
}

void launch_cosine_similarity_complex64_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    cosine_similarity_complex64_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}


} // extern "C"
