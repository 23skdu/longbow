#include "kernels_common.cuh"

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

// ===========================================================================
// Integer Type Kernels (int8, uint8, int16, uint16)
// ===========================================================================
// int8 L2
__global__ void l2_distance_int8_kernel(const int8_t* vectors, const int8_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const int8_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        float diff = float(vec[i]) - float(query[i]);
        sum += diff * diff;
    }
    distances[idx] = sqrtf(sum);
}

void launch_l2_distance_int8_kernel(const int8_t* vectors, const int8_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_int8_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// int8 dot
__global__ void dot_product_int8_kernel(const int8_t* vectors, const int8_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const int8_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        sum += float(vec[i]) * float(query[i]);
    }
    distances[idx] = sum;
}

void launch_dot_product_int8_kernel(const int8_t* vectors, const int8_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_int8_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// uint8 L2
__global__ void l2_distance_uint8_kernel(const uint8_t* vectors, const uint8_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const uint8_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        float diff = float(vec[i]) - float(query[i]);
        sum += diff * diff;
    }
    distances[idx] = sqrtf(sum);
}

void launch_l2_distance_uint8_kernel(const uint8_t* vectors, const uint8_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_uint8_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// uint8 dot
__global__ void dot_product_uint8_kernel(const uint8_t* vectors, const uint8_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const uint8_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        sum += float(vec[i]) * float(query[i]);
    }
    distances[idx] = sum;
}

void launch_dot_product_uint8_kernel(const uint8_t* vectors, const uint8_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_uint8_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// int16 L2
__global__ void l2_distance_int16_kernel(const int16_t* vectors, const int16_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const int16_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        float diff = float(vec[i]) - float(query[i]);
        sum += diff * diff;
    }
    distances[idx] = sqrtf(sum);
}

void launch_l2_distance_int16_kernel(const int16_t* vectors, const int16_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_int16_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// int16 dot
__global__ void dot_product_int16_kernel(const int16_t* vectors, const int16_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const int16_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        sum += float(vec[i]) * float(query[i]);
    }
    distances[idx] = sum;
}

void launch_dot_product_int16_kernel(const int16_t* vectors, const int16_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_int16_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// uint16 L2
__global__ void l2_distance_uint16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const uint16_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        float diff = float(vec[i]) - float(query[i]);
        sum += diff * diff;
    }
    distances[idx] = sqrtf(sum);
}

void launch_l2_distance_uint16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_uint16_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// uint16 dot
__global__ void dot_product_uint16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    float sum = 0.0f;
    const uint16_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        sum += float(vec[i]) * float(query[i]);
    }
    distances[idx] = sum;
}

void launch_dot_product_uint16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_uint16_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// ===========================================================================
// Float64 Kernels
// ===========================================================================
__global__ void l2_distance_float64_kernel(const double* vectors, const double* query, double* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const double* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        double diff = vec[i] - query[i];
        sum += diff * diff;
    }
    distances[idx] = sqrt(sum);
}

void launch_l2_distance_float64_kernel(const double* vectors, const double* query, double* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_float64_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

__global__ void dot_product_float64_kernel(const double* vectors, const double* query, double* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const double* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        sum += vec[i] * query[i];
    }
    distances[idx] = sum;
}

void launch_dot_product_float64_kernel(const double* vectors, const double* query, double* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_float64_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// ===========================================================================
// Int32 Kernels
// ===========================================================================
__global__ void l2_distance_int32_kernel(const int32_t* vectors, const int32_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const int32_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        double diff = double(vec[i]) - double(query[i]);
        sum += diff * diff;
    }
    distances[idx] = sqrtf((float)sum);
}

void launch_l2_distance_int32_kernel(const int32_t* vectors, const int32_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_int32_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

__global__ void dot_product_int32_kernel(const int32_t* vectors, const int32_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const int32_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        sum += double(vec[i]) * double(query[i]);
    }
    distances[idx] = (float)sum;
}

void launch_dot_product_int32_kernel(const int32_t* vectors, const int32_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_int32_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// ===========================================================================
// Uint32 Kernels
// ===========================================================================
__global__ void l2_distance_uint32_kernel(const uint32_t* vectors, const uint32_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const uint32_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        double diff = double(vec[i]) - double(query[i]);
        sum += diff * diff;
    }
    distances[idx] = sqrtf((float)sum);
}

void launch_l2_distance_uint32_kernel(const uint32_t* vectors, const uint32_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_uint32_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

__global__ void dot_product_uint32_kernel(const uint32_t* vectors, const uint32_t* query, float* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const uint32_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        sum += double(vec[i]) * double(query[i]);
    }
    distances[idx] = (float)sum;
}

void launch_dot_product_uint32_kernel(const uint32_t* vectors, const uint32_t* query, float* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_uint32_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// ===========================================================================
// Int64 Kernels
// ===========================================================================
__global__ void l2_distance_int64_kernel(const int64_t* vectors, const int64_t* query, double* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const int64_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        double diff = (double)vec[i] - (double)query[i];
        sum += diff * diff;
    }
    distances[idx] = sqrt(sum);
}

void launch_l2_distance_int64_kernel(const int64_t* vectors, const int64_t* query, double* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_int64_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

__global__ void dot_product_int64_kernel(const int64_t* vectors, const int64_t* query, double* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const int64_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        sum += (double)vec[i] * (double)query[i];
    }
    distances[idx] = sum;
}

void launch_dot_product_int64_kernel(const int64_t* vectors, const int64_t* query, double* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_int64_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

// ===========================================================================
// Uint64 Kernels
// ===========================================================================
__global__ void l2_distance_uint64_kernel(const uint64_t* vectors, const uint64_t* query, double* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const uint64_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        double diff = (double)vec[i] - (double)query[i];
        sum += diff * diff;
    }
    distances[idx] = sqrt(sum);
}

void launch_l2_distance_uint64_kernel(const uint64_t* vectors, const uint64_t* query, double* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    l2_distance_uint64_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

__global__ void dot_product_uint64_kernel(const uint64_t* vectors, const uint64_t* query, double* distances, int dim, int count) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    double sum = 0.0;
    const uint64_t* vec = vectors + (int64_t)idx * dim;
    for (int i = 0; i < dim; i++) {
        sum += (double)vec[i] * (double)query[i];
    }
    distances[idx] = sum;
}

void launch_dot_product_uint64_kernel(const uint64_t* vectors, const uint64_t* query, double* distances, int dim, int count, cudaStream_t stream) {
    int threads = 256;
    int blocks = (count + threads - 1) / threads;
    dot_product_uint64_kernel<<<blocks, threads, 0, stream>>>(vectors, query, distances, dim, count);
}

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

void launch_topk_kernel(const float* distances, const int64_t* ids, int n, int k, float* outDistances, int64_t* outIDs, cudaStream_t stream) {
    int threadsPerBlock = 256;
    int blocksPerGrid = 1; // For now, single block merge for simplicity, or 2-pass
    size_t sharedMemSize = k * sizeof(ResultPair);
    select_topk_kernel<<<blocksPerGrid, threadsPerBlock, sharedMemSize, stream>>>(distances, ids, n, k, outDistances, outIDs);
}

} // extern "C"
