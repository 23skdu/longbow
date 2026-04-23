#include <cuda_runtime.h>
#include <device_launch_parameters.h>
#include <cuda_fp16.h>
#include <math.h>

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

}
