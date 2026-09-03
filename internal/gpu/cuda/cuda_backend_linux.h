//go:build gpu && linux
// +build gpu,linux

#ifndef CUDA_BACKEND_H
#define CUDA_BACKEND_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#ifdef USE_GPU
#include <cuda_runtime.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// CUDA initialization functions
int lb_cuda_init_device(int device);
int lb_cuda_get_device_count(int* count);
int lb_cuda_get_device_properties(int device, char* name, size_t nameLen,
                                 int* major, int* minor, size_t* totalMem);
int lb_cuda_get_mem_info(size_t* free, size_t* total);

// Pinned Host Memory and Async Transfers
int lb_cuda_host_alloc(void** ptr, size_t size, unsigned int flags);
int lb_cuda_free_host(void* ptr);
int lb_cuda_memcpy_async(void* dst, const void* src, size_t count, int kind, void* stream);
int lb_cuda_stream_synchronize(void* stream);

// Distance Kernels
void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, void* stream);
void launch_l2_distance_large_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, void* stream);
void launch_dot_product_large_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, void* stream);
void launch_l2_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, void* stream);
void launch_dot_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, void* stream);
void launch_pq_distance_kernel(const float* lookupTable, const unsigned char* codes, float* distances, int m, int count, void* stream);
void launch_turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, void* stream);
void launch_topk_kernel(const float* distances, const int64_t* ids, int n, int k, float* outDistances, int64_t* outIDs, void* stream);

// Graph Kernels
void launch_graph_bfs_expand_kernel(
    const uint32_t* frontier, 
    int frontierSize,
    const uint32_t* offsets,
    const uint32_t* neighbors,
    unsigned long long* visited,
    uint32_t* nextFrontier,
    int* nextFrontierSize,
    void* stream
);

void launch_graph_activation_propagate_kernel(
    const float* activations,
    float* newActivations,
    const uint32_t* frontier,
    int frontierSize,
    const uint32_t* offsets,
    const uint32_t* neighbors,
    const float* weights,
    float alpha,
    void* stream
);

// K-Means Training Kernels
void launch_assign_to_clusters(const float* vectors, const float* centroids, uint32_t* assignments, int dim, int numVectors, int numCentroids, void* stream);
void launch_sum_centroids(const float* vectors, const uint32_t* assignments, float* centroids, uint32_t* counts, int dim, int numVectors, void* stream);
void launch_finalize_centroids(float* centroids, const uint32_t* counts, int dim, int numCentroids, void* stream);
int cuda_pq_encode(void* handle, float* h_vectors, float* h_codebooks, unsigned char* h_codes, int numVectors, int m, int subDim);

#ifdef __cplusplus
}
#endif

#endif
