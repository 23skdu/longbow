#ifndef CUDA_KERNELS_DECL_H
#define CUDA_KERNELS_DECL_H

#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>
#include <stdbool.h>

typedef struct {
    int device;
    int dimensions;
    cudaStream_t streams[2];
    void* graphOffsets;
    void* graphNeighbors;
    void* graphWeights;
    int graphNodeCount;
    int graphEdgeCount;
} CUDAIndexHandle;

// Function declarations from kernels.cu
void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_l2_distance_kernel_v2(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_large_kernel_v2(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_kernel_v2_batched(const float** page_ptrs, const int* page_starts, const float* query, float* distances, int dim, int total_count, int num_pages, cudaStream_t stream);
void launch_l2_distance_kernel_large_v2_batched(const float** page_ptrs, const int* page_starts, const float* query, float* distances, int dim, int total_count, int num_pages, cudaStream_t stream);
void launch_l2_distance_float64_kernel(const double* vectors, const double* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_float64_kernel(const double* vectors, const double* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_int32_kernel(const int32_t* vectors, const int32_t* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_int32_kernel(const int32_t* vectors, const int32_t* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_uint32_kernel(const uint32_t* vectors, const uint32_t* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_uint32_kernel(const uint32_t* vectors, const uint32_t* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_int64_kernel(const int64_t* vectors, const int64_t* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_int64_kernel(const int64_t* vectors, const int64_t* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_uint64_kernel(const uint64_t* vectors, const uint64_t* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_uint64_kernel(const uint64_t* vectors, const uint64_t* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_dot_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_pq_distance_kernel(const float* lookupTable, const unsigned char* codes, float* distances, int m, int count, cudaStream_t stream);
void launch_turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream);
void launch_turboquant_distance_kernel_v2(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream);
void launch_turboquant_distance_kernel_v2_batched(const float** page_ptrs, const int* page_starts, const float* query, float* distances, int dim, int pow2, int bitsPerAngle, int total_count, int num_pages, cudaStream_t stream);
void init_tq_lookup_tables();
void launch_l2_distance_filtered_kernel(const float* vectors, const float* query, float* distances, const unsigned long long* bitset, int dimensions, int count, cudaStream_t stream);
void launch_topk_kernel(const float* distances, const int64_t* ids, int n, int k, float* outDistances, int64_t* outIDs, cudaStream_t stream);
int cuda_add_vectors_pq(CUDAIndexHandle* handle, unsigned char* h_codes, int64_t* h_ids, int count, int m);

// Graph functions
void launch_graph_bfs_expand_kernel(const uint32_t* frontier, int frontierSize, const uint32_t* offsets, const uint32_t* neighbors, unsigned long long* visited, uint32_t* nextFrontier, int* nextFrontierSize, cudaStream_t stream);
void launch_graph_activation_propagate_kernel(const float* activations, float* newActivations, const uint32_t* frontier, int frontierSize, const uint32_t* offsets, const uint32_t* neighbors, const float* weights, float alpha, cudaStream_t stream);
void launch_haversine_distance_kernel(const float* center, const float* points, float* distances, float earthRadius, int count, cudaStream_t stream);
void launch_l2_squared_kernel(const float* vectors, float* results, int dimensions, int count, cudaStream_t stream);
void launch_turboquant_greedy_descent_kernel(const float* query, const float** page_ptrs, const int* page_starts, const uint32_t* graphOffsets, const uint32_t* graphNeighbors, uint32_t* entryPoint, float* entryDist, int dim, int pow2, int bitsPerAngle, int totalVecs, int numPages, cudaStream_t stream);

// K-Means Training Kernels
void launch_assign_to_clusters(const float* vectors, const float* centroids, uint32_t* assignments, int dim, int numVectors, int numCentroids, cudaStream_t stream);
void launch_sum_centroids(const float* vectors, const uint32_t* assignments, float* centroids, uint32_t* counts, int dim, int numVectors, cudaStream_t stream);
void launch_finalize_centroids(float* centroids, const uint32_t* counts, int dim, int numCentroids, cudaStream_t stream);
void launch_hnsw_prune_neighbors_kernel(const uint32_t* candidateIds, const float* candidateDists, uint32_t* selectedIds, uint32_t* selectedCount, const float** page_ptrs, const int* page_starts, int maxNeighbors, int numCandidates, int dim, int total_count, int num_pages, bool extendedHeuristic, cudaStream_t stream);

// Part 8: Complex type kernels
void launch_l2_distance_complex128_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_complex128_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_cosine_similarity_complex128_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_complex64_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_complex64_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_cosine_similarity_complex64_kernel(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);

static inline int cuda_train_kmeans(CUDAIndexHandle* handle, float* d_vectors, float* d_centroids, float* d_sumCentroids, uint32_t* d_assignments, uint32_t* d_counts, float* h_vectors, float* h_centroids, int numVectors, int dim, int k, int iterations);
int cuda_pq_encode(CUDAIndexHandle* handle, float* d_vectors, float* d_codebooks, unsigned char* d_codes, float* h_vectors, float* h_codebooks, unsigned char* h_codes, int numVectors, int m, int subDim);

static inline CUDAIndexHandle* cuda_init(int dimensions) {
    int device = 0;
    cudaError_t err = cudaSetDevice(device);
    if (err != cudaSuccess) return NULL;

    CUDAIndexHandle* handle = (CUDAIndexHandle*)malloc(sizeof(CUDAIndexHandle));
    handle->device = device;
    handle->dimensions = dimensions;
    handle->graphOffsets = NULL;
    handle->graphNeighbors = NULL;
    handle->graphWeights = NULL;
    handle->graphNodeCount = 0;
    handle->graphEdgeCount = 0;

    cudaStreamCreate(&handle->streams[0]);
    cudaStreamCreate(&handle->streams[1]);

    // Part 2: Initialize TQ sin/cos lookup tables
    init_tq_lookup_tables();

    return handle;
}

static inline void cuda_free(CUDAIndexHandle* handle) {
    if (!handle) return;
    if (handle->graphOffsets) cudaFree(handle->graphOffsets);
    if (handle->graphNeighbors) cudaFree(handle->graphNeighbors);
    if (handle->graphWeights) cudaFree(handle->graphWeights);
    cudaStreamDestroy(handle->streams[0]);
    cudaStreamDestroy(handle->streams[1]);
    free(handle);
}

static inline void cuda_get_device_info(CUDAIndexHandle* handle, char* name, int maxLen, uint64_t* totalMem) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, handle->device);
    if (err == cudaSuccess) {
        strncpy(name, prop.name, maxLen - 1);
        name[maxLen - 1] = '\0';
        *totalMem = prop.totalGlobalMem;
    } else {
        name[0] = '\0';
        *totalMem = 0;
    }
}

static inline int cuda_train_kmeans(CUDAIndexHandle* handle,
    float* d_vectors, float* d_centroids, float* d_sumCentroids,
    uint32_t* d_assignments, uint32_t* d_counts,
    float* h_vectors, float* h_centroids,
    int numVectors, int dim, int k, int iterations) {
    if (!handle) return -1;

    cudaMemcpy(d_vectors, h_vectors, (size_t)numVectors * dim * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy(d_centroids, h_centroids, (size_t)k * dim * sizeof(float), cudaMemcpyHostToDevice);

    for (int i = 0; i < iterations; i++) {
        cudaMemset(d_counts, 0, (size_t)k * sizeof(uint32_t));
        cudaMemset(d_sumCentroids, 0, (size_t)k * dim * sizeof(float));

        launch_assign_to_clusters(d_vectors, d_centroids, d_assignments, dim, numVectors, k, handle->streams[0]);
        launch_sum_centroids(d_vectors, d_assignments, d_sumCentroids, d_counts, dim, numVectors, handle->streams[0]);
        launch_finalize_centroids(d_sumCentroids, d_counts, dim, k, handle->streams[0]);

        // Update centroids for next iteration
        cudaMemcpy(d_centroids, d_sumCentroids, (size_t)k * dim * sizeof(float), cudaMemcpyDeviceToDevice);
    }

    cudaMemcpy(h_centroids, d_centroids, (size_t)k * dim * sizeof(float), cudaMemcpyDeviceToHost);
    return 0;
}



static inline int cuda_haversine_batch(CUDAIndexHandle* handle, float* d_center, float* d_points, float* d_results, float* h_center, float* h_points, float* h_results, float earthRadius, int count) {
    cudaMemcpy(d_center, h_center, 2 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy(d_points, h_points, count * 2 * sizeof(float), cudaMemcpyHostToDevice);

    launch_haversine_distance_kernel(d_center, d_points, d_results, earthRadius, count, 0);

    cudaMemcpy(h_results, d_results, count * sizeof(float), cudaMemcpyDeviceToHost);

    return 0;
}

static inline int cuda_norm_batch_f32(CUDAIndexHandle* handle, float* d_vectors, float* d_results, float* h_vectors, float* h_results, int dimensions, int count) {
    cudaMemcpy(d_vectors, h_vectors, (size_t)count * dimensions * sizeof(float), cudaMemcpyHostToDevice);

    launch_l2_squared_kernel(d_vectors, d_results, dimensions, count, 0);

    cudaMemcpy(h_results, d_results, count * sizeof(float), cudaMemcpyDeviceToHost);

    return 0;
}

static inline void cuda_cleanup(CUDAIndexHandle* handle) {
    cuda_free(handle);
}


static inline int cuda_update_graph(CUDAIndexHandle* handle, uint32_t* h_offsets, uint32_t* h_neighbors, float* h_weights, int nodeCount, int edgeCount) {
    if (handle->graphOffsets) cudaFree(handle->graphOffsets);
    if (handle->graphNeighbors) cudaFree(handle->graphNeighbors);
    if (handle->graphWeights) cudaFree(handle->graphWeights);

    cudaMalloc((void**)&handle->graphOffsets, (nodeCount + 1) * sizeof(uint32_t));
    cudaMalloc((void**)&handle->graphNeighbors, edgeCount * sizeof(uint32_t));
    if (h_weights) cudaMalloc((void**)&handle->graphWeights, edgeCount * sizeof(float));

    cudaMemcpy(handle->graphOffsets, h_offsets, (nodeCount + 1) * sizeof(uint32_t), cudaMemcpyHostToDevice);
    cudaMemcpy(handle->graphNeighbors, h_neighbors, edgeCount * sizeof(uint32_t), cudaMemcpyHostToDevice);
    if (h_weights) cudaMemcpy(handle->graphWeights, h_weights, edgeCount * sizeof(float), cudaMemcpyHostToDevice);

    handle->graphNodeCount = nodeCount;
    handle->graphEdgeCount = edgeCount;
    return 0;
}

static inline int cuda_prune_neighbors(CUDAIndexHandle* handle,
    uint32_t* d_candIds, float* d_candDists, uint32_t* d_selIds, uint32_t* d_selCount,
    const float** d_pagePtrs, const int* d_pageStarts,
    uint32_t* h_selectedIds, uint32_t* h_selectedCount,
    int maxNeighbors, int numCandidates, int dim,
    int total_count, int num_pages, bool extended) {
    if (!handle) return -1;

    launch_hnsw_prune_neighbors_kernel(d_candIds, d_candDists, d_selIds, d_selCount,
        d_pagePtrs, d_pageStarts, maxNeighbors, numCandidates, dim, total_count, num_pages,
        extended, handle->streams[0]);

    uint32_t h_selCount;
    cudaMemcpy(&h_selCount, d_selCount, sizeof(uint32_t), cudaMemcpyDeviceToHost);
    *h_selectedCount = h_selCount;
    cudaMemcpy(h_selectedIds, d_selIds, (size_t)h_selCount * sizeof(uint32_t), cudaMemcpyDeviceToHost);

    return 0;
}

#endif // CUDA_KERNELS_DECL_H
