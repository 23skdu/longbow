#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct {
    void* device;
    void* commandQueue;
    void* vectorBuffer;
    void* idBuffer;
    void* pqPipeline;
    void* assignPipeline;
    void* bfsExpandPipeline;
    void* actPropagatePipeline;
    void* fusedGraphPipeline;
    void* graphOffsets;
    void* graphNeighbors;
    void* graphWeights;
    void* l2DistancePipeline;
    void* ipDistancePipeline;
    void* quantizeSQ8Pipeline;
    void* sigmoidPipeline;
    void* haversinePipeline;
    void* normPipeline;
    void* sumCentroidsPipeline;
    void* finalizeCentroidsPipeline;
    void* prunePipeline;
    void* pruneCandIdBuffer;
    void* pruneCandDistBuffer;
    void* pruneSelIdBuffer;
    void* pruneSelCountBuffer;
    int pruneScratchCapacity;
    int vectorCount;
    int dimensions;
    int capacity;
    int graphNodeCount;
    int graphEdgeCount;
} MetalIndexHandle;

typedef struct MetalIndexOptimized MetalIndexOptimized;

typedef enum {
    METRIC_L2 = 0,
    METRIC_COSINE = 1,
    METRIC_DOT = 2
} DistanceMetric;

typedef enum {
    VECTOR_F32 = 0,
    VECTOR_F16 = 1,
    VECTOR_C64 = 2,
    VECTOR_C128 = 3
} VectorTypeGPU;

struct MetalIndexOptimized {
    void* device;
    void* commandQueue;
    void* vectorBuffer;
    void* idBuffer;
    void* distanceComputePipeline;
    void* distancePagedPipeline;
    void* cosinePipeline;
    void* dotPipeline;
    void* topKPipeline;
    void* l2Fp16Pipeline;
    void* cosineFp16Pipeline;
    void* dotFp16Pipeline;
    void* l2C64Pipeline;
    void* cosineC64Pipeline;
    void* l2C128Pipeline;
    void* cosineC128Pipeline;
    void* tqPipeline;
    void* pqPipeline;
    void* pqBuffer;
    void* haversinePipeline;
    void* normPipeline;
    void* prunePipeline;
    void* greedySearchPipeline;
    void* greedyTQSearchPipeline;
    void* graphOffsetsBuffer;
    void* graphNeighborsBuffer;
    void* graphWeightsBuffer;
    void* trigBuffer;
    void* queryBuffers[2];
    int currentBufferIdx;
    int vectorCount;
    int dimensions;
    int capacity;
    DistanceMetric metric;
};

typedef struct {
    void* device;
    void* commandQueue;
    void* vectorBuffer;
    void* distancePipeline;
    void* pqPipeline;
    void* batchPipeline;
    int vectorCount;
    int dimensions;
} MetalHybridIndex;

void* create_shared_buffer(void* device, size_t size);
void metal_get_device_info(MetalIndexHandle* handle, char* name, int maxLen, uint64_t* totalMem);
int metal_add_vectors(MetalIndexHandle* handle, float* vectors, int64_t* ids, int count);
int metal_compute_distances_gpu(MetalIndexHandle* handle, float* query, float* distances, int count, int type);
int metal_search(MetalIndexHandle* handle, float* query, int k, int64_t* resultIDs, float* resultDistances);
int metal_get_count(MetalIndexHandle* handle);
int metal_haversine_batch(MetalIndexHandle* handle, float* center, float* points, float* results, float earthRadius, int count);
int metal_norm_batch_f32(MetalIndexHandle* handle, float* vectors, float* results, int dims, int count);
void metal_cleanup(MetalIndexHandle* handle);
int metal_search_pq(MetalIndexHandle* handle, float* lookupTable, int m, int k, int64_t* resultIDs, float* resultDistances);
void* metal_get_vector_buffer(MetalIndexHandle* handle);
void* metal_get_id_buffer(MetalIndexHandle* handle);
int metal_assign_to_clusters(MetalIndexHandle* handle, float* vectors, float* centroids, uint32_t* assignments, int numVectors, int numCentroids, int dim);
int metal_train_kmeans(MetalIndexHandle* handle, float* data, float* centroids, int n, int dim, int k, int maxIter);
int metal_update_graph(MetalIndexHandle* handle, uint32_t* h_offsets, uint32_t* h_neighbors, float* h_weights, int nodeCount, int edgeCount);
int metal_graph_expand(MetalIndexHandle* handle, uint32_t* seeds, int numSeeds, int depth, float alpha, uint32_t* outIDs, float* outScores, int* outCount);
int metal_quantize_sq8(MetalIndexHandle* handle, float* vectors, float* mins, float* maxs, unsigned char* results, int count);
int metal_prune_neighbors(MetalIndexHandle* handle, uint32_t* candidateIds, float* candidateDists, uint32_t* selectedIds, uint32_t* selectedCount, float* allVectors, int maxNeighbors, int numCandidates, int dim, bool extended);

MetalIndexOptimized* metal_init_optimized(int dimensions);
void metal_set_pipelines_optimized(MetalIndexOptimized* handle, void* device, void* queue,
                                 void* l2, void* l2Paged, void* cosine, void* dot, void* topK,
                                 void* l2Fp16, void* cosineFp16, void* dotFp16,
                                 void* l2C128, void* cosineC128, void* l2C64, void* cosineC64,
                                 void* tq, void* haversine, void* norm, void* prune, void* greedy, void* greedyTQ);
int metal_greedy_search_optimized(MetalIndexOptimized* handle, float* query, uint32_t* entryPoint, float* entryDist);
int metal_greedy_search_tq_optimized(MetalIndexOptimized* handle, float* query, int pow2, int bitsPerAngle, uint32_t* entryPoint, float* entryDist);
int metal_update_graph_optimized(MetalIndexOptimized* handle, uint32_t* offsets, int numOffsets, uint32_t* neighbors, int numNeighbors, float* weights, int numWeights);
void metal_set_metric(MetalIndexOptimized* handle, DistanceMetric metric);
int metal_add_vectors_optimized(MetalIndexOptimized* handle, float* vectors, int64_t* ids, int count);
int metal_get_count_optimized(MetalIndexOptimized* handle);
int metal_search_optimized(MetalIndexOptimized* handle, float* query, void** page_buffers, int* page_starts, int num_pages, int totalVectors, int k, int64_t* resultIDs, float* resultDistances);
void metal_cleanup_optimized(MetalIndexOptimized* handle);
int metal_search_typed(MetalIndexOptimized* handle, void* query, int k, int64_t* resultIDs, float* resultDistances, VectorTypeGPU vtype);
int metal_add_tq_vectors_optimized(MetalIndexOptimized* handle, unsigned char* tqData, int stride, int64_t* ids, int count);
int metal_search_tq_optimized(MetalIndexOptimized* handle, float* query, int k, int pow2, int bitsPerAngle, int64_t* resultIDs, float* resultDistances);
int metal_add_pq(MetalIndexOptimized* handle, const unsigned char* codes, const int64_t* ids, int count, int m);
int metal_search_pq_optimized(MetalIndexOptimized* handle, const float* lookupTable, int m, int k, int64_t* resultIDs, float* resultDistances);
int metal_haversine_batch_optimized(MetalIndexOptimized* handle, float* center, float* points, float* results, float earthRadius, int count);
int metal_norm_batch_optimized(MetalIndexOptimized* handle, float* vectors, float* results, int dims, int count);
int metal_prune_neighbors_optimized(MetalIndexOptimized* handle, uint32_t* candidateIds, float* candidateDists, uint32_t* selectedIds, uint32_t* selectedCount, float* allVectors, int maxNeighbors, int numCandidates, int dim, bool extended);

MetalHybridIndex* metal_hybrid_init(int dimensions);
int metal_hybrid_add(MetalHybridIndex* handle, float* vectors, int count);
int metal_hybrid_compute_distances(MetalHybridIndex* handle, float* query, float* distances);
int metal_hybrid_compute_pq_distances(MetalHybridIndex* handle, float* lookupTable, int m, float* distances);
int metal_hybrid_compute_batch_distances(MetalHybridIndex* handle, float* query, uint32_t* candidateIDs, int numCandidates, float* distances);
int metal_hybrid_compute_batch_distances_async(MetalHybridIndex* handle, float* query, uint32_t* candidateIDs, int numCandidates, float* distances, uintptr_t callbackHandle);
void metal_hybrid_cleanup(MetalHybridIndex* handle);
void* metal_hybrid_get_vector_buffer(MetalHybridIndex* handle);
