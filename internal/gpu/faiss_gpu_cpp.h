#ifndef FAISS_GPU_CPP_H
#define FAISS_GPU_CPP_H

#ifdef USE_GPU

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* FaissGpuResourcesPtr;
typedef void* FaissGpuIndexFlatPtr;
typedef void* FaissGpuIndexIVFPtr;
typedef void* FaissGpuIndexIVFPQPtr;

// Error handling
int lb_faiss_get_last_error_code();
const char* lb_faiss_get_last_error_msg();

// GPU Resources
FaissGpuResourcesPtr lb_faiss_gpu_resources_new(int device);
void lb_faiss_gpu_resources_free(FaissGpuResourcesPtr ptr);

// Flat L2 Index
FaissGpuIndexFlatPtr lb_faiss_gpu_index_flat_l2_new(FaissGpuResourcesPtr res, int dim);
void lb_faiss_gpu_index_flat_l2_free(FaissGpuIndexFlatPtr ptr);
int lb_faiss_gpu_index_flat_l2_add(FaissGpuIndexFlatPtr ptr, int64_t n, float* vectors);
int lb_faiss_gpu_index_flat_l2_search(FaissGpuIndexFlatPtr ptr, int64_t n, float* queries, int k, float* distances, int64_t* labels);
int lb_faiss_gpu_index_flat_l2_ntotal(FaissGpuIndexFlatPtr ptr);

// IVF Index
FaissGpuIndexIVFPtr lb_faiss_gpu_index_ivf_flat_new(FaissGpuResourcesPtr res, int dim, int nlist);
void lb_faiss_gpu_index_ivf_flat_free(FaissGpuIndexIVFPtr ptr);
int lb_faiss_gpu_index_ivf_flat_train(FaissGpuIndexIVFPtr ptr, int64_t n, float* vectors);
int lb_faiss_gpu_index_ivf_flat_add(FaissGpuIndexIVFPtr ptr, int64_t n, float* vectors);
int lb_faiss_gpu_index_ivf_flat_search(FaissGpuIndexIVFPtr ptr, int64_t n, float* queries, int k, float* distances, int64_t* labels);
int lb_faiss_gpu_index_ivf_flat_set_nprobe(FaissGpuIndexIVFPtr ptr, int nprobe);

// IVF-PQ Index
FaissGpuIndexIVFPQPtr lb_faiss_gpu_index_ivf_pq_new(FaissGpuResourcesPtr res, int dim, int nlist, int m, int nbits_per_idx);
void lb_faiss_gpu_index_ivf_pq_free(FaissGpuIndexIVFPQPtr ptr);
int lb_faiss_gpu_index_ivf_pq_train(FaissGpuIndexIVFPQPtr ptr, int64_t n, float* vectors);
int lb_faiss_gpu_index_ivf_pq_add(FaissGpuIndexIVFPQPtr ptr, int64_t n, float* vectors);
int lb_faiss_gpu_index_ivf_pq_search(FaissGpuIndexIVFPQPtr ptr, int64_t n, float* queries, int k, float* distances, int64_t* labels);
int lb_faiss_gpu_index_ivf_pq_set_nprobe(FaissGpuIndexIVFPQPtr ptr, int nprobe);

#ifdef __cplusplus
}
#endif

#endif // USE_GPU

#endif // FAISS_GPU_CPP_H
