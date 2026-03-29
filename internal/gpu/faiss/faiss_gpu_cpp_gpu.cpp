#include "faiss_gpu_cpp.h"

#ifdef USE_GPU
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/MetricType.h>
#include <cuda_runtime.h>
#include <vector>
#include <string>
#include <cstring>
#include <stdexcept>

extern "C" {

static int faiss_last_error_code = 0;
static char faiss_last_error_msg[1024];

int lb_faiss_get_last_error_code() {
    return faiss_last_error_code;
}

const char* lb_faiss_get_last_error_msg() {
    return faiss_last_error_msg;
}

// GPU Resources
FaissGpuResourcesPtr lb_faiss_gpu_resources_new(int device) {
    cudaSetDevice(device);
    faiss::gpu::StandardGpuResources* res = new faiss::gpu::StandardGpuResources();
    return (FaissGpuResourcesPtr)res;
}

void lb_faiss_gpu_resources_free(FaissGpuResourcesPtr ptr) {
    if (ptr) {
        faiss::gpu::StandardGpuResources* res = (faiss::gpu::StandardGpuResources*)ptr;
        delete res;
    }
}

// Flat L2 Index
FaissGpuIndexFlatPtr lb_faiss_gpu_index_flat_l2_new(FaissGpuResourcesPtr res, int dim) {
    try {
        faiss::gpu::StandardGpuResources* resources = (faiss::gpu::StandardGpuResources*)res;
        
        // Create CPU index first
        faiss::IndexFlatL2* cpu_index = new faiss::IndexFlatL2(dim);
        
        faiss::gpu::GpuIndexFlatConfig config;
        
        // Wrap in GPU index
        faiss::gpu::GpuIndexFlatL2* index = new faiss::gpu::GpuIndexFlatL2(resources, cpu_index, config);
        return (FaissGpuIndexFlatPtr)index;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return NULL;
    }
}

void lb_faiss_gpu_index_flat_l2_free(FaissGpuIndexFlatPtr ptr) {
    if (ptr) {
        faiss::gpu::GpuIndexFlatL2* index = (faiss::gpu::GpuIndexFlatL2*)ptr;
        delete index;
    }
}

int lb_faiss_gpu_index_flat_l2_add(FaissGpuIndexFlatPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexFlatL2* index = (faiss::gpu::GpuIndexFlatL2*)ptr;
        index->add(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int lb_faiss_gpu_index_flat_l2_search(FaissGpuIndexFlatPtr ptr, int64_t n, float* queries, int k, float* distances, int64_t* labels) {
    try {
        faiss::gpu::GpuIndexFlatL2* index = (faiss::gpu::GpuIndexFlatL2*)ptr;
        index->search(n, queries, k, distances, labels);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int lb_faiss_gpu_index_flat_l2_ntotal(FaissGpuIndexFlatPtr ptr) {
    faiss::gpu::GpuIndexFlatL2* index = (faiss::gpu::GpuIndexFlatL2*)ptr;
    return index->ntotal;
}

// IVF Index
FaissGpuIndexIVFPtr lb_faiss_gpu_index_ivf_flat_new(FaissGpuResourcesPtr res, int dim, int nlist) {
    try {
        faiss::gpu::StandardGpuResources* resources = (faiss::gpu::StandardGpuResources*)res;
        
        // Create CPU index first
        faiss::IndexIVFFlat* cpu_index = new faiss::IndexIVFFlat(new faiss::IndexFlatL2(dim), dim, nlist, faiss::METRIC_L2);
        
        faiss::gpu::GpuIndexIVFFlatConfig config;
        
        // Wrap in GPU index
        faiss::gpu::GpuIndexIVFFlat* index = new faiss::gpu::GpuIndexIVFFlat(resources, cpu_index, config);
        return (FaissGpuIndexIVFPtr)index;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return NULL;
    }
}

void lb_faiss_gpu_index_ivf_flat_free(FaissGpuIndexIVFPtr ptr) {
    if (ptr) {
        faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
        delete index;
    }
}

int lb_faiss_gpu_index_ivf_flat_train(FaissGpuIndexIVFPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
        index->train(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int lb_faiss_gpu_index_ivf_flat_add(FaissGpuIndexIVFPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
        index->add(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int lb_faiss_gpu_index_ivf_flat_search(FaissGpuIndexIVFPtr ptr, int64_t n, float* queries, int k, float* distances, int64_t* labels) {
    try {
        faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
        index->search(n, queries, k, distances, labels);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int lb_faiss_gpu_index_ivf_flat_set_nprobe(FaissGpuIndexIVFPtr ptr, int nprobe) {
    faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
    index->nprobe = nprobe;
    return 0;
}

// IVF-PQ Index
FaissGpuIndexIVFPQPtr lb_faiss_gpu_index_ivf_pq_new(FaissGpuResourcesPtr res, int dim, int nlist, int m, int nbits_per_idx) {
    try {
        faiss::gpu::StandardGpuResources* resources = (faiss::gpu::StandardGpuResources*)res;
        
        // Create CPU index first
        faiss::IndexIVFPQ* cpu_index = new faiss::IndexIVFPQ(new faiss::IndexFlatL2(dim), dim, nlist, m, nbits_per_idx);
        
        faiss::gpu::GpuIndexIVFPQConfig config;
        
        faiss::gpu::GpuIndexIVFPQ* index = new faiss::gpu::GpuIndexIVFPQ(resources, cpu_index, config);
        return (FaissGpuIndexIVFPQPtr)index;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return NULL;
    }
}

void lb_faiss_gpu_index_ivf_pq_free(FaissGpuIndexIVFPQPtr ptr) {
    if (ptr) {
        faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
        delete index;
    }
}

int lb_faiss_gpu_index_ivf_pq_train(FaissGpuIndexIVFPQPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
        index->train(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int lb_faiss_gpu_index_ivf_pq_add(FaissGpuIndexIVFPQPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
        index->add(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int lb_faiss_gpu_index_ivf_pq_search(FaissGpuIndexIVFPQPtr ptr, int64_t n, float* queries, int k, float* distances, int64_t* labels) {
    try {
        faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
        index->search(n, queries, k, distances, labels);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int lb_faiss_gpu_index_ivf_pq_set_nprobe(FaissGpuIndexIVFPQPtr ptr, int nprobe) {
    faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
    index->nprobe = nprobe;
    return 0;
}

} // extern "C"
#endif // USE_GPU
