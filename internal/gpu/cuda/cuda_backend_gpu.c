#include "cuda_backend.h"

#ifdef USE_GPU
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <string.h>

// CUDA initialization functions
int lb_cuda_init_device(int device) {
    cudaError_t err = cudaSetDevice(device);
    if (err != cudaSuccess) {
        return (int)err;
    }
    
    // Check if we can allocate memory to verify the initialization
    void* ptr = NULL;
    err = cudaMalloc(&ptr, 1);
    if (err == cudaSuccess) {
        cudaFree(ptr);
    }
    
    return (int)err;
}

int lb_cuda_get_device_count(int* count) {
    cudaError_t err = cudaGetDeviceCount(count);
    return (int)err;
}

int lb_cuda_get_device_properties(int device, char* name, size_t nameLen, 
                                 int* major, int* minor, size_t* totalMem) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device);
    if (err != cudaSuccess) {
        return (int)err;
    }

    strncpy(name, prop.name, nameLen - 1);
    name[nameLen - 1] = '\0';
    *major = prop.major;
    *minor = prop.minor;
    *totalMem = prop.totalGlobalMem;
    
    return 0;
}

int lb_cuda_get_mem_info(size_t* free, size_t* total) {
    cudaError_t err = cudaMemGetInfo(free, total);
    return (int)err;
}
#endif
