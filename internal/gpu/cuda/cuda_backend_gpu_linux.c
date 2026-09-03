//go:build gpu && linux

#include "cuda_backend_linux.h" // IWYU pragma: keep

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

int lb_cuda_host_alloc(void** ptr, size_t size, unsigned int flags) {
    cudaError_t err = cudaHostAlloc(ptr, size, flags);
    return (int)err;
}

int lb_cuda_free_host(void* ptr) {
    cudaError_t err = cudaFreeHost(ptr);
    return (int)err;
}

int lb_cuda_memcpy_async(void* dst, const void* src, size_t count, int kind, void* stream) {
    cudaError_t err = cudaMemcpyAsync(dst, src, count, (enum cudaMemcpyKind)kind, (cudaStream_t)stream);
    return (int)err;
}

int lb_cuda_stream_synchronize(void* stream) {
    cudaError_t err = cudaStreamSynchronize((cudaStream_t)stream);
    return (int)err;
}
#endif
