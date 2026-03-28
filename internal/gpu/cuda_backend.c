#include "cuda_backend.h"
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <string.h>

// CUDA initialization functions
int cudaInitDevice(int device) {
    cudaError_t err = cudaSetDevice(device);
    if (err != cudaSuccess) {
        return -1;
    }

    err = cudaFree(0);  // Initialize context
    if (err != cudaSuccess) {
        return -2;
    }

    return 0;
}

int cudaGetDeviceCountWrap(int* count) {
    cudaError_t err = cudaGetDeviceCount(count);
    if (err != cudaSuccess) {
        return -1;
    }
    return 0;
}

int cudaGetDevicePropertiesWrap(int device, char* name, size_t nameLen,
                                 int* major, int* minor, size_t* totalMem) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, device);
    if (err != cudaSuccess) {
        return -1;
    }

    strncpy(name, prop.name, nameLen - 1);
    name[nameLen - 1] = '\0';
    *major = prop.major;
    *minor = prop.minor;
    *totalMem = prop.totalGlobalMem;

    return 0;
}

int cudaGetMemInfo(size_t* free, size_t* total) {
    cudaError_t err = cudaMemGetInfo(free, total);
    if (err != cudaSuccess) {
        return -1;
    }
    return 0;
}
