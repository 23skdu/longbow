#ifndef CUDA_BACKEND_H
#define CUDA_BACKEND_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// CUDA initialization functions
int cudaInitDevice(int device);
int cudaGetDeviceCountWrap(int* count);
int cudaGetDevicePropertiesWrap(int device, char* name, size_t nameLen,
                                 int* major, int* minor, size_t* totalMem);
int cudaGetMemInfo(size_t* free, size_t* total);

#ifdef __cplusplus
}
#endif

#endif
