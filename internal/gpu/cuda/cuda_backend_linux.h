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

// Distance Kernels
void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, void* stream);
void launch_pq_distance_kernel(const float* lookupTable, const unsigned char* codes, float* distances, int m, int count, void* stream);


#ifdef __cplusplus
}
#endif

#endif
