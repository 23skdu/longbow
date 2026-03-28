#ifndef CUDA_BACKEND_H
#define CUDA_BACKEND_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <cuda_runtime.h>

#ifdef __cplusplus
extern "C" {
#endif

// CUDA initialization functions
int lb_cuda_init_device(int device);
int lb_cuda_get_device_count(int* count);
int lb_cuda_get_device_properties(int device, char* name, size_t nameLen,
                                 int* major, int* minor, size_t* totalMem);
int lb_cuda_get_mem_info(size_t* free, size_t* total);

#ifdef __cplusplus
}
#endif

#endif
