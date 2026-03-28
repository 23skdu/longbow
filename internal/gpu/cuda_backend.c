#include "cuda_backend.h"

#ifdef USE_GPU
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <string.h>

// CUDA initialization functions
int lb_cuda_init_device(int device) {
... (rest of file) ...
}
#endif
