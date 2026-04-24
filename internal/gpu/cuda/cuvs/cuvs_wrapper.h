#ifndef CUVS_WRAPPER_H
#define CUVS_WRAPPER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    void* handle;
} cuvs_resources_t;

int cuvs_init(cuvs_resources_t* res);
int cuvs_search(cuvs_resources_t* res, const float* query, int k, char** ids, float* distances);
int cuvs_index_build(cuvs_resources_t* res, const float* vectors, int n, int dim);

#ifdef __cplusplus
}
#endif

#endif // CUVS_WRAPPER_H
