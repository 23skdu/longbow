package cuvs

/*
#cgo LDFLAGS: -L/usr/local/cuda/lib64 -lcuvs
#include "cuvs_wrapper.h"
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"
	"unsafe"
)

var (
	cuvsSearchLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_cuvs_search_latency_ms",
		Help:    "Latency of cuVS GPU search operations in milliseconds",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})
	cuvsSearchOps = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_cuvs_search_ops_total",
		Help: "Total number of cuVS GPU search operations",
	})
)

// CUVSIndex implements high-performance GPU search using NVIDIA cuVS
type CUVSIndex struct {
	dataset string
	dim     int
	res     C.cuvs_resources_t
}

func NewCUVSIndex(dataset string, dim int) (*CUVSIndex, error) {
	idx := &CUVSIndex{
		dataset: dataset,
		dim:     dim,
	}
	ret := C.cuvs_init(&idx.res)
	if ret != 0 {
		return nil, fmt.Errorf("failed to initialize cuVS resources: error %d", int(ret))
	}
	return idx, nil
}

func (idx *CUVSIndex) Search(ctx context.Context, query []float32, k int) ([]int64, []float32, error) {
	start := time.Now()
	cuvsSearchOps.Inc()
	defer func() {
		cuvsSearchLatency.Observe(float64(time.Since(start).Milliseconds()))
	}()

	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", idx.dim, len(query))
	}

	cDistances := make([]C.float, k)
	cIds := make([]*C.char, k)
	if k > 2147483647 {
		return nil, nil, fmt.Errorf("k too large")
	}
	ret := C.cuvs_search(&idx.res, (*C.float)(&query[0]), C.int(k), (**C.char)(unsafe.Pointer(&cIds[0])), (*C.float)(&cDistances[0])) // #nosec G115
	if ret != 0 {
		return nil, nil, fmt.Errorf("cuVS search failed: error %d", int(ret))
	}

	ids := make([]int64, k)
	distances := make([]float32, k)
	for i := 0; i < k; i++ {
		distances[i] = float32(cDistances[i])
		if cIds[i] != nil {
			// Convert C string to Go string, then to int64 (example logic)
			goId := C.GoString(cIds[i])
			// ... id mapping logic ...
			// For now, we'll assume the ID is encoded in the string or we have a map
			fmt.Sscanf(goId, "%d", &ids[i])
			C.free(unsafe.Pointer(cIds[i]))
		}
	}

	return ids, distances, nil
}

func (idx *CUVSIndex) AddBatch(ctx context.Context, ids []int64, vectors []float32) error {
	if len(vectors) == 0 {
		return nil
	}
	n := len(vectors) / idx.dim
	if n < 0 || n > 2147483647 || idx.dim < 0 || idx.dim > 2147483647 {
		return fmt.Errorf("n or dim too large or invalid")
	}
	ret := C.cuvs_index_build(&idx.res, (*C.float)(&vectors[0]), C.int(n), C.int(idx.dim)) // #nosec G115
	if ret != 0 {
		return fmt.Errorf("cuVS index build failed: error %d", int(ret))
	}
	return nil
}

func (idx *CUVSIndex) Close() error {
	// If cuvs_resources_t had a cleanup, we would call it here.
	return nil
}
