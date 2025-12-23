//go:build gpu && linux

package gpu

/*
#cgo LDFLAGS: -lfaiss_gpu -lcudart -lcublas
#include <stdlib.h>
#include <stdint.h>

// Forward declarations for FAISS GPU functions
// These would normally come from faiss/gpu/GpuIndexFlat.h
typedef void* FaissGpuResourcesPtr;
typedef void* FaissGpuIndexFlatL2Ptr;

// Placeholder function signatures - actual implementation requires FAISS headers
extern FaissGpuResourcesPtr faiss_gpu_resources_new(int device);
extern void faiss_gpu_resources_free(FaissGpuResourcesPtr res);
extern FaissGpuIndexFlatL2Ptr faiss_gpu_index_flat_l2_new(FaissGpuResourcesPtr res, int dim);
extern void faiss_gpu_index_flat_l2_free(FaissGpuIndexFlatL2Ptr idx);
extern int faiss_gpu_index_add(FaissGpuIndexFlatL2Ptr idx, int64_t n, float* vectors);
extern int faiss_gpu_index_search(FaissGpuIndexFlatL2Ptr idx, int64_t n, float* queries, int k, float* distances, int64_t* labels);
*/
import "C"
import (
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

// FaissGPUIndex wraps FAISS GPU index for vector search
type FaissGPUIndex struct {
	dim       int
	deviceID  int
	resources C.FaissGpuResourcesPtr
	index     C.FaissGpuIndexFlatL2Ptr
	mu        sync.RWMutex
	closed    bool
}

// NewFaissGPUIndex creates a new GPU-accelerated index using FAISS
func NewFaissGPUIndex(cfg GPUConfig) (Index, error) {
	if cfg.Dimension <= 0 {
		return nil, fmt.Errorf("dimension must be positive, got %d", cfg.Dimension)
	}

	idx := &FaissGPUIndex{
		dim:      cfg.Dimension,
		deviceID: cfg.DeviceID,
	}

	// Initialize GPU resources
	idx.resources = C.faiss_gpu_resources_new(C.int(cfg.DeviceID))
	if idx.resources == nil {
		return nil, fmt.Errorf("failed to initialize GPU resources for device %d", cfg.DeviceID)
	}

	// Create FAISS GPU index
	idx.index = C.faiss_gpu_index_flat_l2_new(idx.resources, C.int(cfg.Dimension))
	if idx.index == nil {
		C.faiss_gpu_resources_free(idx.resources)
		return nil, fmt.Errorf("failed to create GPU index")
	}

	// Set finalizer to ensure cleanup
	runtime.SetFinalizer(idx, (*FaissGPUIndex).Close)

	return idx, nil
}

// Add adds vectors to the GPU index
func (idx *FaissGPUIndex) Add(ids []int64, vectors []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	if len(vectors)%idx.dim != 0 {
		return fmt.Errorf("vector data length %d not divisible by dimension %d", len(vectors), idx.dim)
	}

	n := len(vectors) / idx.dim
	if len(ids) != n {
		return fmt.Errorf("id count %d does not match vector count %d", len(ids), n)
	}

	// Add vectors to GPU index
	ret := C.faiss_gpu_index_add(
		idx.index,
		C.int64_t(n),
		(*C.float)(unsafe.Pointer(&vectors[0])),
	)

	if ret != 0 {
		return fmt.Errorf("GPU index add failed with code %d", ret)
	}

	return nil
}

// Search queries the GPU index for k-nearest neighbors
func (idx *FaissGPUIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	// Allocate result buffers
	distances := make([]float32, k)
	labels := make([]int64, k)

	// Perform GPU search
	start := time.Now()
	ret := C.faiss_gpu_index_search(
		idx.index,
		1, // single query
		(*C.float)(unsafe.Pointer(&vector[0])),
		C.int(k),
		(*C.float)(unsafe.Pointer(&distances[0])),
		(*C.int64_t)(unsafe.Pointer(&labels[0])),
	)

	if ret != 0 {
		metrics.VectorSearchGPUOperationsTotal.WithLabelValues("search", "error").Inc()
		return nil, nil, fmt.Errorf("GPU search failed with code %d", ret)
	}

	duration := time.Since(start).Seconds()
	metrics.VectorSearchGPULatencySeconds.WithLabelValues("search").Observe(duration)
	metrics.VectorSearchGPUOperationsTotal.WithLabelValues("search", "success").Inc()

	return labels, distances, nil
}

// Close releases GPU resources
func (idx *FaissGPUIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.index != nil {
		C.faiss_gpu_index_flat_l2_free(idx.index)
		idx.index = nil
	}

	if idx.resources != nil {
		C.faiss_gpu_resources_free(idx.resources)
		idx.resources = nil
	}

	idx.closed = true
	return nil
}
