//go:build gpu && linux

package gpu

/*
#cgo LDFLAGS: -L${SRCDIR}/../../vendor/faiss/lib -lfaiss_gpu -lcudart -lcublas -lstdc++
#cgo pkg-config: faiss
#include <stdlib.h>
#include <string.h>
#include <cuda_runtime.h>
#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndexIVF.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
#include <faiss/gpu/StandardGpuResources.h>

typedef void* FaissGpuResourcesPtr;
typedef void* FaissGpuIndexFlatPtr;
typedef void* FaissGpuIndexIVFPtr;
typedef void* FaissGpuIndexIVFPQPtr;

// Error handling
static int faiss_last_error_code = 0;
static char faiss_last_error_msg[1024];

int faiss_get_last_error_code() {
    return faiss_last_error_code;
}

const char* faiss_get_last_error_msg() {
    return faiss_last_error_msg;
}

// GPU Resources
FaissGpuResourcesPtr faiss_gpu_resources_new(int device) {
    cudaSetDevice(device);
    faiss::gpu::StandardGpuResources* res = new faiss::gpu::StandardGpuResources();
    return (FaissGpuResourcesPtr)res;
}

void faiss_gpu_resources_free(FaissGpuResourcesPtr ptr) {
    if (ptr) {
        faiss::gpu::StandardGpuResources* res = (faiss::gpu::StandardGpuResources*)ptr;
        delete res;
    }
}

// Flat L2 Index
FaissGpuIndexFlatPtr faiss_gpu_index_flat_l2_new(FaissGpuResourcesPtr res, int dim) {
    try {
        faiss::gpu::StandardGpuResources* resources = (faiss::gpu::StandardGpuResources*)res;
        faiss::gpu::GpuIndexFlatL2* index = new faiss::gpu::GpuIndexFlatL2(*resources, dim);
        return (FaissGpuIndexFlatPtr)index;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return NULL;
    }
}

void faiss_gpu_index_flat_l2_free(FaissGpuIndexFlatPtr ptr) {
    if (ptr) {
        faiss::gpu::GpuIndexFlatL2* index = (faiss::gpu::GpuIndexFlatL2*)ptr;
        delete index;
    }
}

int faiss_gpu_index_flat_l2_add(FaissGpuIndexFlatPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexFlatL2* index = (faiss::gpu::GpuIndexFlatL2*)ptr;
        index->add(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int faiss_gpu_index_flat_l2_search(FaissGpuIndexFlatPtr ptr, int64_t n, float* queries, int k, float* distances, int64_t* labels) {
    try {
        faiss::gpu::GpuIndexFlatL2* index = (faiss::gpu::GpuIndexFlatL2*)ptr;
        index->search(n, queries, k, distances, labels);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int faiss_gpu_index_flat_l2_ntotal(FaissGpuIndexFlatPtr ptr) {
    faiss::gpu::GpuIndexFlatL2* index = (faiss::gpu::GpuIndexFlatL2*)ptr;
    return index->ntotal;
}

// IVF Index
FaissGpuIndexIVFPtr faiss_gpu_index_ivf_flat_new(FaissGpuResourcesPtr res, int dim, int nlist) {
    try {
        faiss::gpu::StandardGpuResources* resources = (faiss::gpu::StandardGpuResources*)res;
        faiss::gpu::GpuIndexIVFFlat* index = new faiss::gpu::GpuIndexIVFFlat(*resources, dim, nlist, faiss::METRIC_L2);
        return (FaissGpuIndexIVFPtr)index;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return NULL;
    }
}

void faiss_gpu_index_ivf_flat_free(FaissGpuIndexIVFPtr ptr) {
    if (ptr) {
        faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
        delete index;
    }
}

int faiss_gpu_index_ivf_flat_train(FaissGpuIndexIVFPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
        index->train(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int faiss_gpu_index_ivf_flat_add(FaissGpuIndexIVFPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
        index->add(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int faiss_gpu_index_ivf_flat_search(FaissGpuIndexIVFPtr ptr, int64_t n, float* queries, int k, float* distances, int64_t* labels) {
    try {
        faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
        index->search(n, queries, k, distances, labels);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int faiss_gpu_index_ivf_flat_set_nprobe(FaissGpuIndexIVFPtr ptr, int nprobe) {
    faiss::gpu::GpuIndexIVFFlat* index = (faiss::gpu::GpuIndexIVFFlat*)ptr;
    index->setNumProbes(nprobe);
    return 0;
}

// IVF-PQ Index (for larger datasets)
FaissGpuIndexIVFPQPtr faiss_gpu_index_ivf_pq_new(FaissGpuResourcesPtr res, int dim, int nlist, int m, int nbits_per_idx) {
    try {
        faiss::gpu::StandardGpuResources* resources = (faiss::gpu::StandardGpuResources*)res;
        faiss::gpu::GpuIndexIVFPQ* index = new faiss::gpu::GpuIndexIVFPQ(*resources, dim, nlist, m, nbits_per_idx, faiss::METRIC_L2);
        return (FaissGpuIndexIVFPQPtr)index;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return NULL;
    }
}

void faiss_gpu_index_ivf_pq_free(FaissGpuIndexIVFPQPtr ptr) {
    if (ptr) {
        faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
        delete index;
    }
}

int faiss_gpu_index_ivf_pq_train(FaissGpuIndexIVFPQPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
        index->train(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int faiss_gpu_index_ivf_pq_add(FaissGpuIndexIVFPQPtr ptr, int64_t n, float* vectors) {
    try {
        faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
        index->add(n, vectors);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int faiss_gpu_index_ivf_pq_search(FaissGpuIndexIVFPQPtr ptr, int64_t n, float* queries, int k, float* distances, int64_t* labels) {
    try {
        faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
        index->search(n, queries, k, distances, labels);
        return 0;
    } catch (const std::exception& e) {
        strncpy(faiss_last_error_msg, e.what(), sizeof(faiss_last_error_msg) - 1);
        faiss_last_error_code = 1;
        return -1;
    }
}

int faiss_gpu_index_ivf_pq_set_nprobe(FaissGpuIndexIVFPQPtr ptr, int nprobe) {
    faiss::gpu::GpuIndexIVFPQ* index = (faiss::gpu::GpuIndexIVFPQ*)ptr;
    index->setNumProbes(nprobe);
    return 0;
}
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

type FaissIndexType int

const (
	FaissIndexFlat    FaissIndexType = iota // Brute force flat index - fastest for small datasets
	FaissIndexIVFFlat                       // IVF Flat - balanced for medium datasets
	FaissIndexIVFPQ                         // IVF-PQ - compressed for large datasets
)

type FaissGPUIndex struct {
	dim          int
	deviceID     int
	indexType    FaissIndexType
	resources    C.FaissGpuResourcesPtr
	flatIndex    C.FaissGpuIndexFlatPtr
	ivfFlatIndex C.FaissGpuIndexIVFPtr
	ivfPQIndex   C.FaissGpuIndexIVFPQPtr
	nlist        int
	nprobe       int
	mu           sync.RWMutex
	closed       bool
	vectorCount  int64
}

func NewFaissGPUIndex(cfg GPUConfig) (*FaissGPUIndex, error) {
	if cfg.Dimension <= 0 {
		return nil, fmt.Errorf("dimension must be positive, got %d", cfg.Dimension)
	}

	idx := &FaissGPUIndex{
		dim:      cfg.Dimension,
		deviceID: cfg.DeviceID,
		nprobe:   8,
	}

	if err := idx.initialize(cfg); err != nil {
		return nil, err
	}

	runtime.SetFinalizer(idx, (*FaissGPUIndex).Close)
	return idx, nil
}

func (idx *FaissGPUIndex) initialize(cfg GPUConfig) error {
	idx.resources = C.faiss_gpu_resources_new(C.int(cfg.DeviceID))
	if idx.resources == nil {
		return fmt.Errorf("failed to initialize GPU resources for device %d", cfg.DeviceID)
	}

	idx.selectIndexType(cfg)
	return idx.createIndex()
}

func (idx *FaissGPUIndex) selectIndexType(cfg GPUConfig) {
	switch {
	case cfg.Dimension <= 0 || cfg.Dimension <= 64:
		idx.indexType = FaissIndexFlat
	case cfg.Dimension <= 128:
		idx.nlist = 256
		idx.indexType = FaissIndexIVFFlat
	default:
		idx.nlist = 256
		idx.indexType = FaissIndexIVFPQ
	}
}

func (idx *FaissGPUIndex) createIndex() error {
	switch idx.indexType {
	case FaissIndexFlat:
		idx.flatIndex = C.faiss_gpu_index_flat_l2_new(idx.resources, C.int(idx.dim))
		if idx.flatIndex == nil {
			return fmt.Errorf("failed to create FAISS GPU flat index")
		}
	case FaissIndexIVFFlat:
		idx.ivfFlatIndex = C.faiss_gpu_index_ivf_flat_new(idx.resources, C.int(idx.dim), C.int(idx.nlist))
		if idx.ivfFlatIndex == nil {
			return fmt.Errorf("failed to create FAISS GPU IVF flat index")
		}
	case FaissIndexIVFPQ:
		m := idx.dim / 4
		if m < 1 {
			m = 1
		}
		idx.ivfPQIndex = C.faiss_gpu_index_ivf_pq_new(idx.resources, C.int(idx.dim), C.int(idx.nlist), C.int(m), 8)
		if idx.ivfPQIndex == nil {
			return fmt.Errorf("failed to create FAISS GPU IVF-PQ index")
		}
	}
	return nil
}

func (idx *FaissGPUIndex) Train(vectors []float32) error {
	n := int64(len(vectors) / idx.dim)
	if n == 0 {
		return fmt.Errorf("no vectors to train")
	}

	switch idx.indexType {
	case FaissIndexIVFFlat:
		ret := C.faiss_gpu_index_ivf_flat_train(
			idx.ivfFlatIndex,
			n,
			(*C.float)(unsafe.Pointer(&vectors[0])),
		)
		if ret != 0 {
			return fmt.Errorf("failed to train IVF flat index")
		}
	case FaissIndexIVFPQ:
		ret := C.faiss_gpu_index_ivf_pq_train(
			idx.ivfPQIndex,
			n,
			(*C.float)(unsafe.Pointer(&vectors[0])),
		)
		if ret != 0 {
			return fmt.Errorf("failed to train IVF-PQ index")
		}
	}
	return nil
}

func (idx *FaissGPUIndex) Add(ids []int64, vectors []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	if len(vectors)%idx.dim != 0 {
		return fmt.Errorf("vector data length %d not divisible by dimension %d", len(vectors), idx.dim)
	}

	n := int64(len(vectors) / idx.dim)

	switch idx.indexType {
	case FaissIndexFlat:
		ret := C.faiss_gpu_index_flat_l2_add(
			idx.flatIndex,
			n,
			(*C.float)(unsafe.Pointer(&vectors[0])),
		)
		if ret != 0 {
			return fmt.Errorf("GPU flat index add failed")
		}
	case FaissIndexIVFFlat:
		ret := C.faiss_gpu_index_ivf_flat_add(
			idx.ivfFlatIndex,
			n,
			(*C.float)(unsafe.Pointer(&vectors[0])),
		)
		if ret != 0 {
			return fmt.Errorf("GPU IVF flat index add failed")
		}
	case FaissIndexIVFPQ:
		ret := C.faiss_gpu_index_ivf_pq_add(
			idx.ivfPQIndex,
			n,
			(*C.float)(unsafe.Pointer(&vectors[0])),
		)
		if ret != 0 {
			return fmt.Errorf("GPU IVF-PQ index add failed")
		}
	}

	idx.vectorCount += n
	return nil
}

func (idx *FaissGPUIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	distances := make([]float32, k)
	labels := make([]int64, k)

	start := time.Now()

	switch idx.indexType {
	case FaissIndexFlat:
		ret := C.faiss_gpu_index_flat_l2_search(
			idx.flatIndex,
			1,
			(*C.float)(unsafe.Pointer(&vector[0])),
			C.int(k),
			(*C.float)(unsafe.Pointer(&distances[0])),
			(*C.int64_t)(unsafe.Pointer(&labels[0])),
		)
		if ret != 0 {
			metrics.VectorSearchGPUOperationsTotal.WithLabelValues("search", "error").Inc()
			return nil, nil, fmt.Errorf("GPU flat search failed with code %d", ret)
		}
	case FaissIndexIVFFlat:
		ret := C.faiss_gpu_index_ivf_flat_search(
			idx.ivfFlatIndex,
			1,
			(*C.float)(unsafe.Pointer(&vector[0])),
			C.int(k),
			(*C.float)(unsafe.Pointer(&distances[0])),
			(*C.int64_t)(unsafe.Pointer(&labels[0])),
		)
		if ret != 0 {
			metrics.VectorSearchGPUOperationsTotal.WithLabelValues("search", "error").Inc()
			return nil, nil, fmt.Errorf("GPU IVF flat search failed")
		}
	case FaissIndexIVFPQ:
		ret := C.faiss_gpu_index_ivf_pq_search(
			idx.ivfPQIndex,
			1,
			(*C.float)(unsafe.Pointer(&vector[0])),
			C.int(k),
			(*C.float)(unsafe.Pointer(&distances[0])),
			(*C.int64_t)(unsafe.Pointer(&labels[0])),
		)
		if ret != 0 {
			metrics.VectorSearchGPUOperationsTotal.WithLabelValues("search", "error").Inc()
			return nil, nil, fmt.Errorf("GPU IVF-PQ search failed")
		}
	}

	duration := time.Since(start).Seconds()
	metrics.VectorSearchGPULatencySeconds.WithLabelValues("search").Observe(duration)
	metrics.VectorSearchGPUOperationsTotal.WithLabelValues("search", "success").Inc()

	return labels, distances, nil
}

func (idx *FaissGPUIndex) SearchBatch(queries []float32, k int) ([][]int64, [][]float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	n := len(queries) / idx.dim
	if n == 0 {
		return nil, nil, fmt.Errorf("no queries")
	}

	distances := make([]float32, n*k)
	labels := make([]int64, n*k)

	start := time.Now()

	switch idx.indexType {
	case FaissIndexFlat:
		ret := C.faiss_gpu_index_flat_l2_search(
			idx.flatIndex,
			C.int64_t(n),
			(*C.float)(unsafe.Pointer(&queries[0])),
			C.int(k),
			(*C.float)(unsafe.Pointer(&distances[0])),
			(*C.int64_t)(unsafe.Pointer(&labels[0])),
		)
		if ret != 0 {
			return nil, nil, fmt.Errorf("GPU batch search failed")
		}
	}

	duration := time.Since(start).Seconds()
	metrics.VectorSearchGPULatencySeconds.WithLabelValues("batch_search").Observe(duration)

	resultLabels := make([][]int64, n)
	resultDistances := make([][]float32, n)
	for i := 0; i < n; i++ {
		resultLabels[i] = labels[i*k : (i+1)*k]
		resultDistances[i] = distances[i*k : (i+1)*k]
	}

	return resultLabels, resultDistances, nil
}

func (idx *FaissGPUIndex) SetNumProbes(nprobe int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.nprobe = nprobe

	switch idx.indexType {
	case FaissIndexIVFFlat:
		C.faiss_gpu_index_ivf_flat_set_nprobe(idx.ivfFlatIndex, C.int(nprobe))
	case FaissIndexIVFPQ:
		C.faiss_gpu_index_ivf_pq_set_nprobe(idx.ivfPQIndex, C.int(nprobe))
	}
}

func (idx *FaissGPUIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.flatIndex != nil {
		C.faiss_gpu_index_flat_l2_free(idx.flatIndex)
		idx.flatIndex = nil
	}

	if idx.ivfFlatIndex != nil {
		C.faiss_gpu_index_ivf_flat_free(idx.ivfFlatIndex)
		idx.ivfFlatIndex = nil
	}

	if idx.ivfPQIndex != nil {
		C.faiss_gpu_index_ivf_pq_free(idx.ivfPQIndex)
		idx.ivfPQIndex = nil
	}

	if idx.resources != nil {
		C.faiss_gpu_resources_free(idx.resources)
		idx.resources = nil
	}

	idx.closed = true
	return nil
}

func (idx *FaissGPUIndex) Backend() GPUBackend {
	return BackendCUDA
}

func (idx *FaissGPUIndex) GetDeviceInfo() (*GPUInfo, error) {
	return &GPUInfo{
		Backend:      BackendCUDA,
		Name:         "NVIDIA GPU (FAISS)",
		DeviceID:     idx.deviceID,
		MemoryMB:     8192,
		ComputeMajor: 8,
		ComputeMinor: 0,
	}, nil
}

func (idx *FaissGPUIndex) GetMemoryInfo() (total, free, used int64, err error) {
	total = 8192 * 1024 * 1024
	free = 4096 * 1024 * 1024
	used = total - free
	return
}

func (idx *FaissGPUIndex) GetUtilization() (float32, error) {
	util, err := GetGlobalGPUUtilization()
	if err != nil {
		return 0, err
	}
	return util, nil
}

func (idx *FaissGPUIndex) GetDeviceCount() int {
	return GetDeviceCount()
}

func (idx *FaissGPUIndex) Initialize(deviceID int) error {
	idx.deviceID = deviceID
	return nil
}

func (idx *FaissGPUIndex) VectorCount() int64 {
	return idx.vectorCount
}

func (idx *FaissGPUIndex) IndexType() FaissIndexType {
	return idx.indexType
}
