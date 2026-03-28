//go:build gpu && linux

package gpu

/*
#cgo LDFLAGS: -lfaiss -lfaiss_gpu -lcudart -lcublas -lstdc++
#cgo pkg-config: faiss
#include "faiss_gpu_cpp.h"
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
		ret := C.lb_faiss_gpu_index_flat_l2_add(
			idx.flatIndex,
			n,
			(*C.float)(unsafe.Pointer(&vectors[0])),
		)
		if ret != 0 {
			return fmt.Errorf("GPU flat index add failed: %s", C.GoString(C.lb_faiss_get_last_error_msg()))
		}
	case FaissIndexIVFFlat:
		ret := C.lb_faiss_gpu_index_ivf_flat_add(
			idx.ivfFlatIndex,
			n,
			(*C.float)(unsafe.Pointer(&vectors[0])),
		)
		if ret != 0 {
			return fmt.Errorf("GPU IVF flat index add failed: %s", C.GoString(C.lb_faiss_get_last_error_msg()))
		}
	case FaissIndexIVFPQ:
		ret := C.lb_faiss_gpu_index_ivf_pq_add(
			idx.ivfPQIndex,
			n,
			(*C.float)(unsafe.Pointer(&vectors[0])),
		)
		if ret != 0 {
			return fmt.Errorf("GPU IVF-PQ index add failed: %s", C.GoString(C.lb_faiss_get_last_error_msg()))
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
		ret := C.lb_faiss_gpu_index_flat_l2_search(
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
		ret := C.lb_faiss_gpu_index_ivf_flat_search(
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
		ret := C.lb_faiss_gpu_index_ivf_pq_search(
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
		ret := C.lb_faiss_gpu_index_flat_l2_search(
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
		C.lb_faiss_gpu_index_ivf_flat_set_nprobe(idx.ivfFlatIndex, C.int(nprobe))
	case FaissIndexIVFPQ:
		C.lb_faiss_gpu_index_ivf_pq_set_nprobe(idx.ivfPQIndex, C.int(nprobe))
	}
}

func (idx *FaissGPUIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.flatIndex != nil {
		C.lb_faiss_gpu_index_flat_l2_free(idx.flatIndex)
		idx.flatIndex = nil
	}

	if idx.ivfFlatIndex != nil {
		C.lb_faiss_gpu_index_ivf_flat_free(idx.ivfFlatIndex)
		idx.ivfFlatIndex = nil
	}

	if idx.ivfPQIndex != nil {
		C.lb_faiss_gpu_index_ivf_pq_free(idx.ivfPQIndex)
		idx.ivfPQIndex = nil
	}

	if idx.resources != nil {
		C.lb_faiss_gpu_resources_free(idx.resources)
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
