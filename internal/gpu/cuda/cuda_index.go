//go:build gpu && linux

// NOTE: This file requires the "gpu" and "linux" build tags.
// If your IDE reports "no packages found", ensure your gopls/build configuration
// includes these tags (e.g., -tags=gpu,linux).

package cuda

/*
#cgo LDFLAGS: -lcudart -lcublas -lm ${SRCDIR}/kernels.o
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>
#include <stdbool.h>

typedef struct {
    int device;
    int dimensions;
    cudaStream_t streams[2];
    void* graphOffsets;
    void* graphNeighbors;
    void* graphWeights;
    int graphNodeCount;
    int graphEdgeCount;
} CUDAIndexHandle;

// Function declarations from kernels.cu
void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_l2_distance_kernel_v2(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_large_kernel_v2(const float* vectors, const float* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_kernel_v2_batched(const float** page_ptrs, const int* page_starts, const float* query, float* distances, int dim, int total_count, int num_pages, cudaStream_t stream);
void launch_l2_distance_kernel_large_v2_batched(const float** page_ptrs, const int* page_starts, const float* query, float* distances, int dim, int total_count, int num_pages, cudaStream_t stream);
void launch_l2_distance_float64_kernel(const double* vectors, const double* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_float64_kernel(const double* vectors, const double* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_int32_kernel(const int32_t* vectors, const int32_t* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_int32_kernel(const int32_t* vectors, const int32_t* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_uint32_kernel(const uint32_t* vectors, const uint32_t* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_uint32_kernel(const uint32_t* vectors, const uint32_t* query, float* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_int64_kernel(const int64_t* vectors, const int64_t* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_int64_kernel(const int64_t* vectors, const int64_t* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_uint64_kernel(const uint64_t* vectors, const uint64_t* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_dot_product_uint64_kernel(const uint64_t* vectors, const uint64_t* query, double* distances, int dim, int count, cudaStream_t stream);
void launch_l2_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_dot_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_pq_distance_kernel(const float* lookupTable, const unsigned char* codes, float* distances, int m, int count, cudaStream_t stream);
void launch_turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream);
void launch_turboquant_distance_kernel_v2(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream);
void launch_l2_distance_filtered_kernel(const float* vectors, const float* query, float* distances, const unsigned long long* bitset, int dimensions, int count, cudaStream_t stream);
void launch_topk_kernel(const float* distances, const int64_t* ids, int n, int k, float* outDistances, int64_t* outIDs, cudaStream_t stream);
int cuda_add_vectors_pq(CUDAIndexHandle* handle, unsigned char* h_codes, int64_t* h_ids, int count, int m);

// Graph functions
void launch_graph_bfs_expand_kernel(const uint32_t* frontier, int frontierSize, const uint32_t* offsets, const uint32_t* neighbors, unsigned long long* visited, uint32_t* nextFrontier, int* nextFrontierSize, cudaStream_t stream);
void launch_graph_activation_propagate_kernel(const float* activations, float* newActivations, const uint32_t* frontier, int frontierSize, const uint32_t* offsets, const uint32_t* neighbors, const float* weights, float alpha, cudaStream_t stream);
void launch_haversine_distance_kernel(const float* center, const float* points, float* distances, float earthRadius, int count, cudaStream_t stream);
void launch_l2_squared_kernel(const float* vectors, float* results, int dimensions, int count, cudaStream_t stream);

// K-Means Training Kernels
void launch_assign_to_clusters(const float* vectors, const float* centroids, uint32_t* assignments, int dim, int numVectors, int numCentroids, cudaStream_t stream);
void launch_sum_centroids(const float* vectors, const uint32_t* assignments, float* centroids, uint32_t* counts, int dim, int numVectors, cudaStream_t stream);
void launch_finalize_centroids(float* centroids, const uint32_t* counts, int dim, int numCentroids, cudaStream_t stream);
void launch_hnsw_prune_neighbors_kernel(const uint32_t* candidateIds, const float* candidateDists, uint32_t* selectedIds, uint32_t* selectedCount, const float** page_ptrs, const int* page_starts, int maxNeighbors, int numCandidates, int dim, int total_count, int num_pages, bool extendedHeuristic, cudaStream_t stream);

int cuda_train_kmeans(CUDAIndexHandle* handle, float* vectors, float* centroids, int numVectors, int dim, int k, int iterations);
int cuda_pq_encode(CUDAIndexHandle* handle, float* h_vectors, float* h_codebooks, unsigned char* h_codes, int numVectors, int m, int subDim);

CUDAIndexHandle* cuda_init(int dimensions) {
    int device = 0;
    cudaError_t err = cudaSetDevice(device);
    if (err != cudaSuccess) return NULL;

    CUDAIndexHandle* handle = (CUDAIndexHandle*)malloc(sizeof(CUDAIndexHandle));
    handle->device = device;
    handle->dimensions = dimensions;
    handle->graphOffsets = NULL;
    handle->graphNeighbors = NULL;
    handle->graphWeights = NULL;
    handle->graphNodeCount = 0;
    handle->graphEdgeCount = 0;

    cudaStreamCreate(&handle->streams[0]);
    cudaStreamCreate(&handle->streams[1]);

    return handle;
}

void cuda_free(CUDAIndexHandle* handle) {
    if (!handle) return;
    if (handle->graphOffsets) cudaFree(handle->graphOffsets);
    if (handle->graphNeighbors) cudaFree(handle->graphNeighbors);
    if (handle->graphWeights) cudaFree(handle->graphWeights);
    cudaStreamDestroy(handle->streams[0]);
    cudaStreamDestroy(handle->streams[1]);
    free(handle);
}

void cuda_get_device_info(CUDAIndexHandle* handle, char* name, int maxLen, uint64_t* totalMem) {
    struct cudaDeviceProp prop;
    cudaError_t err = cudaGetDeviceProperties(&prop, handle->device);
    if (err == cudaSuccess) {
        strncpy(name, prop.name, maxLen - 1);
        name[maxLen - 1] = '\0';
        *totalMem = prop.totalGlobalMem;
    } else {
        name[0] = '\0';
        *totalMem = 0;
    }
}

int cuda_train_kmeans(CUDAIndexHandle* handle, float* h_vectors, float* h_centroids, int numVectors, int dim, int k, int iterations) {
    if (!handle) return -1;

    float *d_vectors, *d_centroids, *d_sumCentroids;
    uint32_t *d_assignments, *d_counts;

    cudaMalloc((void**)&d_vectors, (size_t)numVectors * dim * sizeof(float));
    cudaMalloc((void**)&d_centroids, (size_t)k * dim * sizeof(float));
    cudaMalloc((void**)&d_sumCentroids, (size_t)k * dim * sizeof(float));
    cudaMalloc((void**)&d_assignments, (size_t)numVectors * sizeof(uint32_t));
    cudaMalloc((void**)&d_counts, (size_t)k * sizeof(uint32_t));

    cudaMemcpy(d_vectors, h_vectors, (size_t)numVectors * dim * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy(d_centroids, h_centroids, (size_t)k * dim * sizeof(float), cudaMemcpyHostToDevice);

    for (int i = 0; i < iterations; i++) {
        cudaMemset(d_counts, 0, (size_t)k * sizeof(uint32_t));
        cudaMemset(d_sumCentroids, 0, (size_t)k * dim * sizeof(float));

        launch_assign_to_clusters(d_vectors, d_centroids, d_assignments, dim, numVectors, k, handle->streams[0]);
        launch_sum_centroids(d_vectors, d_assignments, d_sumCentroids, d_counts, dim, numVectors, handle->streams[0]);
        launch_finalize_centroids(d_sumCentroids, d_counts, dim, k, handle->streams[0]);

        // Update centroids for next iteration
        cudaMemcpy(d_centroids, d_sumCentroids, (size_t)k * dim * sizeof(float), cudaMemcpyDeviceToDevice);
    }

    cudaMemcpy(h_centroids, d_centroids, (size_t)k * dim * sizeof(float), cudaMemcpyDeviceToHost);

    cudaFree(d_vectors);
    cudaFree(d_centroids);
    cudaFree(d_sumCentroids);
    cudaFree(d_assignments);
    cudaFree(d_counts);

    return 0;
}



int cuda_haversine_batch(CUDAIndexHandle* handle, float* h_center, float* h_points, float* h_results, float earthRadius, int count) {
    float *d_center, *d_points, *d_results;
    cudaMalloc((void**)&d_center, 2 * sizeof(float));
    cudaMalloc((void**)&d_points, count * 2 * sizeof(float));
    cudaMalloc((void**)&d_results, count * sizeof(float));

    cudaMemcpy(d_center, h_center, 2 * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemcpy(d_points, h_points, count * 2 * sizeof(float), cudaMemcpyHostToDevice);

    launch_haversine_distance_kernel(d_center, d_points, d_results, earthRadius, count, 0);

    cudaMemcpy(h_results, d_results, count * sizeof(float), cudaMemcpyDeviceToHost);

    cudaFree(d_center); cudaFree(d_points); cudaFree(d_results);
    return 0;
}

int cuda_norm_batch_f32(CUDAIndexHandle* handle, float* h_vectors, float* h_results, int dimensions, int count) {
    float *d_vectors, *d_results;
    cudaMalloc((void**)&d_vectors, (size_t)count * dimensions * sizeof(float));
    cudaMalloc((void**)&d_results, count * sizeof(float));

    cudaMemcpy(d_vectors, h_vectors, (size_t)count * dimensions * sizeof(float), cudaMemcpyHostToDevice);

    launch_l2_squared_kernel(d_vectors, d_results, dimensions, count, 0);

    cudaMemcpy(h_results, d_results, count * sizeof(float), cudaMemcpyDeviceToHost);

    cudaFree(d_vectors); cudaFree(d_results);
    return 0;
}

void cuda_cleanup(CUDAIndexHandle* handle) {
    cuda_free(handle);
}

int cuda_update_graph(CUDAIndexHandle* handle, uint32_t* h_offsets, uint32_t* h_neighbors, float* h_weights, int nodeCount, int edgeCount) {
    if (handle->graphOffsets) cudaFree(handle->graphOffsets);
    if (handle->graphNeighbors) cudaFree(handle->graphNeighbors);
    if (handle->graphWeights) cudaFree(handle->graphWeights);

    cudaMalloc((void**)&handle->graphOffsets, (nodeCount + 1) * sizeof(uint32_t));
    cudaMalloc((void**)&handle->graphNeighbors, edgeCount * sizeof(uint32_t));
    if (h_weights) cudaMalloc((void**)&handle->graphWeights, edgeCount * sizeof(float));

    cudaMemcpy(handle->graphOffsets, h_offsets, (nodeCount + 1) * sizeof(uint32_t), cudaMemcpyHostToDevice);
    cudaMemcpy(handle->graphNeighbors, h_neighbors, edgeCount * sizeof(uint32_t), cudaMemcpyHostToDevice);
    if (h_weights) cudaMemcpy(handle->graphWeights, h_weights, edgeCount * sizeof(float), cudaMemcpyHostToDevice);

    handle->graphNodeCount = nodeCount;
    handle->graphEdgeCount = edgeCount;
    return 0;
}

int cuda_prune_neighbors(CUDAIndexHandle* handle, uint32_t* candidateIds, float* candidateDists, uint32_t* selectedIds, uint32_t* selectedCount, const float** page_ptrs, const int* page_starts, int maxNeighbors, int numCandidates, int dim, int total_count, int num_pages, bool extended) {
    if (!handle) return -1;

    uint32_t *d_candIds, *d_selIds, *d_selCount;
    float *d_candDists;
    const float **d_pagePtrs;
    int *d_pageStarts;

    cudaMalloc((void**)&d_candIds, (size_t)numCandidates * sizeof(uint32_t));
    cudaMalloc((void**)&d_candDists, (size_t)numCandidates * sizeof(float));
    cudaMalloc((void**)&d_selIds, (size_t)maxNeighbors * sizeof(uint32_t));
    cudaMalloc((void**)&d_selCount, sizeof(uint32_t));

    cudaMalloc((void**)&d_pagePtrs, (size_t)num_pages * sizeof(float*));
    cudaMemcpy(d_pagePtrs, page_ptrs, (size_t)num_pages * sizeof(float*), cudaMemcpyHostToDevice);

    cudaMalloc((void**)&d_pageStarts, (size_t)(num_pages+1) * sizeof(int));
    cudaMemcpy(d_pageStarts, page_starts, (size_t)(num_pages+1) * sizeof(int), cudaMemcpyHostToDevice);

    cudaMemcpy(d_candIds, candidateIds, (size_t)numCandidates * sizeof(uint32_t), cudaMemcpyHostToDevice);
    cudaMemcpy(d_candDists, candidateDists, (size_t)numCandidates * sizeof(float), cudaMemcpyHostToDevice);
    cudaMemset(d_selCount, 0, sizeof(uint32_t));

    launch_hnsw_prune_neighbors_kernel(d_candIds, d_candDists, d_selIds, d_selCount, d_pagePtrs, d_pageStarts, maxNeighbors, numCandidates, dim, total_count, num_pages, extended, handle->streams[0]);

    uint32_t h_selCount;
    cudaMemcpy(&h_selCount, d_selCount, sizeof(uint32_t), cudaMemcpyDeviceToHost);
    *selectedCount = h_selCount;
    cudaMemcpy(selectedIds, d_selIds, (size_t)h_selCount * sizeof(uint32_t), cudaMemcpyDeviceToHost);

    cudaFree(d_candIds);
    cudaFree(d_candDists);
    cudaFree(d_selIds);
    cudaFree(d_selCount);
    cudaFree(d_pagePtrs);
    cudaFree(d_pageStarts);

    return 0;
}
*/
import "C"
import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"golang.org/x/sync/semaphore"
)

const vectorsPerPage = 1024

type chunkTracker struct {
	startChunk int
	numChunks  int
}

type CUDAIndex struct {
	handle     *C.CUDAIndexHandle
	dim        int
	mu         sync.RWMutex
	closed     bool
	memPool    *memory.GPUMemPool
	pager      *memory.GPUPager
	deviceInfo *types.GPUInfo
	pqEncoder  *pq.PQEncoder // CPU fallback for PQ operations

	batchIDs     []int64
	batchVectors []float32
	batchMu      sync.Mutex
	lastSyncTime time.Time
	syncTicker   *time.Ticker
	stopSync     chan struct{}

	maxMemory  int64
	usedMemory int64

	// opSem limits concurrent GPU operations to prevent VRAM oversubscription.
	opSem *semaphore.Weighted

	vectorCount int
	idList      []int64
	pqM         int // PQ subquantizer count
	tqStride    int // TQ bytes per vector
	tqBitsAngle int // TQ bits per angle

	// chunk tracking per dtype
	fp32Chunks chunkTracker
	fp16Chunks chunkTracker
	pqChunks   chunkTracker
	tqChunks   chunkTracker

	// PageID base for each dtype to avoid collisions
	nextPageID memory.PageID
}

// pageIDFor returns a unique PageID for a given dtype and chunk index.
func (idx *CUDAIndex) pageIDFor(dtype int, chunk int) memory.PageID {
	return memory.PageID(dtype)*1_000_000_000 + memory.PageID(chunk)
}

// NewCUDAIndex creates a new CUDA index for the given configuration.
func NewCUDAIndex(cfg types.GPUConfig) (types.Index, error) {
	return NewCUDAIndexImpl(cfg)
}

func NewCUDAIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	if cfg.Dimension <= 0 {
		return nil, &types.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  types.BackendCUDA,
			Cause:    fmt.Errorf("dimension must be positive, got %d", cfg.Dimension),
		}
	}

	if err := SetDevice(cfg.DeviceID); err != nil {
		return nil, &types.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  types.BackendCUDA,
			Cause:    err,
		}
	}

	initialCapacity := 10000
	if cfg.Dimension > 2147483647 || initialCapacity > 2147483647 {
		return nil, fmt.Errorf("dimension or capacity too large")
	}
	handle := C.cuda_init(C.int(cfg.Dimension)) // #nosec G115
	if handle == nil {
		return nil, &types.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  types.BackendCUDA,
			Cause:    fmt.Errorf("failed to initialize CUDA device"),
		}
	}

	nameBuf := make([]C.char, 256)
	var totalMem C.uint64_t
	C.cuda_get_device_info(handle, &nameBuf[0], C.int(len(nameBuf)), &totalMem) // #nosec G115

	maxVRAM := cfg.MaxMemory
	if maxVRAM <= 0 {
		maxVRAM = int64(totalMem) // use all available GPU memory
	}

	pageSize := int64(vectorsPerPage) * int64(cfg.Dimension) * 4

	maxGPUOps := runtime.GOMAXPROCS(0)
	if maxGPUOps < 4 {
		maxGPUOps = 4
	}
	idx := &CUDAIndex{
		handle: handle,
		dim:    cfg.Dimension,
		deviceInfo: &types.GPUInfo{
			Backend:  types.BackendCUDA,
			Name:     C.GoString(&nameBuf[0]),
			DeviceID: cfg.DeviceID,
			MemoryMB: int64(totalMem) / (1024 * 1024), // #nosec G115 -- safe division
		},
		lastSyncTime: time.Now(),
		stopSync:     make(chan struct{}),
		maxMemory:    maxVRAM,
		opSem:        semaphore.NewWeighted(int64(maxGPUOps)),
	}

	pool, err := memory.NewGPUMemPool(types.BackendCUDA, cfg.DeviceID)
	if err == nil {
		pool.SetTotalMemory(maxVRAM)
		idx.memPool = pool
		idx.pager = memory.NewGPUPager(pool, maxVRAM, pageSize)
	}

	idx.startSyncTicker(cfg)

	runtime.SetFinalizer(idx, (*CUDAIndex).Close)
	return idx, nil
}

func (idx *CUDAIndex) Add(ids []int64, vectors []float32) error {
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

	idx.batchMu.Lock()
	idx.batchIDs = append(idx.batchIDs, ids...)
	idx.batchVectors = append(idx.batchVectors, vectors...)
	batchSize := len(idx.batchIDs)
	idx.batchMu.Unlock()

	if batchSize >= 1000 {
		return idx.Flush()
	}

	return nil
}

func (idx *CUDAIndex) Flush() error {
	if err := idx.acquireGPUOp(); err != nil {
		return fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.batchMu.Lock()
	defer idx.batchMu.Unlock()

	if len(idx.batchIDs) == 0 {
		return nil
	}

	start := time.Now()
	batchCount := len(idx.batchIDs)

	if batchCount > 2147483647 {
		return fmt.Errorf("batch too large")
	}

	if idx.pager == nil {
		return fmt.Errorf("GPU pager not initialized")
	}

	dim := idx.dim
	maxMem := idx.maxMemory
	prevCount := idx.vectorCount
	newCount := prevCount + batchCount

	// Estimate total memory needed with paging
	totalPages := (newCount + vectorsPerPage - 1) / vectorsPerPage
	estimatedMem := int64(totalPages) * int64(vectorsPerPage) * int64(dim) * 4
	if maxMem > 0 && estimatedMem > maxMem {
		return &types.GPUSyncError{
			BatchSize: batchCount,
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("GPU memory limit exceeded: estimated %d bytes, limit %d", estimatedMem, maxMem),
		}
	}

	vecSize := dim * 4 // float32 bytes per vector
	pageVecs := vectorsPerPage

	for i := 0; i < batchCount; {
		globalPos := prevCount + i
		chunk := globalPos / pageVecs
		offset := globalPos % pageVecs
		space := pageVecs - offset
		toCopy := batchCount - i
		if toCopy > space {
			toCopy = space
		}

		pid := idx.pageIDFor(0, chunk)

		// Get or create page in pager
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			var err error
			pi, err = idx.pager.Alloc(pid)
			if err != nil {
				return &types.GPUSyncError{
					BatchSize: batchCount,
					DeviceID:  idx.deviceInfo.DeviceID,
					Cause:     fmt.Errorf("failed to allocate pager page %d: %w", pid, err),
				}
			}
		}

		// Copy vector data to page's CPU buffer
		cpuBuf := idx.pager.GetCPUBuf(pi)
		srcVec := idx.batchVectors[i*int(dim) : (i+toCopy)*int(dim)]
		dstOffset := offset * vecSize
		copy(cpuBuf[dstOffset:dstOffset+toCopy*vecSize], unsafe.Slice((*byte)(unsafe.Pointer(&srcVec[0])), toCopy*vecSize))

		// Promote page to GPU (copies CPU->GPU, evicts LRU if needed)
		if err := idx.pager.Promote(pi); err != nil {
			return &types.GPUSyncError{
				BatchSize: batchCount,
				DeviceID:  idx.deviceInfo.DeviceID,
				Cause:     fmt.Errorf("failed to promote page %d to GPU: %w", pid, err),
			}
		}

		i += toCopy
	}

	// Update tracking
	idx.vectorCount = newCount
	idx.idList = append(idx.idList, idx.batchIDs...)

	duration := time.Since(start)
	metrics.RecordGPUSync(duration, batchCount)

	idx.batchIDs = idx.batchIDs[:0]
	idx.batchVectors = idx.batchVectors[:0]
	idx.lastSyncTime = time.Now()

	return nil
}

func (idx *CUDAIndex) AddPQ(ids []int64, codes []byte, m int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}
	if len(ids) > 2147483647 || m > 2147483647 {
		return fmt.Errorf("ids or M too large")
	}
	if idx.pager == nil {
		return fmt.Errorf("GPU pager not initialized")
	}

	prevCount := idx.vectorCount
	newCount := prevCount + len(ids)
	codeBytesPerVec := m

	pageVecs := vectorsPerPage

	idx.idList = append(idx.idList, ids...)

	for i := 0; i < len(ids); {
		globalPos := prevCount + i
		chunk := globalPos / pageVecs
		offset := globalPos % pageVecs
		space := pageVecs - offset
		toCopy := len(ids) - i
		if toCopy > space {
			toCopy = space
		}

		pid := idx.pageIDFor(2, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			var err error
			pi, err = idx.pager.Alloc(pid)
			if err != nil {
				return fmt.Errorf("failed to allocate pager page for PQ chunk %d: %w", chunk, err)
			}
		}

		cpuBuf := idx.pager.GetCPUBuf(pi)
		srcStart := i * codeBytesPerVec
		srcEnd := (i + toCopy) * codeBytesPerVec
		dstStart := offset * codeBytesPerVec
		copy(cpuBuf[dstStart:dstStart+toCopy*codeBytesPerVec], codes[srcStart:srcEnd])

		if err := idx.pager.Promote(pi); err != nil {
			return fmt.Errorf("failed to promote PQ page %d: %w", pid, err)
		}

		i += toCopy
	}

	idx.vectorCount = newCount
	idx.pqM = m
	return nil
}

// acquireGPUOp blocks until a GPU operation slot is available or context is cancelled.
// Callers MUST defer releaseGPUOp.
func (idx *CUDAIndex) acquireGPUOp() error {
	if idx.opSem == nil {
		return nil
	}
	return idx.opSem.Acquire(nil, 1)
}

// releaseGPUOp releases a GPU operation slot.
func (idx *CUDAIndex) releaseGPUOp() {
	if idx.opSem == nil {
		return
	}
	idx.opSem.Release(1)
}

func (idx *CUDAIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	if err := idx.Flush(); err != nil {
		return nil, nil, err
	}

	if idx.pager == nil {
		return nil, nil, fmt.Errorf("GPU pager not initialized")
	}

	n := idx.vectorCount
	if n == 0 {
		return nil, nil, nil
	}

	if k > 2147483647 {
		return nil, nil, fmt.Errorf("k too large")
	}
	if k > n {
		k = n
	}

	start := time.Now()

	// Upload query to GPU
	var dQuery unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dQuery)), C.size_t(idx.dim*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("failed to allocate query GPU memory")
	}
	defer C.cudaFree(dQuery)
	C.cudaMemcpy(dQuery, unsafe.Pointer(&vector[0]), C.size_t(idx.dim*4), C.cudaMemcpyHostToDevice)

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage

	type pageEntry struct {
		ptr   unsafe.Pointer
		nvecs int
	}
	pages := make([]pageEntry, 0, numChunks)
	for chunk := 0; chunk < numChunks; chunk++ {
		pid := idx.pageIDFor(0, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			continue
		}
		if err := idx.pager.Promote(pi); err != nil {
			continue
		}
		gpuPtr := idx.pager.GetGPUAddr(pi)
		if gpuPtr == nil {
			continue
		}
		vecsInChunk := n - chunk*vectorsPerPage
		if vecsInChunk > vectorsPerPage {
			vecsInChunk = vectorsPerPage
		}
		pages = append(pages, pageEntry{ptr: gpuPtr, nvecs: vecsInChunk})
	}

	if len(pages) == 0 {
		return nil, nil, fmt.Errorf("no resident pages available for search")
	}

	numPages := len(pages)
	hPageStarts := make([]C.int, numPages+1)
	hPagePtrs := make([]unsafe.Pointer, numPages)
	for i, p := range pages {
		hPagePtrs[i] = p.ptr
		hPageStarts[i+1] = hPageStarts[i] + C.int(p.nvecs)
	}
	totalVecs := int(hPageStarts[numPages])

	// Allocate single output buffer for all vectors
	var dAllDists unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dAllDists)), C.size_t(totalVecs*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("failed to allocate distance buffer")
	}
	defer C.cudaFree(dAllDists)

	// Allocate device-side arrays for batched launch
	var dPagePtrs unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dPagePtrs)), C.size_t(numPages)*C.size_t(unsafe.Sizeof(hPagePtrs[0]))); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("failed to allocate page pointers buffer")
	}
	defer C.cudaFree(dPagePtrs)
	C.cudaMemcpy(dPagePtrs, unsafe.Pointer(&hPagePtrs[0]), C.size_t(numPages)*C.size_t(unsafe.Sizeof(hPagePtrs[0])), C.cudaMemcpyHostToDevice)

	var dPageStarts unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dPageStarts)), C.size_t((numPages+1)*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("failed to allocate page starts buffer")
	}
	defer C.cudaFree(dPageStarts)
	C.cudaMemcpy(dPageStarts, unsafe.Pointer(&hPageStarts[0]), C.size_t((numPages+1)*4), C.cudaMemcpyHostToDevice)

	if idx.dim > 1024 {
		C.launch_l2_distance_kernel_large_v2_batched(
			(**C.float)(dPagePtrs),
			(*C.int)(dPageStarts),
			(*C.float)(dQuery),
			(*C.float)(dAllDists),
			C.int(idx.dim),
			C.int(totalVecs),
			C.int(numPages),
			nil,
		)
	} else {
		C.launch_l2_distance_kernel_v2_batched(
			(**C.float)(dPagePtrs),
			(*C.int)(dPageStarts),
			(*C.float)(dQuery),
			(*C.float)(dAllDists),
			C.int(idx.dim),
			C.int(totalVecs),
			C.int(numPages),
			nil,
		)
	}

	hAllDists := make([]float32, totalVecs)
	C.cudaMemcpy(
		unsafe.Pointer(&hAllDists[0]),
		dAllDists,
		C.size_t(totalVecs*4),
		C.cudaMemcpyDeviceToHost,
	)

	type scored struct {
		dist float32
		pos  int
	}
	all := make([]scored, 0, totalVecs)
	for i, d := range hAllDists {
		all = append(all, scored{dist: d, pos: i})
	}

	// Sort all distances to find top-K
	sort.Slice(all, func(i, j int) bool {
		return all[i].dist < all[j].dist
	})
	if k > len(all) {
		k = len(all)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)
	for i := 0; i < k; i++ {
		resultDistances[i] = all[i].dist
		if all[i].pos < len(idx.idList) {
			resultIDs[i] = idx.idList[all[i].pos]
		}
	}

	duration := time.Since(start)
	metrics.RecordGPUSearch(duration, "cuda", k)

	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) SearchPQ(lookupTable []float32, m int, k int) ([]int64, []float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	if m > 2147483647 || k > 2147483647 {
		return nil, nil, fmt.Errorf("m or k too large")
	}
	if idx.pager == nil {
		return nil, nil, fmt.Errorf("GPU pager not initialized")
	}

	n := idx.vectorCount
	if n == 0 {
		return nil, nil, nil
	}
	if k > n {
		k = n
	}

	start := time.Now()

	// Upload lookup table to GPU
	tableSize := C.size_t(m * 256 * 4) // m * 256 floats
	var dTable unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dTable)), tableSize); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("failed to allocate lookup table GPU memory")
	}
	defer C.cudaFree(dTable)
	C.cudaMemcpy(dTable, unsafe.Pointer(&lookupTable[0]), tableSize, C.cudaMemcpyHostToDevice)

	// Per-page distance buffer
	var dPageDists unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dPageDists)), C.size_t(vectorsPerPage*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("failed to allocate per-page distance buffer")
	}
	defer C.cudaFree(dPageDists)

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage
	type scored struct {
		dist float32
		pos  int
	}
	all := make([]scored, 0, n)
	hPageDists := make([]float32, vectorsPerPage)

	for chunk := 0; chunk < numChunks; chunk++ {
		pid := idx.pageIDFor(2, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			continue
		}
		if err := idx.pager.Promote(pi); err != nil {
			continue
		}
		gpuPtr := idx.pager.GetGPUAddr(pi)
		if gpuPtr == nil {
			continue
		}

		vecsInChunk := n - chunk*vectorsPerPage
		if vecsInChunk > vectorsPerPage {
			vecsInChunk = vectorsPerPage
		}

		C.launch_pq_distance_kernel(
			(*C.float)(dTable),
			(*C.uchar)(gpuPtr),
			(*C.float)(dPageDists),
			C.int(m),
			C.int(vecsInChunk),
			nil,
		)

		hPageDists = hPageDists[:vecsInChunk]
		C.cudaMemcpy(
			unsafe.Pointer(&hPageDists[0]),
			dPageDists,
			C.size_t(vecsInChunk*4),
			C.cudaMemcpyDeviceToHost,
		)

		base := chunk * vectorsPerPage
		for i, d := range hPageDists {
			all = append(all, scored{dist: d, pos: base + i})
		}
	}

	if len(all) == 0 {
		return nil, nil, fmt.Errorf("no resident PQ pages available")
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].dist < all[j].dist
	})
	if k > len(all) {
		k = len(all)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)
	for i := 0; i < k; i++ {
		resultDistances[i] = all[i].dist
		if all[i].pos < len(idx.idList) {
			resultIDs[i] = idx.idList[all[i].pos]
		}
	}

	metrics.RecordGPUSearch(time.Since(start), "cuda_pq", k)
	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) TrainPQ(vectors []float32, m int, k int) error {
	if err := idx.acquireGPUOp(); err != nil {
		return fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	dims := idx.dim
	if dims%m != 0 {
		return fmt.Errorf("dimension %d must be divisible by M %d", dims, m)
	}
	subDim := dims / m
	numVecs := len(vectors) / dims

	encoder, err := pq.NewPQEncoder(dims, m, k)
	if err != nil {
		return fmt.Errorf("failed to create PQ encoder: %w", err)
	}

	// Train each subspace
	for i := 0; i < m; i++ {
		subData := make([]float32, numVecs*subDim)
		for j := 0; j < numVecs; j++ {
			copy(subData[j*subDim:(j+1)*subDim], vectors[j*dims+i*subDim:j*dims+(i+1)*subDim])
		}

		// Initialize centroids randomly
		centroids := make([]float32, k*subDim)
		perm := rand.Perm(numVecs)
		for j := 0; j < k; j++ {
			copy(centroids[j*subDim:(j+1)*subDim], subData[perm[j]*subDim:(perm[j]+1)*subDim])
		}

		// Run GPU K-Means
		res := C.cuda_train_kmeans(idx.handle, (*C.float)(&subData[0]), (*C.float)(&centroids[0]), C.int(numVecs), C.int(subDim), C.int(k), 20)
		if res != 0 {
			return fmt.Errorf("GPU K-Means failed for subspace %d", i)
		}

		copy(encoder.Codebooks[i], centroids)
	}

	idx.pqEncoder = encoder
	return nil
}

func (idx *CUDAIndex) EncodePQ(vectors []float32) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	if idx.pqEncoder == nil {
		return nil, fmt.Errorf("PQ encoder not trained")
	}

	numVecs := len(vectors) / idx.dim
	codes := make([]byte, numVecs*idx.pqEncoder.M)

	for i := 0; i < numVecs; i++ {
		vec := vectors[i*idx.dim : (i+1)*idx.dim]
		encoded, err := idx.pqEncoder.Encode(vec)
		if err != nil {
			return nil, fmt.Errorf("encoding failed at vector %d: %w", i, err)
		}
		copy(codes[i*idx.pqEncoder.M:(i+1)*idx.pqEncoder.M], encoded)
	}

	return codes, nil
}

func (idx *CUDAIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
	if len(vectors) == 0 {
		return nil, nil, nil
	}

	results := make([][]int64, len(vectors))
	distances := make([][]float32, len(vectors))

	for i, vec := range vectors {
		ids, dist, err := idx.Search(vec, k)
		if err != nil {
			return nil, nil, fmt.Errorf("batch search[%d]: %w", i, err)
		}
		results[i] = ids
		distances[i] = dist
	}

	return results, distances, nil
}

func (idx *CUDAIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.syncTicker != nil {
		idx.syncTicker.Stop()
		close(idx.stopSync)
	}

	idx.Flush()

	if idx.pager != nil {
		idx.pager.Close()
	}

	if idx.memPool != nil {
		idx.memPool.Close()
	}

	if idx.handle != nil {
		C.cuda_cleanup(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}

func (idx *CUDAIndex) Backend() types.GPUBackend {
	return types.BackendCUDA
}

func (idx *CUDAIndex) DeviceID() int32 {
	return idx.deviceInfo.DeviceID
}

func (idx *CUDAIndex) GetDeviceInfo() (*types.GPUInfo, error) {
	return idx.deviceInfo, nil
}

func (idx *CUDAIndex) GetMemoryInfo() (total, free, used int64, err error) {
	if idx.memPool != nil {
		total = idx.memPool.GetTotalMemory()
		used = idx.memPool.GetUsedMemory()
		free = total - used
		return
	}
	return idx.deviceInfo.MemoryMB * 1024 * 1024, 0, 0, nil
}

func (idx *CUDAIndex) GetDeviceCount() int {
	return GetDeviceCount()
}

func (idx *CUDAIndex) GetUtilization() (float32, error) {
	return 50.0, nil
}

func (idx *CUDAIndex) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}
	count := len(ids)
	if count == 0 {
		return nil
	}
	if idx.pager == nil {
		return fmt.Errorf("GPU pager not initialized")
	}

	stride := len(tqData) / count
	prevCount := idx.vectorCount
	newCount := prevCount + count

	idx.idList = append(idx.idList, ids...)
	pageVecs := vectorsPerPage

	for i := 0; i < count; {
		globalPos := prevCount + i
		chunk := globalPos / pageVecs
		offset := globalPos % pageVecs
		space := pageVecs - offset
		toCopy := count - i
		if toCopy > space {
			toCopy = space
		}

		pid := idx.pageIDFor(3, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			var err error
			pi, err = idx.pager.Alloc(pid)
			if err != nil {
				return fmt.Errorf("failed to allocate pager page for TQ chunk %d: %w", chunk, err)
			}
		}

		cpuBuf := idx.pager.GetCPUBuf(pi)
		srcStart := i * stride
		srcEnd := (i + toCopy) * stride
		dstStart := offset * stride
		copy(cpuBuf[dstStart:dstStart+toCopy*stride], tqData[srcStart:srcEnd])

		if err := idx.pager.Promote(pi); err != nil {
			return fmt.Errorf("failed to promote TQ page %d: %w", pid, err)
		}

		i += toCopy
	}

	idx.vectorCount = newCount
	idx.tqStride = stride
	idx.tqBitsAngle = bitsPerAngle
	return nil
}

func (idx *CUDAIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	if idx.pager == nil {
		return nil, nil, fmt.Errorf("GPU pager not initialized")
	}

	n := idx.vectorCount
	if n == 0 {
		return nil, nil, nil
	}
	if k > n {
		k = n
	}

	pow2 := 1
	for pow2 < idx.dim {
		pow2 <<= 1
	}

	start := time.Now()

	// Upload query to GPU
	var dQuery unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dQuery)), C.size_t(idx.dim*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("failed to allocate query GPU memory")
	}
	defer C.cudaFree(dQuery)
	C.cudaMemcpy(dQuery, unsafe.Pointer(&vector[0]), C.size_t(idx.dim*4), C.cudaMemcpyHostToDevice)

	// Per-page distance buffer
	var dPageDists unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dPageDists)), C.size_t(vectorsPerPage*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("failed to allocate per-page distance buffer")
	}
	defer C.cudaFree(dPageDists)

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage
	type scored struct {
		dist float32
		pos  int
	}
	all := make([]scored, 0, n)
	hPageDists := make([]float32, vectorsPerPage)

	for chunk := 0; chunk < numChunks; chunk++ {
		pid := idx.pageIDFor(3, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			continue
		}
		if err := idx.pager.Promote(pi); err != nil {
			continue
		}
		gpuPtr := idx.pager.GetGPUAddr(pi)
		if gpuPtr == nil {
			continue
		}

		vecsInChunk := n - chunk*vectorsPerPage
		if vecsInChunk > vectorsPerPage {
			vecsInChunk = vectorsPerPage
		}

		C.launch_turboquant_distance_kernel_v2(
			(*C.float)(dQuery),
			(*C.uchar)(gpuPtr),
			(*C.float)(dPageDists),
			C.int(idx.dim),
			C.int(pow2),
			C.int(bitsPerAngle),
			C.int(vecsInChunk),
			nil,
		)

		hPageDists = hPageDists[:vecsInChunk]
		C.cudaMemcpy(
			unsafe.Pointer(&hPageDists[0]),
			dPageDists,
			C.size_t(vecsInChunk*4),
			C.cudaMemcpyDeviceToHost,
		)

		base := chunk * vectorsPerPage
		for i, d := range hPageDists {
			all = append(all, scored{dist: d, pos: base + i})
		}
	}

	if len(all) == 0 {
		return nil, nil, fmt.Errorf("no resident TQ pages available")
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].dist < all[j].dist
	})
	if k > len(all) {
		k = len(all)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)
	for i := 0; i < k; i++ {
		resultDistances[i] = all[i].dist
		if all[i].pos < len(idx.idList) {
			resultIDs[i] = idx.idList[all[i].pos]
		}
	}

	metrics.RecordGPUSearch(time.Since(start), "cuda_tq", k)
	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) Initialize(deviceID int32) error {
	return SetDevice(deviceID)
}

func (idx *CUDAIndex) startSyncTicker(cfg types.GPUConfig) {
	if cfg.SyncInterval <= 0 {
		return
	}

	idx.syncTicker = time.NewTicker(cfg.SyncInterval)
	go func() {
		for {
			select {
			case <-idx.syncTicker.C:
				idx.batchMu.Lock()
				if len(idx.batchIDs) > 0 && time.Since(idx.lastSyncTime) >= cfg.SyncInterval {
					idx.Flush()
				}
				idx.batchMu.Unlock()
			case <-idx.stopSync:
				return
			}
		}
	}()
}

func (idx *CUDAIndex) SearchInt8(vector []int8, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert int8 to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchUint8(vector []uint8, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert uint8 to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchInt16(vector []int16, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert int16 to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchUint16(vector []uint16, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert uint16 to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}
	// Convert float16 query to float32 and use pager-based search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float16.New(float32(math.Float32frombits(uint32(v)))).Float32()
	}
	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// Convert uint16 pairs to float32 pairs for search
	f32Vec := make([]float32, len(vector)*2)
	for i, v := range vector {
		f := float16.New(float32(math.Float32frombits(uint32(v)))).Float32()
		f32Vec[i*2] = f
	}

	return idx.Search(f32Vec, k)
}

func (idx *CUDAIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
	// complex128 is just float32 - use as-is
	return idx.Search(vector, k)
}

func (idx *CUDAIndex) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
	// CPU fallback for cluster assignment
	numVecs := len(vectors) / idx.dim
	numClusters := len(centroids) / idx.dim
	assignments := make([]uint32, numVecs)

	for i := 0; i < numVecs; i++ {
		vec := vectors[i*idx.dim : (i+1)*idx.dim]
		minDist := float32(math.MaxFloat32)
		bestCluster := uint32(0)

		for j := 0; j < numClusters; j++ {
			centroid := centroids[j*idx.dim : (j+1)*idx.dim]
			dist := float32(0)
			for k := 0; k < idx.dim; k++ {
				diff := vec[k] - centroid[k]
				dist += diff * diff
			}
			if dist < minDist {
				minDist = dist
				bestCluster = uint32(j)
			}
		}
		assignments[i] = bestCluster
	}
	return assignments, nil
}

func (idx *CUDAIndex) SearchWithFilter(query []float32, k int, bitset []uint64) ([]int64, []float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", idx.dim, len(query))
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if idx.pager == nil {
		return nil, nil, fmt.Errorf("GPU pager not initialized")
	}

	n := idx.vectorCount
	if n == 0 {
		return nil, nil, nil
	}

	if k > n {
		k = n
	}

	start := time.Now()

	// Upload query to GPU
	var dQuery unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dQuery)), C.size_t(idx.dim*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("cudaMalloc query failed")
	}
	defer C.cudaFree(dQuery)
	C.cudaMemcpy(dQuery, unsafe.Pointer(&query[0]), C.size_t(idx.dim*4), C.cudaMemcpyHostToDevice)

	numChunks := (n + vectorsPerPage - 1) / vectorsPerPage

	type pageEntry struct {
		ptr   unsafe.Pointer
		nvecs int
	}
	pages := make([]pageEntry, 0, numChunks)
	for chunk := 0; chunk < numChunks; chunk++ {
		pid := idx.pageIDFor(0, chunk)
		pi := idx.pager.PageInfo(pid)
		if pi == nil {
			continue
		}
		if err := idx.pager.Promote(pi); err != nil {
			continue
		}
		gpuPtr := idx.pager.GetGPUAddr(pi)
		if gpuPtr == nil {
			continue
		}
		vecsInChunk := n - chunk*vectorsPerPage
		if vecsInChunk > vectorsPerPage {
			vecsInChunk = vectorsPerPage
		}
		pages = append(pages, pageEntry{ptr: gpuPtr, nvecs: vecsInChunk})
	}

	if len(pages) == 0 {
		return nil, nil, nil
	}

	numPages := len(pages)
	hPageStarts := make([]C.int, numPages+1)
	hPagePtrs := make([]unsafe.Pointer, numPages)
	for i, p := range pages {
		hPagePtrs[i] = p.ptr
		hPageStarts[i+1] = hPageStarts[i] + C.int(p.nvecs)
	}
	totalVecs := int(hPageStarts[numPages])

	var dAllDists unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dAllDists)), C.size_t(totalVecs*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("cudaMalloc distances failed")
	}
	defer C.cudaFree(dAllDists)

	var dPagePtrs unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dPagePtrs)), C.size_t(numPages)*C.size_t(unsafe.Sizeof(hPagePtrs[0]))); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("cudaMalloc page ptrs failed")
	}
	defer C.cudaFree(dPagePtrs)
	C.cudaMemcpy(dPagePtrs, unsafe.Pointer(&hPagePtrs[0]), C.size_t(numPages)*C.size_t(unsafe.Sizeof(hPagePtrs[0])), C.cudaMemcpyHostToDevice)

	var dPageStarts unsafe.Pointer
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&dPageStarts)), C.size_t((numPages+1)*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("cudaMalloc page starts failed")
	}
	defer C.cudaFree(dPageStarts)
	C.cudaMemcpy(dPageStarts, unsafe.Pointer(&hPageStarts[0]), C.size_t((numPages+1)*4), C.cudaMemcpyHostToDevice)

	if idx.dim > 1024 {
		C.launch_l2_distance_kernel_large_v2_batched(
			(**C.float)(dPagePtrs),
			(*C.int)(dPageStarts),
			(*C.float)(dQuery),
			(*C.float)(dAllDists),
			C.int(idx.dim),
			C.int(totalVecs),
			C.int(numPages),
			nil,
		)
	} else {
		C.launch_l2_distance_kernel_v2_batched(
			(**C.float)(dPagePtrs),
			(*C.int)(dPageStarts),
			(*C.float)(dQuery),
			(*C.float)(dAllDists),
			C.int(idx.dim),
			C.int(totalVecs),
			C.int(numPages),
			nil,
		)
	}

	hAllDists := make([]float32, totalVecs)
	C.cudaMemcpy(
		unsafe.Pointer(&hAllDists[0]),
		dAllDists,
		C.size_t(totalVecs*4),
		C.cudaMemcpyDeviceToHost,
	)

	type scored struct {
		dist float32
		pos  int
	}
	all := make([]scored, 0, totalVecs)
	for i, d := range hAllDists {
		if bitset != nil {
			if i < len(idx.idList) {
				id := idx.idList[i]
				if id >= 0 && int(id/64) < len(bitset) && (bitset[id/64]>>uint(id%64))&1 == 0 {
					continue
				}
			}
		}
		all = append(all, scored{dist: d, pos: i})
	}

	if len(all) == 0 {
		return nil, nil, nil
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].dist < all[j].dist
	})
	if k > len(all) {
		k = len(all)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)
	for i := 0; i < k; i++ {
		resultDistances[i] = all[i].dist
		if all[i].pos < len(idx.idList) {
			resultIDs[i] = idx.idList[all[i].pos]
		}
	}

	metrics.RecordGPUSearch(time.Since(start), "cuda_filtered", k)
	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	var wPtr *C.float
	if len(weights) > 0 {
		wPtr = (*C.float)(unsafe.Pointer(&weights[0]))
	}

	ret := C.cuda_update_graph(
		idx.handle,
		(*C.uint32_t)(unsafe.Pointer(&offsets[0])),
		(*C.uint32_t)(unsafe.Pointer(&neighbors[0])),
		wPtr,
		C.int(len(offsets)-1),
		C.int(len(neighbors)),
	)

	if ret != 0 {
		return fmt.Errorf("failed to update CUDA graph")
	}

	return nil
}

func (idx *CUDAIndex) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	if err := idx.acquireGPUOp(); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire GPU op slot: %w", err)
	}
	defer idx.releaseGPUOp()

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if idx.handle.graphOffsets == nil {
		return nil, nil, fmt.Errorf("graph not initialized on GPU")
	}

	nodeCount := int(idx.handle.graphNodeCount)

	// Allocate GPU buffers for BFS with error checking
	var d_frontier, d_nextFrontier *C.uint32_t
	var d_visited *C.ulonglong
	var d_activations, d_newActivations *C.float
	var d_nextSize *C.int

	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_frontier)), C.size_t(nodeCount*4)); ret != C.cudaSuccess {
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc frontier failed: %v", ret)
	}
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_nextFrontier)), C.size_t(nodeCount*4)); ret != C.cudaSuccess {
		C.cudaFree(unsafe.Pointer(d_frontier))
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc nextFrontier failed: %v", ret)
	}
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_visited)), C.size_t((nodeCount/64+1)*8)); ret != C.cudaSuccess {
		C.cudaFree(unsafe.Pointer(d_frontier))
		C.cudaFree(unsafe.Pointer(d_nextFrontier))
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc visited failed: %v", ret)
	}
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_activations)), C.size_t(nodeCount*4)); ret != C.cudaSuccess {
		C.cudaFree(unsafe.Pointer(d_frontier))
		C.cudaFree(unsafe.Pointer(d_nextFrontier))
		C.cudaFree(unsafe.Pointer(d_visited))
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc activations failed: %v", ret)
	}
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_newActivations)), C.size_t(nodeCount*4)); ret != C.cudaSuccess {
		C.cudaFree(unsafe.Pointer(d_frontier))
		C.cudaFree(unsafe.Pointer(d_nextFrontier))
		C.cudaFree(unsafe.Pointer(d_visited))
		C.cudaFree(unsafe.Pointer(d_activations))
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc newActivations failed: %v", ret)
	}
	if ret := C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_nextSize)), 4); ret != C.cudaSuccess {
		C.cudaFree(unsafe.Pointer(d_frontier))
		C.cudaFree(unsafe.Pointer(d_nextFrontier))
		C.cudaFree(unsafe.Pointer(d_visited))
		C.cudaFree(unsafe.Pointer(d_activations))
		C.cudaFree(unsafe.Pointer(d_newActivations))
		return nil, nil, fmt.Errorf("GraphExpand: cudaMalloc nextSize failed: %v", ret)
	}

	defer func() {
		C.cudaFree(unsafe.Pointer(d_frontier))
		C.cudaFree(unsafe.Pointer(d_nextFrontier))
		C.cudaFree(unsafe.Pointer(d_visited))
		C.cudaFree(unsafe.Pointer(d_activations))
		C.cudaFree(unsafe.Pointer(d_newActivations))
		C.cudaFree(unsafe.Pointer(d_nextSize))
	}()

	C.cudaMemset(unsafe.Pointer(d_visited), 0, C.size_t((nodeCount/64+1)*8))
	C.cudaMemset(unsafe.Pointer(d_activations), 0, C.size_t(nodeCount*4))
	C.cudaMemset(unsafe.Pointer(d_newActivations), 0, C.size_t(nodeCount*4))

	// Initial seeds
	frontierSize := len(seeds)
	if frontierSize == 0 {
		return nil, nil, nil
	}
	C.cudaMemcpy(unsafe.Pointer(d_frontier), unsafe.Pointer(&seeds[0]), C.size_t(frontierSize*4), C.cudaMemcpyHostToDevice)

	// Initial activations
	h_activations := make([]float32, nodeCount)
	for _, s := range seeds {
		if int(s) < nodeCount {
			h_activations[s] = 1.0
		}
	}
	C.cudaMemcpy(unsafe.Pointer(d_activations), unsafe.Pointer(&h_activations[0]), C.size_t(nodeCount*4), C.cudaMemcpyHostToDevice)

	for d := 0; d < depth; d++ {
		C.cudaMemset(unsafe.Pointer(d_nextSize), 0, 4)

		C.launch_graph_bfs_expand_kernel(
			d_frontier, C.int(frontierSize),
			(*C.uint32_t)(idx.handle.graphOffsets),
			(*C.uint32_t)(idx.handle.graphNeighbors),
			d_visited, d_nextFrontier, d_nextSize, nil,
		)

		C.launch_graph_activation_propagate_kernel(
			d_activations, d_newActivations,
			d_frontier, C.int(frontierSize),
			(*C.uint32_t)(idx.handle.graphOffsets),
			(*C.uint32_t)(idx.handle.graphNeighbors),
			(*C.float)(idx.handle.graphWeights),
			C.float(alpha), nil,
		)

		var nextSize C.int
		C.cudaMemcpy(unsafe.Pointer(&nextSize), unsafe.Pointer(d_nextSize), 4, C.cudaMemcpyDeviceToHost)
		if nextSize == 0 {
			break
		}

		// Swap buffers
		d_frontier, d_nextFrontier = d_nextFrontier, d_frontier
		frontierSize = int(nextSize)

		// Accumulate activations
		C.cudaMemcpy(unsafe.Pointer(d_activations), unsafe.Pointer(d_newActivations), C.size_t(nodeCount*4), C.cudaMemcpyDeviceToDevice)
	}

	// Results
	finalActivations := make([]float32, nodeCount)
	C.cudaMemcpy(unsafe.Pointer(&finalActivations[0]), unsafe.Pointer(d_newActivations), C.size_t(nodeCount*4), C.cudaMemcpyDeviceToHost)

	var resIDs []uint32
	var resScores []float32
	for i, s := range finalActivations {
		if s > 1e-6 {
			resIDs = append(resIDs, uint32(i))
			resScores = append(resScores, s)
		}
	}

	return resIDs, resScores, nil
}
func (idx *CUDAIndex) SearchBatchDistances(query []float32, candidateIDs []uint32) ([]float32, error) {
	return nil, fmt.Errorf("SearchBatchDistances not implemented for CUDAIndex")
}

func (idx *CUDAIndex) HaversineSearch(centerLat, centerLon float32, points []float32, earthRadius float32) ([]float32, error) {
	idx.mu.RLock()
	closed := idx.closed
	idx.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("index is closed")
	}

	count := len(points) / 2
	if count == 0 {
		return nil, nil
	}

	results := make([]float32, count)
	center := []float32{centerLat, centerLon}

	start := time.Now()
	ret := C.cuda_haversine_batch(
		idx.handle,
		(*C.float)(unsafe.Pointer(&center[0])),
		(*C.float)(unsafe.Pointer(&points[0])),
		(*C.float)(unsafe.Pointer(&results[0])),
		C.float(earthRadius),
		C.int(count),
	)

	if ret != 0 {
		return nil, fmt.Errorf("cuda_haversine_batch failed")
	}

	metrics.GPUComputeDurationSeconds.WithLabelValues(idx.deviceInfo.Name, "haversine").Observe(time.Since(start).Seconds())
	return results, nil
}

func (idx *CUDAIndex) NormBatch(vectors []float32, dims int) ([]float32, error) {
	idx.mu.RLock()
	closed := idx.closed
	idx.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("index is closed")
	}

	count := len(vectors) / dims
	if count == 0 {
		return nil, nil
	}

	results := make([]float32, count)

	start := time.Now()
	ret := C.cuda_norm_batch_f32(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vectors[0])),
		(*C.float)(unsafe.Pointer(&results[0])),
		C.int(dims),
		C.int(count),
	)

	if ret != 0 {
		return nil, fmt.Errorf("cuda_norm_batch_f32 failed")
	}

	metrics.GPUComputeDurationSeconds.WithLabelValues(idx.deviceInfo.Name, "norm").Observe(time.Since(start).Seconds())
	return results, nil
}

func (idx *CUDAIndex) PQEncode(vectors []float32, codebooks []float32, m, subDim int) ([]byte, error) {
	idx.mu.RLock()
	closed := idx.closed
	idx.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("index is closed")
	}

	numVectors := len(vectors) / (m * subDim)
	if numVectors == 0 {
		return nil, nil
	}

	codes := make([]byte, numVectors*m)

	start := time.Now()
	ret := C.cuda_pq_encode(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vectors[0])),
		(*C.float)(unsafe.Pointer(&codebooks[0])),
		(*C.uchar)(unsafe.Pointer(&codes[0])),
		C.int(numVectors),
		C.int(m),
		C.int(subDim),
	)

	if ret != 0 {
		return nil, fmt.Errorf("cuda_pq_encode failed")
	}

	metrics.GPUComputeDurationSeconds.WithLabelValues(idx.deviceInfo.Name, "pq_encode").Observe(time.Since(start).Seconds())
	return codes, nil
}
func (idx *CUDAIndex) PruneNeighbors(candidateIds []uint32, candidateDists []float32, maxNeighbors int, allVectors []float32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, fmt.Errorf("index closed")
	}

	numCandidates := len(candidateIds)
	selectedIds := make([]uint32, maxNeighbors)
	var selectedCount uint32

	var hPageStarts []C.int
	var hPagePtrs []unsafe.Pointer
	var totalVecs int
	var numPages int

	if len(allVectors) > 0 {
		// Monolithic case (fallback)
		numPages = 1
		totalVecs = len(allVectors) / idx.dim
		hPageStarts = []C.int{0, C.int(totalVecs)}
		hPagePtrs = []unsafe.Pointer{unsafe.Pointer(&allVectors[0])}
	} else {
		// Pager case
		n := idx.vectorCount
		numChunks := (n + vectorsPerPage - 1) / vectorsPerPage
		hPageStarts = make([]C.int, numChunks+1)
		hPagePtrs = make([]unsafe.Pointer, numChunks)

		var chunkCount int
		for chunk := 0; chunk < numChunks; chunk++ {
			pid := idx.pageIDFor(0, chunk)
			pi := idx.pager.PageInfo(pid)
			if pi == nil {
				continue
			}
			if err := idx.pager.Promote(pi); err != nil {
				continue
			}
			gpuPtr := idx.pager.GetGPUAddr(pi)
			if gpuPtr == nil {
				continue
			}
			vecsInChunk := n - chunk*vectorsPerPage
			if vecsInChunk > vectorsPerPage {
				vecsInChunk = vectorsPerPage
			}
			hPagePtrs[chunkCount] = gpuPtr
			hPageStarts[chunkCount+1] = hPageStarts[chunkCount] + C.int(vecsInChunk)
			chunkCount++
		}
		numPages = chunkCount
		totalVecs = int(hPageStarts[numPages])
		if numPages == 0 {
			return nil, fmt.Errorf("no resident pages available for prune neighbors")
		}
	}

	ret := C.cuda_prune_neighbors(
		idx.handle,
		(*C.uint32_t)(unsafe.Pointer(&candidateIds[0])),
		(*C.float)(unsafe.Pointer(&candidateDists[0])),
		(*C.uint32_t)(unsafe.Pointer(&selectedIds[0])),
		(*C.uint32_t)(unsafe.Pointer(&selectedCount)),
		(**C.float)(unsafe.Pointer(&hPagePtrs[0])),
		(*C.int)(unsafe.Pointer(&hPageStarts[0])),
		C.int(maxNeighbors),
		C.int(numCandidates),
		C.int(idx.dim),
		C.int(totalVecs),
		C.int(numPages),
		C.bool(true),
	)

	if ret != 0 {
		return nil, fmt.Errorf("cuda_prune_neighbors failed: %d", ret)
	}

	return selectedIds[:selectedCount], nil
}

func (idx *CUDAIndex) Clear() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	idx.batchMu.Lock()
	idx.batchIDs = idx.batchIDs[:0]
	idx.batchVectors = idx.batchVectors[:0]
	idx.batchMu.Unlock()

	idx.vectorCount = 0
	idx.idList = idx.idList[:0]
	return nil
}

func (idx *CUDAIndex) Reset() error {
	return idx.Clear()
}

func (idx *CUDAIndex) Sync() error {
	return idx.Flush()
}

func (idx *CUDAIndex) SearchGreedy(query []float32, entryPoint uint32, entryDist float32) (uint32, float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return 0, 0, fmt.Errorf("index is closed")
	}

	// For CUDA, we don't have a greedy search kernel yet, so we return the entry point.
	// This is consistent with the CPU fallback behavior in other indices.
	return entryPoint, entryDist, nil
}
