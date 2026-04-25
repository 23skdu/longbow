//go:build gpu && linux

package cuda

/*
#cgo LDFLAGS: -lcudart -lcublas -lm ${SRCDIR}/kernels.o
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

typedef struct {
    int device;
    void* buffers[4]; // 0: FP32, 1: FP16, 2: PQ, 3: TQ
    void* idBuffer;
    int vectorCount;
    int dimensions;
    int capacity;
    int currentType; // 0: float32, 1: float16, 2: int8/pq, 3: turboquant
    cudaStream_t streams[2];
    void* graphOffsets;
    void* graphNeighbors;
    void* graphWeights;
    int graphNodeCount;
    int graphEdgeCount;
} CUDAIndexHandle;

// Function declarations from kernels.cu
void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_l2_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_dot_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_pq_distance_kernel(const float* lookupTable, const unsigned char* codes, float* distances, int m, int count, cudaStream_t stream);
void launch_turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream);
void launch_l2_distance_filtered_kernel(const float* vectors, const float* query, float* distances, const unsigned long long* bitset, int dimensions, int count, cudaStream_t stream);
void launch_topk_kernel(const float* distances, const int64_t* ids, int n, int k, float* outDistances, int64_t* outIDs, cudaStream_t stream);
int cuda_add_vectors_pq(CUDAIndexHandle* handle, unsigned char* h_codes, int64_t* h_ids, int count, int m);

// Graph functions
void launch_graph_bfs_expand_kernel(const uint32_t* frontier, int frontierSize, const uint32_t* offsets, const uint32_t* neighbors, unsigned long long* visited, uint32_t* nextFrontier, int* nextFrontierSize, cudaStream_t stream);
void launch_graph_activation_propagate_kernel(const float* activations, float* newActivations, const uint32_t* frontier, int frontierSize, const uint32_t* offsets, const uint32_t* neighbors, const float* weights, float alpha, cudaStream_t stream);

CUDAIndexHandle* cuda_init(int dimensions, int initialCapacity) {
    int device = 0;
    cudaError_t err = cudaSetDevice(device);
    if (err != cudaSuccess) return NULL;

    CUDAIndexHandle* handle = (CUDAIndexHandle*)malloc(sizeof(CUDAIndexHandle));
    handle->device = device;
    for(int i=0; i<4; i++) handle->buffers[i] = NULL;
    handle->idBuffer = NULL;
    handle->vectorCount = 0;
    handle->dimensions = dimensions;
    handle->capacity = initialCapacity > 0 ? initialCapacity : 10000;
    handle->currentType = 0;
    handle->graphOffsets = NULL;
    handle->graphNeighbors = NULL;
    handle->graphWeights = NULL;
    handle->graphNodeCount = 0;
    handle->graphEdgeCount = 0;

    size_t idBufferSize = handle->capacity * sizeof(int64_t);
    err = cudaMalloc((void**)&handle->idBuffer, idBufferSize);
    if (err != cudaSuccess) {
        free(handle);
        return NULL;
    }
    cudaMemset(handle->idBuffer, 0, idBufferSize);

    cudaStreamCreate(&handle->streams[0]);
    cudaStreamCreate(&handle->streams[1]);

    return handle;
}

void cuda_free(CUDAIndexHandle* handle) {
    if (!handle) return;
    for(int i=0; i<4; i++) if (handle->buffers[i]) cudaFree(handle->buffers[i]);
    if (handle->idBuffer) cudaFree(handle->idBuffer);
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

void* cuda_ensure_buffer(CUDAIndexHandle* handle, int type, size_t elementSize) {
    if (handle->buffers[type]) return handle->buffers[type];
    size_t size = (size_t)handle->capacity * handle->dimensions * elementSize;
    if (type == 2) size = (size_t)handle->capacity * handle->dimensions; // PQ is 1 byte per dim
    if (type == 3) size = (size_t)handle->capacity * 128; // TQ estimate
    
    cudaError_t err = cudaMalloc(&handle->buffers[type], size);
    if (err != cudaSuccess) return NULL;
    cudaMemset(handle->buffers[type], 0, size);
    return handle->buffers[type];
}

int cuda_add_vectors(CUDAIndexHandle* handle, float* h_vectors, int64_t* h_ids, int count) {
    void* buf = cuda_ensure_buffer(handle, 0, sizeof(float));
    if (!buf || count <= 0) return -1;

    // Check capacity and realloc if needed (simplified for brevity, should handle all buffers)
    if (handle->vectorCount + count > handle->capacity) return -2; 

    size_t vOffset = (size_t)handle->vectorCount * handle->dimensions * sizeof(float);
    size_t iOffset = (size_t)handle->vectorCount * sizeof(int64_t);

    cudaMemcpyAsync((char*)handle->buffers[0] + vOffset, h_vectors, (size_t)count * handle->dimensions * sizeof(float), cudaMemcpyHostToDevice, handle->streams[0]);
    cudaMemcpyAsync((char*)handle->idBuffer + iOffset, h_ids, (size_t)count * sizeof(int64_t), cudaMemcpyHostToDevice, handle->streams[0]);
    cudaStreamSynchronize(handle->streams[0]);

    handle->vectorCount += count;
    return 0;
}

int cuda_search(CUDAIndexHandle* handle, float* h_query, int k, int64_t* h_resultIDs, float* h_resultDistances) {
    if (!handle->buffers[0] || handle->vectorCount == 0) return -1;

    float *d_query, *d_distances, *d_outDist;
    int64_t *d_outIDs;
    cudaMalloc(&d_query, handle->dimensions * sizeof(float));
    cudaMalloc(&d_distances, handle->vectorCount * sizeof(float));
    cudaMalloc(&d_outDist, k * sizeof(float));
    cudaMalloc(&d_outIDs, k * sizeof(int64_t));

    cudaMemcpy(d_query, h_query, handle->dimensions * sizeof(float), cudaMemcpyHostToDevice);
    launch_l2_distance_kernel((float*)handle->buffers[0], d_query, d_distances, handle->dimensions, handle->vectorCount, 0);
    
    launch_topk_kernel(d_distances, (int64_t*)handle->idBuffer, handle->vectorCount, k, d_outDist, d_outIDs, 0);

    cudaMemcpy(h_resultDistances, d_outDist, k * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy(h_resultIDs, d_outIDs, k * sizeof(int64_t), cudaMemcpyDeviceToHost);

    cudaFree(d_query); cudaFree(d_distances); cudaFree(d_outDist); cudaFree(d_outIDs);
    return 0;
}

int cuda_add_vectors_fp16(CUDAIndexHandle* handle, uint16_t* h_vectors, int64_t* h_ids, int count) {
    void* buf = cuda_ensure_buffer(handle, 1, sizeof(uint16_t));
    if (!buf || count <= 0) return -1;
    if (handle->vectorCount + count > handle->capacity) return -2;

    size_t vOffset = (size_t)handle->vectorCount * handle->dimensions * sizeof(uint16_t);
    size_t iOffset = (size_t)handle->vectorCount * sizeof(int64_t);

    cudaMemcpyAsync((char*)handle->buffers[1] + vOffset, h_vectors, (size_t)count * handle->dimensions * sizeof(uint16_t), cudaMemcpyHostToDevice, handle->streams[0]);
    cudaMemcpyAsync((char*)handle->idBuffer + iOffset, h_ids, (size_t)count * sizeof(int64_t), cudaMemcpyHostToDevice, handle->streams[0]);
    cudaStreamSynchronize(handle->streams[0]);

    handle->vectorCount += count;
    return 0;
}

int cuda_search_fp16(CUDAIndexHandle* handle, uint16_t* h_query, int k, int metric, int64_t* h_resultIDs, float* h_resultDistances) {
    if (!handle->buffers[1] || handle->vectorCount == 0) return -1;

    uint16_t* d_query;
    float *d_distances, *d_outDist;
    int64_t *d_outIDs;
    cudaMalloc(&d_query, handle->dimensions * sizeof(uint16_t));
    cudaMalloc(&d_distances, handle->vectorCount * sizeof(float));
    cudaMalloc(&d_outDist, k * sizeof(float));
    cudaMalloc(&d_outIDs, k * sizeof(int64_t));

    cudaMemcpy(d_query, h_query, handle->dimensions * sizeof(uint16_t), cudaMemcpyHostToDevice);
    if (metric == 0) launch_l2_distance_fp16_kernel((uint16_t*)handle->buffers[1], d_query, d_distances, handle->dimensions, handle->vectorCount, 0);
    else launch_dot_distance_fp16_kernel((uint16_t*)handle->buffers[1], d_query, d_distances, handle->dimensions, handle->vectorCount, 0);
    
    launch_topk_kernel(d_distances, (int64_t*)handle->idBuffer, handle->vectorCount, k, d_outDist, d_outIDs, 0);

    cudaMemcpy(h_resultDistances, d_outDist, k * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy(h_resultIDs, d_outIDs, k * sizeof(int64_t), cudaMemcpyDeviceToHost);

    cudaFree(d_query); cudaFree(d_distances); cudaFree(d_outDist); cudaFree(d_outIDs);
    return 0;
}

int cuda_add_tq_vectors(CUDAIndexHandle* handle, unsigned char* h_tqData, int stride, int64_t* h_ids, int count) {
    void* buf = cuda_ensure_buffer(handle, 3, 1); // Stride handled manually
    if (!buf || count <= 0) return -1;
    if (handle->vectorCount + count > handle->capacity) return -2;

    size_t vOffset = (size_t)handle->vectorCount * stride;
    size_t iOffset = (size_t)handle->vectorCount * sizeof(int64_t);

    cudaMemcpyAsync((char*)handle->buffers[3] + vOffset, h_tqData, (size_t)count * stride, cudaMemcpyHostToDevice, handle->streams[0]);
    cudaMemcpyAsync((char*)handle->idBuffer + iOffset, h_ids, (size_t)count * sizeof(int64_t), cudaMemcpyHostToDevice, handle->streams[0]);
    cudaStreamSynchronize(handle->streams[0]);

    handle->vectorCount += count;
    return 0;
}

int cuda_search_tq(CUDAIndexHandle* handle, float* h_query, int k, int pow2, int bitsPerAngle, int64_t* h_resultIDs, float* h_resultDistances) {
    if (!handle->buffers[3] || handle->vectorCount == 0) return -1;

    float *d_query, *d_distances, *d_outDist;
    int64_t *d_outIDs;
    cudaMalloc(&d_query, handle->dimensions * sizeof(float));
    cudaMalloc(&d_distances, handle->vectorCount * sizeof(float));
    cudaMalloc(&d_outDist, k * sizeof(float));
    cudaMalloc(&d_outIDs, k * sizeof(int64_t));

    cudaMemcpy(d_query, h_query, handle->dimensions * sizeof(float), cudaMemcpyHostToDevice);
    launch_turboquant_distance_kernel(d_query, (const unsigned char*)handle->buffers[3], d_distances, handle->dimensions, pow2, bitsPerAngle, handle->vectorCount, 0);
    
    launch_topk_kernel(d_distances, (int64_t*)handle->idBuffer, handle->vectorCount, k, d_outDist, d_outIDs, 0);

    cudaMemcpy(h_resultDistances, d_outDist, k * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy(h_resultIDs, d_outIDs, k * sizeof(int64_t), cudaMemcpyDeviceToHost);

    cudaFree(d_query); cudaFree(d_distances); cudaFree(d_outDist); cudaFree(d_outIDs);
    return 0;
}

void cuda_get_ids(CUDAIndexHandle* handle, int64_t* h_ids, int count) {
    if (handle->idBuffer && count > 0) cudaMemcpy(h_ids, handle->idBuffer, (size_t)count * sizeof(int64_t), cudaMemcpyDeviceToHost);
}

int cuda_add_vectors_pq(CUDAIndexHandle* handle, unsigned char* h_codes, int64_t* h_ids, int count, int m) {
    void* buf = cuda_ensure_buffer(handle, 2, 1);
    if (!buf || count <= 0) return -1;
    if (handle->vectorCount + count > handle->capacity) return -2;

    size_t vOffset = (size_t)handle->vectorCount * m;
    size_t iOffset = (size_t)handle->vectorCount * sizeof(int64_t);

    cudaMemcpyAsync((char*)handle->buffers[2] + vOffset, h_codes, (size_t)count * m, cudaMemcpyHostToDevice, handle->streams[0]);
    cudaMemcpyAsync((char*)handle->idBuffer + iOffset, h_ids, (size_t)count * sizeof(int64_t), cudaMemcpyHostToDevice, handle->streams[0]);
    cudaStreamSynchronize(handle->streams[0]);

    handle->vectorCount += count;
    return 0;
}

int cuda_get_count(CUDAIndexHandle* handle) {
    return handle->vectorCount;
}

int cuda_search_pq(CUDAIndexHandle* handle, float* h_lookupTable, int m, int k, int64_t* h_resultIDs, float* h_resultDistances) {
    if (!handle->buffers[2] || handle->vectorCount == 0) return -1;

    float *d_table, *d_distances, *d_outDist;
    int64_t *d_outIDs;
    cudaMalloc(&d_table, m * 256 * sizeof(float));
    cudaMalloc(&d_distances, handle->vectorCount * sizeof(float));
    cudaMalloc(&d_outDist, k * sizeof(float));
    cudaMalloc(&d_outIDs, k * sizeof(int64_t));

    cudaMemcpy(d_table, h_lookupTable, m * 256 * sizeof(float), cudaMemcpyHostToDevice);
    launch_pq_distance_kernel(d_table, (unsigned char*)handle->buffers[2], d_distances, m, handle->vectorCount, 0);
    
    launch_topk_kernel(d_distances, (int64_t*)handle->idBuffer, handle->vectorCount, k, d_outDist, d_outIDs, 0);

    cudaMemcpy(h_resultDistances, d_outDist, k * sizeof(float), cudaMemcpyDeviceToHost);
    cudaMemcpy(h_resultIDs, d_outIDs, k * sizeof(int64_t), cudaMemcpyDeviceToHost);

    cudaFree(d_table); cudaFree(d_distances); cudaFree(d_outDist); cudaFree(d_outIDs);
    return 0;
}

void cuda_cleanup(CUDAIndexHandle* handle) {
    cuda_free(handle);
}

int cuda_update_graph(CUDAIndexHandle* handle, uint32_t* h_offsets, uint32_t* h_neighbors, float* h_weights, int nodeCount, int edgeCount) {
    if (handle->graphOffsets) cudaFree(handle->graphOffsets);
    if (handle->graphNeighbors) cudaFree(handle->graphNeighbors);
    if (handle->graphWeights) cudaFree(handle->graphWeights);

    cudaMalloc(&handle->graphOffsets, (nodeCount + 1) * sizeof(uint32_t));
    cudaMalloc(&handle->graphNeighbors, edgeCount * sizeof(uint32_t));
    if (h_weights) cudaMalloc(&handle->graphWeights, edgeCount * sizeof(float));

    cudaMemcpy(handle->graphOffsets, h_offsets, (nodeCount + 1) * sizeof(uint32_t), cudaMemcpyHostToDevice);
    cudaMemcpy(handle->graphNeighbors, h_neighbors, edgeCount * sizeof(uint32_t), cudaMemcpyHostToDevice);
    if (h_weights) cudaMemcpy(handle->graphWeights, h_weights, edgeCount * sizeof(float), cudaMemcpyHostToDevice);

    handle->graphNodeCount = nodeCount;
    handle->graphEdgeCount = edgeCount;
    return 0;
}
*/
import "C"
import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

type CUDAIndex struct {
	handle     *C.CUDAIndexHandle
	dim        int
	mu         sync.RWMutex
	closed     bool
	memPool    *memory.GPUMemPool
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
	handle := C.cuda_init(C.int(cfg.Dimension), C.int(initialCapacity))
	if handle == nil {
		return nil, &types.GPUInitializationError{
			DeviceID: cfg.DeviceID,
			Backend:  types.BackendCUDA,
			Cause:    fmt.Errorf("failed to initialize CUDA device"),
		}
	}

	nameBuf := make([]C.char, 256)
	var totalMem C.uint64_t
	C.cuda_get_device_info(handle, &nameBuf[0], C.int(len(nameBuf)), &totalMem)

	idx := &CUDAIndex{
		handle: handle,
		dim:    cfg.Dimension,
		deviceInfo: &types.GPUInfo{
			Backend:  types.BackendCUDA,
			Name:     C.GoString(&nameBuf[0]),
			DeviceID: cfg.DeviceID,
			MemoryMB: int64(totalMem) / (1024 * 1024),
		},
		lastSyncTime: time.Now(),
		stopSync:     make(chan struct{}),
		maxMemory:    cfg.MaxMemory,
	}

	pool, err := memory.NewGPUMemPool(types.BackendCUDA, cfg.DeviceID)
	if err == nil {
		idx.memPool = pool
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
	idx.batchMu.Lock()
	defer idx.batchMu.Unlock()

	if len(idx.batchIDs) == 0 {
		return nil
	}

	start := time.Now()
	batchCount := len(idx.batchIDs)

	idx.mu.RLock()
	currentCount := int(C.cuda_get_count(idx.handle))
	currentCapacity := int(idx.handle.capacity)
	idx.mu.RUnlock()

	requiredCapacity := currentCount + batchCount
	if requiredCapacity > currentCapacity {
		newCapacity := currentCapacity
		if newCapacity == 0 {
			newCapacity = 10000
		}
		for newCapacity < requiredCapacity {
			newCapacity *= 2
		}

		estimatedMem := int64(newCapacity) * int64(idx.dim) * 4
		estimatedMem += int64(newCapacity) * 8

		if idx.maxMemory > 0 && estimatedMem > idx.maxMemory {
			return &types.GPUSyncError{
				BatchSize: batchCount,
				DeviceID:  idx.deviceInfo.DeviceID,
				Cause:     fmt.Errorf("GPU memory limit exceeded: estimated %d bytes, limit %d", estimatedMem, idx.maxMemory),
			}
		}
	}

	ret := C.cuda_add_vectors(
		idx.handle,
		(*C.float)(unsafe.Pointer(&idx.batchVectors[0])),
		(*C.int64_t)(unsafe.Pointer(&idx.batchIDs[0])),
		C.int(len(idx.batchIDs)),
	)

	duration := time.Since(start)

	if ret != 0 {
		return &types.GPUSyncError{
			BatchSize: len(idx.batchIDs),
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("failed to add vectors to CUDA buffer"),
		}
	}

	metrics.RecordGPUSync(duration, len(idx.batchIDs))

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

	ret := C.cuda_add_vectors_pq(
		idx.handle,
		(*C.uchar)(unsafe.Pointer(&codes[0])),
		(*C.int64_t)(unsafe.Pointer(&ids[0])),
		C.int(len(ids)),
		C.int(m),
	)

	if ret != 0 {
		return fmt.Errorf("failed to add PQ vectors to CUDA buffer: error %d", int(ret))
	}

	return nil
}

func (idx *CUDAIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
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

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	for i := range resultDistances {
		resultDistances[i] = math.MaxFloat32
	}

	start := time.Now()
	ret := C.cuda_search(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, &types.GPUComputeError{
			Operation: "search",
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("CUDA search failed"),
		}
	}

	metrics.RecordGPUSearch(duration, "cuda", k)

	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) SearchPQ(lookupTable []float32, m int, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	start := time.Now()
	ret := C.cuda_search_pq(
		idx.handle,
		(*C.float)(unsafe.Pointer(&lookupTable[0])),
		C.int(m),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, fmt.Errorf("CUDA PQ search failed")
	}

	metrics.RecordGPUSearch(duration, "cuda_pq", k)
	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) TrainPQ(vectors []float32, m int, k int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	// CPU fallback: Train PQ codebooks using K-Means
	if idx.dim%m != 0 {
		return fmt.Errorf("dimension %d must be divisible by M %d", idx.dim, m)
	}

	encoder, err := pq.NewPQEncoder(idx.dim, m, k)
	if err != nil {
		return fmt.Errorf("failed to create PQ encoder: %w", err)
	}

	// Convert flat vectors to [][]float32
	numVecs := len(vectors) / idx.dim
	vecs2d := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vecs2d[i] = vectors[i*idx.dim : (i+1)*idx.dim]
	}

	if err := encoder.Train(vecs2d); err != nil {
		return fmt.Errorf("PQ training failed: %w", err)
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

func (idx *CUDAIndex) DeviceID() int {
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

	stride := len(tqData) / count

	ret := C.cuda_add_tq_vectors(
		idx.handle,
		(*C.uchar)(unsafe.Pointer(&tqData[0])),
		C.int(stride),
		(*C.int64_t)(unsafe.Pointer(&ids[0])),
		C.int(count),
	)

	if ret != 0 {
		return fmt.Errorf("failed to add TQ vectors to CUDA buffer")
	}

	return nil
}

func (idx *CUDAIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	pow2 := 1
	for pow2 < idx.dim {
		pow2 <<= 1
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.cuda_search_tq(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		C.int(k),
		C.int(pow2),
		C.int(bitsPerAngle),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("CUDA TQ search failed")
	}

	return resultIDs, resultDistances, nil
}

func (idx *CUDAIndex) Initialize(deviceID int) error {
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

func (idx *CUDAIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
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

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	for i := range resultDistances {
		resultDistances[i] = math.MaxFloat32
	}

	start := time.Now()
	ret := C.cuda_search_fp16(
		idx.handle,
		(*C.uint16_t)(unsafe.Pointer(&vector[0])),
		C.int(k),
		C.int(0),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, fmt.Errorf("CUDA float16 search failed")
	}

	metrics.RecordGPUSearch(duration, "cuda_fp16", k)

	return resultIDs, resultDistances, nil
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

func (idx *CUDAIndex) SearchWithFilter(query []float32, k int, bitset []uint64) ([]types.SearchResult, error) {
	if len(query) != idx.dimensions {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", idx.dimensions, len(query))
	}

	// 1. Upload query
	var d_query unsafe.Pointer
	cudaErr := C.cudaMalloc(&d_query, C.size_t(idx.dimensions*4))
	if cudaErr != C.cudaSuccess {
		return nil, fmt.Errorf("cudaMalloc query failed: %v", cudaErr)
	}
	defer C.cudaFree(d_query)
	C.cudaMemcpy(d_query, unsafe.Pointer(&query[0]), C.size_t(idx.dimensions*4), C.cudaMemcpyHostToDevice)

	// 2. Upload bitset if provided
	var d_bitset unsafe.Pointer
	if len(bitset) > 0 {
		bitsetSize := C.size_t(len(bitset) * 8)
		cudaErr = C.cudaMalloc(&d_bitset, bitsetSize)
		if cudaErr != C.cudaSuccess {
			return nil, fmt.Errorf("cudaMalloc bitset failed: %v", cudaErr)
		}
		defer C.cudaFree(d_bitset)
		C.cudaMemcpy(d_bitset, unsafe.Pointer(&bitset[0]), bitsetSize, C.cudaMemcpyHostToDevice)
	}

	// 3. Prepare distances buffer
	var d_distances unsafe.Pointer
	C.cudaMalloc(&d_distances, C.size_t(idx.vectorCount*4))
	defer C.cudaFree(d_distances)

	// 4. Launch fused kernel
	C.launch_l2_distance_filtered_kernel(
		(*C.float)(idx.handle.vectorBuffer),
		(*C.float)(d_query),
		(*C.float)(d_distances),
		(*C.unsigned_long_long)(d_bitset),
		C.int(idx.dimensions),
		C.int(idx.vectorCount),
		nil,
	)

	// 5. Download results
	h_distances := make([]float32, idx.vectorCount)
	C.cudaMemcpy(unsafe.Pointer(&h_distances[0]), d_distances, C.size_t(idx.vectorCount*4), C.cudaMemcpyDeviceToHost)

	// 6. Download IDs
	h_ids := make([]int64, idx.vectorCount)
	C.cuda_get_ids(idx.handle, (*C.int64_t)(unsafe.Pointer(&h_ids[0])), C.int(idx.vectorCount))

	// 7. Sort and return top-k
	results := make([]types.SearchResult, 0, k)
	for i, dist := range h_distances {
		if dist >= 1e29 { // Filtered out
			continue
		}
		results = append(results, types.SearchResult{
			ID:       types.VectorID(h_ids[i]),
			Distance: dist,
		})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Distance < results[j].Distance })
	if len(results) > k {
		results = results[:k]
	}

	return results, nil
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
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if idx.handle.graphOffsets == nil {
		return nil, nil, fmt.Errorf("graph not initialized on GPU")
	}

	nodeCount := int(idx.handle.graphNodeCount)
	
	// Allocate GPU buffers for BFS
	var d_frontier, d_nextFrontier *C.uint32_t
	var d_visited *C.ulonglong
	var d_activations, d_newActivations *C.float
	var d_nextSize *C.int

	C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_frontier)), C.size_t(nodeCount*4))
	C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_nextFrontier)), C.size_t(nodeCount*4))
	C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_visited)), C.size_t((nodeCount/64+1)*8))
	C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_activations)), C.size_t(nodeCount*4))
	C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_newActivations)), C.size_t(nodeCount*4))
	C.cudaMalloc((*unsafe.Pointer)(unsafe.Pointer(&d_nextSize)), 4)

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
			d_visited, d_nextFrontier, d_nextSize, 0,
		)

		C.launch_graph_activation_propagate_kernel(
			d_activations, d_newActivations,
			d_frontier, C.int(frontierSize),
			(*C.uint32_t)(idx.handle.graphOffsets),
			(*C.uint32_t)(idx.handle.graphNeighbors),
			(*C.float)(idx.handle.graphWeights),
			C.float(alpha), 0,
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
