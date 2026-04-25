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
    void* vectorBuffer;
    void* idBuffer;
    int vectorCount;
    int dimensions;
    int capacity;
} CUDAIndexHandle;

// Function declarations from kernels.cu
void launch_l2_distance_kernel(const float* vectors, const float* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_l2_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_dot_distance_fp16_kernel(const uint16_t* vectors, const uint16_t* query, float* distances, int dimensions, int count, cudaStream_t stream);
void launch_pq_distance_kernel(const float* lookupTable, const unsigned char* codes, float* distances, int m, int count, cudaStream_t stream);
void launch_turboquant_distance_kernel(const float* query, const unsigned char* tqData, float* distances, int dim, int pow2, int bitsPerAngle, int count, cudaStream_t stream);

CUDAIndexHandle* cuda_init(int dimensions, int initialCapacity) {
    int device = 0;
    cudaError_t err = cudaSetDevice(device);
    if (err != cudaSuccess) {
        return NULL;
    }

    CUDAIndexHandle* handle = (CUDAIndexHandle*)malloc(sizeof(CUDAIndexHandle));
    handle->device = device;
    handle->vectorBuffer = NULL;
    handle->idBuffer = NULL;
    handle->vectorCount = 0;
    handle->dimensions = dimensions;
    handle->capacity = initialCapacity > 0 ? initialCapacity : 10000;

    size_t bufferSize = handle->capacity * dimensions * sizeof(float);
    err = cudaMalloc((void**)&handle->vectorBuffer, bufferSize);
    if (err != cudaSuccess) {
        handle->vectorBuffer = NULL;
        free(handle);
        return NULL;
    }

    size_t idBufferSize = handle->capacity * sizeof(int64_t);
    err = cudaMalloc((void**)&handle->idBuffer, idBufferSize);
    if (err != cudaSuccess) {
        if (handle->vectorBuffer) cudaFree(handle->vectorBuffer);
        handle->vectorBuffer = NULL;
        free(handle);
        return NULL;
    }

    cudaMemset(handle->vectorBuffer, 0, bufferSize);
    cudaMemset(handle->idBuffer, 0, idBufferSize);

    return handle;
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

int cuda_add_vectors(CUDAIndexHandle* handle, float* h_vectors, int64_t* h_ids, int count) {
    if (!handle->vectorBuffer || count <= 0) {
        return -1;
    }

    int requiredCapacity = handle->vectorCount + count;
    if (requiredCapacity > handle->capacity) {
        int newCapacity = handle->capacity;
        while (newCapacity < requiredCapacity) {
            newCapacity *= 2;
        }

        size_t newBufferSize = newCapacity * handle->dimensions * sizeof(float);
        void* newVectorBuffer = NULL;
        cudaError_t err = cudaMalloc((void**)&newVectorBuffer, newBufferSize);
        if (err != cudaSuccess) {
            return -1;
        }

        if (handle->vectorCount > 0) {
            cudaMemcpy(newVectorBuffer, handle->vectorBuffer,
                       handle->vectorCount * handle->dimensions * sizeof(float),
                       cudaMemcpyDeviceToDevice);
        }

        cudaFree(handle->vectorBuffer);
        handle->vectorBuffer = newVectorBuffer;

        size_t newIdBufferSize = newCapacity * sizeof(int64_t);
        void* newIdBuffer = NULL;
        err = cudaMalloc((void**)&newIdBuffer, newIdBufferSize);
        if (err != cudaSuccess) {
            return -1;
        }

        if (handle->vectorCount > 0) {
            cudaMemcpy(newIdBuffer, handle->idBuffer,
                       handle->vectorCount * sizeof(int64_t),
                       cudaMemcpyDeviceToDevice);
        }

        cudaFree(handle->idBuffer);
        handle->idBuffer = newIdBuffer;
        handle->capacity = newCapacity;
    }

    size_t vectorSize = count * handle->dimensions * sizeof(float);
    void* vectorOffset = (char*)handle->vectorBuffer + handle->vectorCount * handle->dimensions * sizeof(float);
    cudaError_t err = cudaMemcpy(vectorOffset, h_vectors, vectorSize, cudaMemcpyHostToDevice);
    if (err != cudaSuccess) {
        return -1;
    }

    size_t idSize = count * sizeof(int64_t);
    void* idOffset = (char*)handle->idBuffer + handle->vectorCount * sizeof(int64_t);
    err = cudaMemcpy(idOffset, h_ids, idSize, cudaMemcpyHostToDevice);
    if (err != cudaSuccess) {
        return -1;
    }

    handle->vectorCount += count;
    return 0;
}

int cuda_add_vectors_fp16(CUDAIndexHandle* handle, uint16_t* h_vectors, int64_t* h_ids, int count) {
    if (!handle->vectorBuffer || count <= 0) {
        return -1;
    }

    int requiredCapacity = handle->vectorCount + count;
    if (requiredCapacity > handle->capacity) {
        int newCapacity = handle->capacity;
        while (newCapacity < requiredCapacity) {
            newCapacity *= 2;
        }

        // Each vector element is uint16_t (2 bytes)
        size_t newBufferSize = newCapacity * handle->dimensions * sizeof(uint16_t);
        void* newVectorBuffer = NULL;
        cudaError_t err = cudaMalloc((void**)&newVectorBuffer, newBufferSize);
        if (err != cudaSuccess) return -1;

        if (handle->vectorCount > 0) {
            cudaMemcpy(newVectorBuffer, handle->vectorBuffer,
                       handle->vectorCount * handle->dimensions * sizeof(uint16_t),
                       cudaMemcpyDeviceToDevice);
        }

        cudaFree(handle->vectorBuffer);
        handle->vectorBuffer = newVectorBuffer;

        size_t newIdBufferSize = newCapacity * sizeof(int64_t);
        void* newIdBuffer = NULL;
        err = cudaMalloc((void**)&newIdBuffer, newIdBufferSize);
        if (err != cudaSuccess) return -1;

        if (handle->vectorCount > 0) {
            cudaMemcpy(newIdBuffer, handle->idBuffer,
                       handle->vectorCount * sizeof(int64_t),
                       cudaMemcpyDeviceToDevice);
        }

        cudaFree(handle->idBuffer);
        handle->idBuffer = newIdBuffer;
        handle->capacity = newCapacity;
    }

    size_t vectorSize = count * handle->dimensions * sizeof(uint16_t);
    void* vectorOffset = (char*)handle->vectorBuffer + handle->vectorCount * handle->dimensions * sizeof(uint16_t);
    cudaError_t err = cudaMemcpy(vectorOffset, h_vectors, vectorSize, cudaMemcpyHostToDevice);
    if (err != cudaSuccess) return -1;

    size_t idSize = count * sizeof(int64_t);
    void* idOffset = (char*)handle->idBuffer + handle->vectorCount * sizeof(int64_t);
    err = cudaMemcpy(idOffset, h_ids, idSize, cudaMemcpyHostToDevice);
    if (err != cudaSuccess) return -1;

    handle->vectorCount += count;
    return 0;
}

int cuda_search(CUDAIndexHandle* handle, float* h_query, int k, int64_t* h_resultIDs, float* h_resultDistances) {
    if (!handle->vectorBuffer || handle->vectorCount == 0) {
        return -1;
    }

    float* d_query;
    float* d_distances;
    size_t querySize = handle->dimensions * sizeof(float);
    size_t distancesSize = handle->vectorCount * sizeof(float);
    
    cudaMalloc((void**)&d_query, querySize);
    cudaMalloc((void**)&d_distances, distancesSize);
    
    cudaMemcpy(d_query, h_query, querySize, cudaMemcpyHostToDevice);

    launch_l2_distance_kernel((float*)handle->vectorBuffer, d_query, d_distances, handle->dimensions, handle->vectorCount, 0);
    
    float* h_distances = (float*)malloc(distancesSize);
    cudaMemcpy(h_distances, d_distances, distancesSize, cudaMemcpyDeviceToHost);
    
    int n = handle->vectorCount;
    int resultCount = k < n ? k : n;
    int64_t* h_ids = (int64_t*)malloc(n * sizeof(int64_t));
    cudaMemcpy(h_ids, handle->idBuffer, n * sizeof(int64_t), cudaMemcpyDeviceToHost);

    for (int i = 0; i < resultCount; i++) {
        int minIdx = i;
        float minDist = h_distances[i];
        for (int j = i + 1; j < n; j++) {
            if (h_distances[j] < minDist) {
                minDist = h_distances[j];
                minIdx = j;
            }
        }
        h_resultIDs[i] = h_ids[minIdx];
        h_resultDistances[i] = minDist;
        h_distances[minIdx] = INFINITY;
    }

    cudaFree(d_query);
    cudaFree(d_distances);
    free(h_distances);
    free(h_ids);
    return 0;
}

int cuda_search_fp16(CUDAIndexHandle* handle, uint16_t* h_query, int k, int metric, int64_t* h_resultIDs, float* h_resultDistances) {
    if (!handle->vectorBuffer || handle->vectorCount == 0) {
        return -1;
    }

    uint16_t* d_query;
    float* d_distances;
    size_t querySize = handle->dimensions * sizeof(uint16_t);
    size_t distancesSize = handle->vectorCount * sizeof(float);
    
    cudaMalloc((void**)&d_query, querySize);
    cudaMalloc((void**)&d_distances, distancesSize);
    
    cudaMemcpy(d_query, h_query, querySize, cudaMemcpyHostToDevice);

    if (metric == 0) { // L2
        launch_l2_distance_fp16_kernel((uint16_t*)handle->vectorBuffer, d_query, d_distances, handle->dimensions, handle->vectorCount, 0);
    } else { // Dot
        launch_dot_distance_fp16_kernel((uint16_t*)handle->vectorBuffer, d_query, d_distances, handle->dimensions, handle->vectorCount, 0);
    }
    
    float* h_distances = (float*)malloc(distancesSize);
    cudaMemcpy(h_distances, d_distances, distancesSize, cudaMemcpyDeviceToHost);
    
    int n = handle->vectorCount;
    int resultCount = k < n ? k : n;
    int64_t* h_ids = (int64_t*)malloc(n * sizeof(int64_t));
    cudaMemcpy(h_ids, handle->idBuffer, n * sizeof(int64_t), cudaMemcpyDeviceToHost);

    for (int i = 0; i < resultCount; i++) {
        int bestIdx = i;
        float bestVal = h_distances[i];
        for (int j = i + 1; j < n; j++) {
            if (metric == 0) { // L2 (smaller is better)
                if (h_distances[j] < bestVal) {
                    bestVal = h_distances[j];
                    bestIdx = j;
                }
            } else { // Dot (larger is better)
                if (h_distances[j] > bestVal) {
                    bestVal = h_distances[j];
                    bestIdx = j;
                }
            }
        }
        h_resultIDs[i] = h_ids[bestIdx];
        h_resultDistances[i] = bestVal;
        h_distances[bestIdx] = (metric == 0) ? INFINITY : -INFINITY;
    }

    cudaFree(d_query);
    cudaFree(d_distances);
    free(h_distances);
    free(h_ids);
    return 0;
}

int cuda_search_pq(CUDAIndexHandle* handle, float* h_lookupTable, int m, int k, int64_t* h_resultIDs, float* h_resultDistances) {
    if (!handle->idBuffer || handle->vectorCount == 0) {
        return -1;
    }

    // Distance calculation on GPU
    float* d_table;
    float* d_distances;
    size_t tableSize = m * 256 * sizeof(float);
    size_t distancesSize = handle->vectorCount * sizeof(float);
    
    cudaMalloc((void**)&d_table, tableSize);
    cudaMalloc((void**)&d_distances, distancesSize);
    cudaMemcpy(d_table, h_lookupTable, tableSize, cudaMemcpyHostToDevice);

    // Launch PQ kernel
    // Codes are stored in handle->vectorBuffer as uint8 (if configured for PQ)
    // Actually, current CUDAIndex assumes float vectors. 
    // If it's a PQ index, it should store uint8 codes.
    
    launch_pq_distance_kernel(d_table, (unsigned char*)handle->vectorBuffer, d_distances, m, handle->vectorCount, 0);

    float* h_distances = (float*)malloc(distancesSize);
    cudaMemcpy(h_distances, d_distances, distancesSize, cudaMemcpyDeviceToHost);

    int n = handle->vectorCount;
    int resultCount = k < n ? k : n;
    int64_t* h_ids = (int64_t*)malloc(n * sizeof(int64_t));
    cudaMemcpy(h_ids, handle->idBuffer, n * sizeof(int64_t), cudaMemcpyDeviceToHost);

    for (int i = 0; i < resultCount; i++) {
        int minIdx = i;
        float minDist = h_distances[i];
        for (int j = i + 1; j < n; j++) {
            if (h_distances[j] < minDist) {
                minDist = h_distances[j];
                minIdx = j;
            }
        }
        h_resultIDs[i] = h_ids[minIdx];
        h_resultDistances[i] = minDist;
        h_distances[minIdx] = INFINITY;
    }

    cudaFree(d_table);
    cudaFree(d_distances);
    free(h_distances);
    free(h_ids);
    return 0;
}

int cuda_get_count(CUDAIndexHandle* handle) {
    return handle->vectorCount;
}

int cuda_add_tq_vectors(CUDAIndexHandle* handle, unsigned char* h_tqData, int stride, int64_t* h_ids, int count) {
    if (!handle->vectorBuffer || count <= 0) return -1;
    int requiredCapacity = handle->vectorCount + count;
    if (requiredCapacity > handle->capacity) {
        int newCapacity = handle->capacity > 0 ? handle->capacity : 1024;
        while (newCapacity < requiredCapacity) newCapacity *= 2;

        size_t newBufferSize = (size_t)newCapacity * stride;
        void* newVectorBuffer = NULL;
        if (cudaMalloc(&newVectorBuffer, newBufferSize) != cudaSuccess) return -1;
        if (handle->vectorCount > 0) {
            cudaMemcpy(newVectorBuffer, handle->vectorBuffer, (size_t)handle->vectorCount * stride, cudaMemcpyDeviceToDevice);
        }
        cudaFree(handle->vectorBuffer);
        handle->vectorBuffer = newVectorBuffer;

        size_t newIdBufferSize = (size_t)newCapacity * sizeof(int64_t);
        void* newIdBuffer = NULL;
        if (cudaMalloc(&newIdBuffer, newIdBufferSize) != cudaSuccess) return -1;
        if (handle->vectorCount > 0) {
            cudaMemcpy(newIdBuffer, handle->idBuffer, (size_t)handle->vectorCount * sizeof(int64_t), cudaMemcpyDeviceToDevice);
        }
        cudaFree(handle->idBuffer);
        handle->idBuffer = newIdBuffer;
        handle->capacity = newCapacity;
    }

    cudaMemcpy((unsigned char*)handle->vectorBuffer + (size_t)handle->vectorCount * stride, h_tqData, (size_t)count * stride, cudaMemcpyHostToDevice);
    cudaMemcpy((int64_t*)handle->idBuffer + handle->vectorCount, h_ids, (size_t)count * sizeof(int64_t), cudaMemcpyHostToDevice);
    handle->vectorCount += count;
    return 0;
}

int cuda_search_tq(CUDAIndexHandle* handle, float* h_query, int k, int pow2, int bitsPerAngle, int64_t* h_resultIDs, float* h_resultDistances) {
    if (!handle->vectorBuffer || handle->vectorCount == 0) return -1;
    float *d_query, *d_distances;
    cudaMalloc((void**)&d_query, handle->dimensions * sizeof(float));
    cudaMalloc((void**)&d_distances, handle->vectorCount * sizeof(float));

    cudaMemcpy(d_query, h_query, handle->dimensions * sizeof(float), cudaMemcpyHostToDevice);

    launch_turboquant_distance_kernel(d_query, (const unsigned char*)handle->vectorBuffer, d_distances, handle->dimensions, pow2, bitsPerAngle, handle->vectorCount, 0);

    float* h_distances = (float*)malloc(handle->vectorCount * sizeof(float));
    cudaMemcpy(h_distances, d_distances, handle->vectorCount * sizeof(float), cudaMemcpyDeviceToHost);
    int64_t* h_ids = (int64_t*)malloc(handle->vectorCount * sizeof(int64_t));
    cudaMemcpy(h_ids, handle->idBuffer, handle->vectorCount * sizeof(int64_t), cudaMemcpyDeviceToHost);

    int n = handle->vectorCount;
    int resultCount = k < n ? k : n;
    for (int i = 0; i < resultCount; i++) {
        int minIdx = i;
        float minDist = h_distances[i];
        for (int j = i + 1; j < n; j++) {
            if (h_distances[j] < minDist) {
                minDist = h_distances[j];
                minIdx = j;
            }
        }
        h_resultIDs[i] = h_ids[minIdx];
        h_resultDistances[i] = minDist;
        h_distances[minIdx] = INFINITY;
    }
    cudaFree(d_query);
    cudaFree(d_distances);
    free(h_distances);
    free(h_ids);
    return 0;
}

void cuda_cleanup(CUDAIndexHandle* handle) {
    if (handle->idBuffer) {
        cudaFree(handle->idBuffer);
    }
    if (handle->vectorBuffer) {
        cudaFree(handle->vectorBuffer);
    }
    free(handle);
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

