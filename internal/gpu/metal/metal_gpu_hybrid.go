//go:build gpu && darwin && arm64 && cgo

package metal

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Accelerate -framework Metal -framework MetalPerformanceShaders -framework Foundation

#include "metal_bridge.h"
*/
import "C"
import (
	"fmt"
	"runtime"
	"runtime/cgo"
	"sort"
	"sync"
	"time"
	"unsafe"

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
)

//export goMetalBatchCallback
func goMetalBatchCallback(handleVal C.uintptr_t) {
	h := cgo.Handle(handleVal)
	ch := h.Value().(chan struct{})
	close(ch)
}

// MetalHybridIndex uses GPU for distances, CPU for selection
type MetalHybridIndex struct {
	handle    *C.MetalHybridIndex
	dim       int
	mu        sync.RWMutex
	closed    bool
	pqEncoder *pq.PQEncoder // CPU fallback for PQ operations
}

// NewMetalHybridIndex creates a hybrid Metal/CPU index
func NewMetalHybridIndex(cfg gputypes.GPUConfig) (gputypes.Index, error) {
	handle := C.metal_hybrid_init(C.int(cfg.Dimension))
	if handle == nil {
		return nil, fmt.Errorf("failed to initialize hybrid Metal device")
	}

	idx := &MetalHybridIndex{
		handle: handle,
		dim:    cfg.Dimension,
	}

	runtime.SetFinalizer(idx, (*MetalHybridIndex).Close)
	return idx, nil
}

// Add runs the add operation.
func (idx *MetalHybridIndex) Add(ids []int64, vectors []float32) error {
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

	ret := C.metal_hybrid_add(idx.handle, (*C.float)(unsafe.Pointer(&vectors[0])), C.int(n))
	if ret != 0 {
		return fmt.Errorf("failed to add vectors to hybrid Metal buffer")
	}

	return nil
}

// Search runs the search operation.
func (idx *MetalHybridIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	// Step 1: GPU computes all distances in parallel
	vectorCount := int(C.int(idx.handle.vectorCount))
	distances := make([]float32, vectorCount)

	ret := C.metal_hybrid_compute_distances(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		(*C.float)(unsafe.Pointer(&distances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("hybrid Metal distance computation failed")
	}

	// Step 2: CPU finds top-k using simple selection (could use vDSP for further optimization)
	ids := make([]int64, vectorCount)
	for i := 0; i < vectorCount; i++ {
		ids[i] = int64(i)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	for i := 0; i < k && i < vectorCount; i++ {
		minIdx := i
		minDist := distances[i]

		for j := i + 1; j < vectorCount; j++ {
			if distances[j] < minDist {
				minDist = distances[j]
				minIdx = j
			}
		}

		// Swap both distance and original ID
		if minIdx != i {
			distances[i], distances[minIdx] = distances[minIdx], distances[i]
			ids[i], ids[minIdx] = ids[minIdx], ids[i]
		}

		resultIDs[i] = ids[i]
		resultDistances[i] = distances[i]
	}

	return resultIDs, resultDistances, nil
}

// SearchBatch runs the search batch operation.
func (idx *MetalHybridIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
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

// Close runs the close operation.
func (idx *MetalHybridIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return nil
	}

	if idx.handle != nil {
		C.metal_hybrid_cleanup(idx.handle)
		idx.handle = nil
	}

	idx.closed = true
	return nil
}

// Backend runs the backend operation.
func (idx *MetalHybridIndex) Backend() gputypes.GPUBackend {
	return gputypes.BackendMetal
}

// DeviceID runs the device id operation.
func (idx *MetalHybridIndex) DeviceID() int32 {
	return 0
}

// GetDeviceInfo returns the device info.
func (idx *MetalHybridIndex) GetDeviceInfo() (*gputypes.GPUInfo, error) {
	return &gputypes.GPUInfo{
		Backend:  gputypes.BackendMetal,
		Name:     "Apple Silicon GPU",
		DeviceID: 0,
	}, nil
}

// GetMemoryInfo returns the memory info.
func (idx *MetalHybridIndex) GetMemoryInfo() (int64, int64, int64, error) {
	return 0, 0, 0, nil
}

// SearchPQ runs the search pq operation.
func (idx *MetalHybridIndex) SearchPQ(lookupTable []float32, m, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	vectorCount := int(C.int(idx.handle.vectorCount))
	if vectorCount == 0 {
		return []int64{}, []float32{}, nil
	}
	if k > vectorCount {
		k = vectorCount
	}

	distances := make([]float32, vectorCount)
	ret := C.metal_hybrid_compute_pq_distances(
		idx.handle,
		(*C.float)(unsafe.Pointer(&lookupTable[0])),
		C.int(m),
		(*C.float)(unsafe.Pointer(&distances[0])),
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal hybrid PQ search failed")
	}

	type result struct {
		id   int64
		dist float32
	}
	results := make([]result, vectorCount)
	for i := 0; i < vectorCount; i++ {
		results[i] = result{id: int64(i), dist: distances[i]}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].dist < results[j].dist
	})

	resIDs := make([]int64, k)
	resDists := make([]float32, k)
	for i := 0; i < k; i++ {
		resIDs[i] = results[i].id
		resDists[i] = results[i].dist
	}

	// Record metrics
	metrics.RecordGPUSearch(time.Duration(0), "metal_hybrid_pq", k) // Duration is 0 because it's measured inside C or not at all here

	return resIDs, resDists, nil
}

// TrainPQ runs the train pq operation.
func (idx *MetalHybridIndex) TrainPQ(vectors []float32, m, k int) error {
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

// EncodePQ runs the encode pq operation.
func (idx *MetalHybridIndex) EncodePQ(vectors []float32) ([]byte, error) {
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

// GetUtilization returns the utilization.
func (idx *MetalHybridIndex) GetUtilization() (float32, error) {
	return 50.0, nil
}
