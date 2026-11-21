//go:build gpu && darwin && arm64 && cgo

package metal

/*
#include "metal_bridge.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/pq"
)

func (idx *MetalIndexOptimized) Backend() types.GPUBackend {
	return types.BackendMetal
}

func (idx *MetalIndexOptimized) DeviceID() int32 {
	return 0
}

func (idx *MetalIndexOptimized) GetDeviceInfo() (*types.GPUInfo, error) {
	return &types.GPUInfo{
		Backend:  types.BackendMetal,
		Name:     "Apple Silicon GPU (Optimized)",
		DeviceID: 0,
	}, nil
}

func (idx *MetalIndexOptimized) GetMemoryInfo() (int64, int64, int64, error) {
	return 0, 0, 0, nil
}

func (idx *MetalIndexOptimized) AddPQ(ids []int64, codes []byte, m int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	if len(ids) == 0 {
		return nil
	}

	ret := C.metal_add_pq(idx.handle, (*C.uchar)(unsafe.Pointer(&codes[0])), (*C.int64_t)(unsafe.Pointer(&ids[0])), C.int(len(ids)), C.int(m))
	if ret != 0 {
		return fmt.Errorf("failed to add PQ vectors to Metal: error %d", int(ret))
	}

	return nil
}

func (idx *MetalIndexOptimized) SearchPQ(lookupTable []float32, m, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	resIDs := make([]int64, k)
	resDists := make([]float32, k)

	ret := C.metal_search_pq_optimized(idx.handle, (*C.float)(unsafe.Pointer(&lookupTable[0])), C.int(m), C.int(k), (*C.int64_t)(unsafe.Pointer(&resIDs[0])), (*C.float)(unsafe.Pointer(&resDists[0])))
	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal PQ search failed: error %d", int(ret))
	}

	return resIDs, resDists, nil
}

func (idx *MetalIndexOptimized) TrainPQ(vectors []float32, m, k int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	encoder, err := pq.NewPQEncoder(idx.dim, m, k)
	if err != nil {
		return err
	}

	numVecs := len(vectors) / idx.dim
	vecs2d := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vecs2d[i] = vectors[i*idx.dim : (i+1)*idx.dim]
	}

	if err := encoder.Train(vecs2d); err != nil {
		return err
	}

	idx.pqEncoder = encoder
	return nil
}

func (idx *MetalIndexOptimized) EncodePQ(vectors []float32) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.pqEncoder == nil {
		return nil, fmt.Errorf("PQ encoder not trained")
	}

	numVecs := len(vectors) / idx.dim
	codes := make([]byte, numVecs*idx.pqEncoder.M)
	for i := 0; i < numVecs; i++ {
		vec := vectors[i*idx.dim : (i+1)*idx.dim]
		encoded, err := idx.pqEncoder.Encode(vec)
		if err != nil {
			return nil, err
		}
		copy(codes[i*idx.pqEncoder.M:], encoded)
	}
	return codes, nil
}

func (idx *MetalIndexOptimized) GetUtilization() (float32, error) {
	return 50.0, nil
}

func (idx *MetalIndexOptimized) Sync() error {
	return nil
}

func (idx *MetalIndexOptimized) Clear() error {
	idx.batchMu.Lock()
	idx.batchIDs = idx.batchIDs[:0]
	idx.batchVectors = idx.batchVectors[:0]
	idx.batchMu.Unlock()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	// Reset vector counts and ID list
	idx.vectorCount = 0
	idx.idList = idx.idList[:0]
	if idx.handle != nil {
		idx.handle.vectorCount = 0
	}

	// Reset graph metadata
	idx.graphOffsets = nil
	idx.graphNeighbors = nil
	idx.graphWeights = nil

	return nil
}

func (idx *MetalIndexOptimized) Reset() error {
	return idx.Clear()
}
