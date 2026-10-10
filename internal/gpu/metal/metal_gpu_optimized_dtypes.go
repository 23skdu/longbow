//go:build gpu && darwin && arm64 && cgo

package metal

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/memory"
	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (idx *MetalIndexOptimized) SearchInt8(query []int8, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float32(v)
	}
	return idx.Search(f32, k)
}

func (idx *MetalIndexOptimized) SearchUint8(query []uint8, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float32(v)
	}
	return idx.Search(f32, k)
}

func (idx *MetalIndexOptimized) SearchInt16(query []int16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float32(v)
	}
	return idx.Search(f32, k)
}

func (idx *MetalIndexOptimized) SearchUint16(query []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float32(v)
	}
	return idx.Search(f32, k)
}

func (idx *MetalIndexOptimized) SearchFloat16(query []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.metal_search_typed(
		idx.handle,
		unsafe.Pointer(&query[0]),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
		vecTypeF16,
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal float16 search failed")
	}

	return resultIDs, resultDistances, nil
}

func (idx *MetalIndexOptimized) SearchComplex64(query []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.metal_search_typed(
		idx.handle,
		unsafe.Pointer(&query[0]),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
		vecTypeC64,
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal complex64 search failed")
	}

	return resultIDs, resultDistances, nil
}

func (idx *MetalIndexOptimized) SearchComplex128(query []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	ret := C.metal_search_typed(
		idx.handle,
		unsafe.Pointer(&query[0]),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
		vecTypeC128,
	)

	if ret != 0 {
		return nil, nil, fmt.Errorf("Metal complex128 search failed")
	}

	return resultIDs, resultDistances, nil
}

// SearchBatch queries the optimized Metal GPU index with multiple vectors in parallel.
// This improves GPU utilization by batching multiple queries.
