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

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (idx *MetalIndex) SearchInt8(vector []int8, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalIndex) SearchUint8(vector []uint8, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalIndex) SearchInt16(vector []int16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalIndex) SearchUint16(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}
	// Note: uint16 here is treated as an integer type, not float16.
	// For float16, use SearchFloat16.
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float32(v)
	}
	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// Convert uint16 (fp16) to float32 for search
	f32Vec := make([]float32, len(vector))
	for i, v := range vector {
		f32Vec[i] = float16.FromBits(v).Float32()
	}

	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// Convert uint16 pairs (complex64 stored as fp16 pairs) to float32
	// complex64 has 2 elements for every float32 dims
	f32Vec := make([]float32, len(vector)*2)
	for i, v := range vector {
		f32Vec[i*2] = float16.FromBits(v).Float32()
	}

	return idx.searchFloat32(f32Vec, k)
}

func (idx *MetalIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	// complex128 is stored as interleaved float32 (real, imag pairs)
	// If input is 128 floats, that's 64 complex numbers
	// Just use as-is since Metal expects float32
	return idx.searchFloat32(vector, k)
}

func (idx *MetalIndex) searchFloat32(vector []float32, k int) ([]int64, []float32, error) {
	if len(vector) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(vector), idx.dim)
	}

	resultIDs := make([]int64, k)
	resultDistances := make([]float32, k)

	// Initialize distances to infinity
	for i := range resultDistances {
		resultDistances[i] = math.MaxFloat32
	}

	start := time.Now()
	ret := C.metal_search(
		idx.handle,
		(*C.float)(unsafe.Pointer(&vector[0])),
		C.int(k),
		(*C.int64_t)(unsafe.Pointer(&resultIDs[0])),
		(*C.float)(unsafe.Pointer(&resultDistances[0])),
	)
	duration := time.Since(start)

	if ret != 0 {
		return nil, nil, &gputypes.GPUComputeError{
			Operation: "search",
			DeviceID:  idx.deviceInfo.DeviceID,
			Cause:     fmt.Errorf("Metal search failed"),
		}
	}

	// Record metrics
	metrics.RecordGPUSearch(duration, "metal", k)

	return resultIDs, resultDistances, nil
}

