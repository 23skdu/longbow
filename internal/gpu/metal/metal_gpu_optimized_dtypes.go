//go:build gpu && darwin && arm64 && cgo

package metal

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// SearchInt8 runs the search int8 operation.
func (idx *MetalIndexOptimized) SearchInt8(query []int8, k int) ([]int64, []float32, error) {
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float32(v)
	}
	return idx.Search(f32, k)
}

// SearchUint8 runs the search uint8 operation.
func (idx *MetalIndexOptimized) SearchUint8(query []uint8, k int) ([]int64, []float32, error) {
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float32(v)
	}
	return idx.Search(f32, k)
}

// SearchInt16 runs the search int16 operation.
func (idx *MetalIndexOptimized) SearchInt16(query []int16, k int) ([]int64, []float32, error) {
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float32(v)
	}
	return idx.Search(f32, k)
}

// SearchUint16 runs the search uint16 operation.
func (idx *MetalIndexOptimized) SearchUint16(query []uint16, k int) ([]int64, []float32, error) {
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float32(v)
	}
	return idx.Search(f32, k)
}

// SearchFloat16 runs the search float16 operation.
func (idx *MetalIndexOptimized) SearchFloat16(query []uint16, k int) ([]int64, []float32, error) {
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float16.FromBits(v).Float32()
	}
	return idx.Search(f32, k)
}

// SearchComplex64 runs the search complex64 operation.
func (idx *MetalIndexOptimized) SearchComplex64(query []uint16, k int) ([]int64, []float32, error) {
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	f32 := make([]float32, len(query))
	for i, v := range query {
		f32[i] = float16.FromBits(v).Float32()
	}
	return idx.Search(f32, k)
}

// SearchComplex128 runs the search complex128 operation.
func (idx *MetalIndexOptimized) SearchComplex128(query []float32, k int) ([]int64, []float32, error) {
	if len(query) != idx.dim {
		return nil, nil, fmt.Errorf("query vector dimension %d does not match index dimension %d", len(query), idx.dim)
	}
	return idx.Search(query, k)
}

// SearchBatch queries the optimized Metal GPU index with multiple vectors in parallel.
// This improves GPU utilization by batching multiple queries.
