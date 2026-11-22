//go:build !gpu || !linux || !cuda

package cuvs

import (
	"context"
	"fmt"
)

// CUVSIndex is the cuVS-backed index placeholder for builds without cuVS support.
type CUVSIndex struct {
	dataset string
	dim     int
}

// NewCUVSIndex reports an error because cuVS requires the gpu, linux, and cuda build tags.
func NewCUVSIndex(dataset string, dim int) (*CUVSIndex, error) {
	return nil, fmt.Errorf("cuVS index not supported on this platform: build with -tags gpu,linux,cuda")
}

// Search reports an error because cuVS is unavailable in this build.
func (idx *CUVSIndex) Search(ctx context.Context, query []float32, k int) ([]int64, []float32, error) {
	return nil, nil, fmt.Errorf("cuVS index not supported on this platform")
}

// AddBatch reports an error because cuVS is unavailable in this build.
func (idx *CUVSIndex) AddBatch(ctx context.Context, ids []int64, vectors []float32) error {
	return fmt.Errorf("cuVS index not supported on this platform")
}

// Close is a no-op for the unsupported-build placeholder.
func (idx *CUVSIndex) Close() error {
	return nil
}
