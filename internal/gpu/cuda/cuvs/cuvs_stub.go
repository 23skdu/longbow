//go:build !gpu || !linux || !cuda

package cuvs

import (
	"context"
	"fmt"
)

type CUVSIndex struct {
	dataset string
	dim     int
}

func NewCUVSIndex(dataset string, dim int) (*CUVSIndex, error) {
	return nil, fmt.Errorf("cuVS index not supported on this platform: build with -tags gpu,linux,cuda")
}

func (idx *CUVSIndex) Search(ctx context.Context, query []float32, k int) ([]int64, []float32, error) {
	return nil, nil, fmt.Errorf("cuVS index not supported on this platform")
}

func (idx *CUVSIndex) AddBatch(ctx context.Context, ids []int64, vectors []float32) error {
	return fmt.Errorf("cuVS index not supported on this platform")
}

func (idx *CUVSIndex) Close() error {
	return nil
}
