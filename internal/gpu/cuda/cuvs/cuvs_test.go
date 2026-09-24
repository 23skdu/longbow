//go:build !gpu || !linux || !cuda

package cuvs

import (
	"context"
	"testing"
)

func TestCUVSStub(t *testing.T) {
	idx, err := NewCUVSIndex("test", 128)
	if err == nil || idx != nil {
		t.Errorf("expected error from NewCUVSIndex on stub, got %v", idx)
	}

	stubIdx := &CUVSIndex{dataset: "test", dim: 128}
	if _, _, err := stubIdx.Search(context.Background(), []float32{1, 2}, 1); err == nil {
		t.Errorf("expected error from Search on stub")
	}

	if err := stubIdx.AddBatch(context.Background(), nil, nil); err == nil {
		t.Errorf("expected error from AddBatch on stub")
	}

	if err := stubIdx.Close(); err != nil {
		t.Errorf("Close on stub failed: %v", err)
	}
}
