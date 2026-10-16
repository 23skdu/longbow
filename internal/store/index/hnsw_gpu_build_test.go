//go:build gpu

package index

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

func TestGPUBatchBuildConfig(t *testing.T) {
	config := DefaultGPUBatchBuildConfig()

	if config.BatchSize <= 0 {
		t.Errorf("Expected positive BatchSize, got %d", config.BatchSize)
	}
	if config.ParallelSearch <= 0 {
		t.Errorf("Expected positive ParallelSearch, got %d", config.ParallelSearch)
	}
	if config.SyncInterval <= 0 {
		t.Errorf("Expected positive SyncInterval, got %v", config.SyncInterval)
	}
}

func TestNewGPUBatchBuilder_NilIndex(t *testing.T) {
	logger := zerolog.Nop()
	config := DefaultGPUBatchBuildConfig()

	_, err := NewGPUBatchBuilder(nil, config, logger)
	if err == nil {
		t.Error("Expected error for nil index")
	}
}

func TestBatchInsertWithGPU_CPUFallback(t *testing.T) {
	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeFloat32
	config.Dims = 128

	ds := &MockDataset{
		Schema: nil,
	}
	index := NewArrowHNSW(ds, &config, nil)

	vectors := [][]float32{
		make([]float32, 128),
		make([]float32, 128),
	}
	for i := range vectors[0] {
		vectors[0][i] = float32(i) / 128.0
		vectors[1][i] = float32(i+1) / 128.0
	}

	ids := []uint32{0, 1}

	err := index.BatchInsertWithGPU(context.Background(), ids, vectors, -1)
	if err != nil {
		t.Errorf("BatchInsertWithGPU failed: %v", err)
	}

	if index.Len() != 2 {
		t.Errorf("Expected 2 vectors inserted, got %d", index.Len())
	}
}

func TestBuildIndexWithGPU_CPUFallback(t *testing.T) {
	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeFloat32
	config.Dims = 128

	ds := &MockDataset{
		Schema: nil,
	}
	index := NewArrowHNSW(ds, &config, nil)

	nVectors := 100
	vectors := make([][]float32, nVectors)
	ids := make([]uint32, nVectors)

	for i := 0; i < nVectors; i++ {
		vectors[i] = make([]float32, 128)
		for j := range vectors[i] {
			vectors[i][j] = float32(i*128+j) / float32(nVectors*128)
		}
		ids[i] = uint32(i)
	}

	buildConfig := DefaultGPUBatchBuildConfig()
	buildConfig.BatchSize = 10

	ctx := context.Background()
	logger := zerolog.Nop()

	err := index.BuildIndexWithGPU(ctx, vectors, ids, buildConfig, logger)
	if err != nil {
		t.Errorf("BuildIndexWithGPU failed: %v", err)
	}

	if index.Len() != nVectors {
		t.Errorf("Expected %d vectors inserted, got %d", nVectors, index.Len())
	}
}
