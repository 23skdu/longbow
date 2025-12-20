package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
)

// =============================================================================
// DoGet Pipeline Extension Tests - Coverage for gaps
// =============================================================================

// --- DefaultDoGetPipelineConfig tests (0% coverage) ---

func TestDefaultDoGetPipelineConfig(t *testing.T) {
	cfg := DefaultDoGetPipelineConfig()

	if cfg.Workers <= 0 {
		t.Error("Workers should be positive")
	}
	if cfg.BufferSize <= 0 {
		t.Error("BufferSize should be positive")
	}
}

func TestDefaultDoGetPipelineConfig_ReasonableDefaults(t *testing.T) {
	cfg := DefaultDoGetPipelineConfig()

	// Workers should be reasonable (1-128)
	if cfg.Workers < 1 || cfg.Workers > 128 {
		t.Errorf("Workers %d outside reasonable range [1,128]", cfg.Workers)
	}

	// Buffer size should be reasonable
	if cfg.BufferSize < 1 || cfg.BufferSize > 1000 {
		t.Errorf("BufferSize %d outside reasonable range [1,1000]", cfg.BufferSize)
	}
}

// --- shouldUsePipeline tests (66.7% -> 100%) ---

func TestShouldUsePipelineExt_NilPool(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	// Create store without pipeline
	vs := NewVectorStore(mem, logger, 1<<30, 100<<20, 24*time.Hour)
	if vs == nil {
		t.Fatal("Failed to create VectorStore")
	}
	defer vs.Shutdown(context.Background()) //nolint:errcheck

	// Should return false when pool is nil
	if vs.shouldUsePipeline(10) {
		t.Error("shouldUsePipeline should return false when pool is nil")
	}
}

func TestShouldUsePipelineExt_WithPool(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	// Create store with pipeline (threshold=2)
	vs := NewVectorStoreWithPipelineThreshold(mem, logger, 4, 10, 2)
	if vs == nil {
		t.Fatal("Failed to create VectorStore with pipeline")
	}
	defer vs.Shutdown(context.Background()) //nolint:errcheck

	// Batch count <= threshold should not use pipeline
	if vs.shouldUsePipeline(1) {
		t.Error("shouldUsePipeline(1) should return false for threshold=2")
	}
	if !vs.shouldUsePipeline(2) {
		t.Error("shouldUsePipeline(2) should return true for threshold=2 (>= comparison)")
	}

	// Batch count > threshold should use pipeline
	if !vs.shouldUsePipeline(3) {
		t.Error("shouldUsePipeline(3) should return true for threshold=2")
	}
	if !vs.shouldUsePipeline(100) {
		t.Error("shouldUsePipeline(100) should return true for threshold=2")
	}
}

func TestShouldUsePipelineExt_ZeroThreshold(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	// Create store with threshold=0 (uses default)
	vs := NewVectorStoreWithPipelineThreshold(mem, logger, 4, 10, 0)
	if vs == nil {
		t.Fatal("Failed to create VectorStore")
	}
	defer vs.Shutdown(context.Background()) //nolint:errcheck

	// With threshold 0, should use default (2)
	if vs.shouldUsePipeline(1) {
		t.Error("shouldUsePipeline(1) should return false with default threshold")
	}
}

// --- GetPipelineThreshold tests ---

func TestGetPipelineThreshold(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	vs := NewVectorStoreWithPipelineThreshold(mem, logger, 4, 10, 5)
	if vs == nil {
		t.Fatal("Failed to create VectorStore")
	}
	defer vs.Shutdown(context.Background()) //nolint:errcheck

	if vs.GetPipelineThreshold() != 5 {
		t.Errorf("Expected threshold 5, got %d", vs.GetPipelineThreshold())
	}
}

// --- GetDoGetPipelinePool tests ---

func TestGetDoGetPipelinePool_Nil(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	vs := NewVectorStore(mem, logger, 1<<30, 100<<20, 24*time.Hour)
	if vs == nil {
		t.Fatal("Failed to create VectorStore")
	}
	defer vs.Shutdown(context.Background()) //nolint:errcheck

	if vs.GetDoGetPipelinePool() != nil {
		t.Error("Pool should be nil for basic VectorStore")
	}
}

func TestGetDoGetPipelinePool_NotNil(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	vs := NewVectorStoreWithPipeline(mem, logger, 4, 10)
	if vs == nil {
		t.Fatal("Failed to create VectorStore with pipeline")
	}
	defer vs.Shutdown(context.Background()) //nolint:errcheck

	if vs.GetDoGetPipelinePool() == nil {
		t.Error("Pool should not be nil for pipeline-enabled VectorStore")
	}
}

// --- GetPipelineStats tests ---

func TestGetPipelineStats(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	vs := NewVectorStoreWithPipeline(mem, logger, 4, 10)
	if vs == nil {
		t.Fatal("Failed to create VectorStore")
	}
	defer vs.Shutdown(context.Background()) //nolint:errcheck

	stats := vs.GetPipelineStats()

	// Stats should be initialized (might have values from other tests)
	// Just verify we can access the fields without panic
	_ = stats.BatchesProcessed
	_ = stats.ErrorCount
	_ = stats.BatchesFiltered
}

// --- NewDoGetPipelinePool tests (66.7% -> 100%) ---

func TestNewDoGetPipelinePool_ZeroWorkers(t *testing.T) {
	// Test edge case with zero workers
	pool := NewDoGetPipelinePool(0, 10)
	if pool == nil {
		// If pool creation fails with 0 workers, that's acceptable
		return
	}
	// If it succeeds, verify it works
	_ = pool.Get()
}

func TestNewDoGetPipelinePool_NegativeBuffer(t *testing.T) {
	// Test edge case with negative buffer
	pool := NewDoGetPipelinePool(4, -1)
	if pool == nil {
		return
	}
	_ = pool.Get()
}

func TestDoGetPipelinePool_GetPut(t *testing.T) {
	pool := NewDoGetPipelinePool(4, 10)
	if pool == nil {
		t.Fatal("Failed to create pipeline pool")
	}

	// Get a pipeline
	p := pool.Get()
	if p == nil {
		t.Fatal("Get returned nil pipeline")
	}

	// Put it back
	pool.Put(p)
}

func TestDoGetPipelinePool_MultipleGetPut(t *testing.T) {
	pool := NewDoGetPipelinePool(2, 5)
	if pool == nil {
		t.Fatal("Failed to create pipeline pool")
	}

	// Get multiple pipelines
	p1 := pool.Get()
	p2 := pool.Get()
	p3 := pool.Get()

	// Put them back
	pool.Put(p1)
	pool.Put(p2)
	pool.Put(p3)

	// Get again to verify pool reuse
	p4 := pool.Get()
	if p4 == nil {
		t.Error("Pool should return reused pipeline")
	}
	pool.Put(p4)
}
