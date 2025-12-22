package store

import (
	"go.uber.org/zap"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// Subtask 4: VectorStore Hybrid Search Integration Tests
// =============================================================================

func TestVectorStoreHybridSearchConfig(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	hybridCfg := DefaultHybridSearchConfig()
	hybridCfg.Enabled = true
	hybridCfg.TextColumns = []string{"description"}
	hybridCfg.Alpha = 0.7

	store, err := NewVectorStoreWithHybridConfig(mem, logger, hybridCfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer func() { _ = store.Close() }()

	cfg := store.GetHybridSearchConfig()
	if !cfg.Enabled {
		t.Error("expected hybrid search to be enabled")
	}
	if cfg.Alpha != 0.7 {
		t.Errorf("expected alpha=0.7, got %f", cfg.Alpha)
	}
	if len(cfg.TextColumns) != 1 || cfg.TextColumns[0] != "description" {
		t.Errorf("expected TextColumns=[description], got %v", cfg.TextColumns)
	}
}

func TestVectorStoreHybridSearchDisabled(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	store := NewVectorStore(mem, logger, 1<<30, 1<<28, 5*time.Minute)
	defer func() { _ = store.Close() }()

	cfg := store.GetHybridSearchConfig()
	if cfg.Enabled {
		t.Error("expected hybrid search to be disabled by default")
	}
}

func TestVectorStoreBM25IndexCreation(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	hybridCfg := DefaultHybridSearchConfig()
	hybridCfg.Enabled = true
	hybridCfg.TextColumns = []string{"title", "body"}

	store, err := NewVectorStoreWithHybridConfig(mem, logger, hybridCfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if store.GetBM25Index() == nil {
		t.Error("expected BM25 index to be created when hybrid search is enabled")
	}
}

func TestVectorStoreBM25IndexNilWhenDisabled(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	hybridCfg := DefaultHybridSearchConfig()
	hybridCfg.Enabled = false

	store, err := NewVectorStoreWithHybridConfig(mem, logger, hybridCfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer func() { _ = store.Close() }()

	if store.GetBM25Index() != nil {
		t.Error("expected BM25 index to be nil when hybrid search is disabled")
	}
}

func TestVectorStoreHybridConfigValidation(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	hybridCfg := DefaultHybridSearchConfig()
	hybridCfg.Enabled = true
	hybridCfg.TextColumns = []string{"text"}
	hybridCfg.Alpha = 1.5 // Invalid: > 1.0

	_, err := NewVectorStoreWithHybridConfig(mem, logger, hybridCfg)
	if err == nil {
		t.Error("expected error for invalid alpha")
	}
}

func TestVectorStoreIndexTextColumns(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	hybridCfg := DefaultHybridSearchConfig()
	hybridCfg.Enabled = true
	hybridCfg.TextColumns = []string{"description"}

	store, err := NewVectorStoreWithHybridConfig(mem, logger, hybridCfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer func() { _ = store.Close() }()

	batch := buildTestRecordBatchWithText(mem, []string{"hello world test"}, "description")
	defer batch.Release()

	store.indexTextColumnsForHybridSearch(batch, 0)

	bm25 := store.GetBM25Index()
	if bm25 == nil {
		t.Fatal("BM25 index is nil")
	}

	results := bm25.SearchBM25("hello", 10)
	if len(results) == 0 {
		t.Error("expected to find 'hello' in BM25 index")
	}
}

// buildTestRecordBatchWithText creates a test RecordBatch with a string column
func buildTestRecordBatchWithText(mem memory.Allocator, texts []string, colName string) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: colName, Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.StringBuilder).AppendValues(texts, nil)

	return bldr.NewRecordBatch()
}
