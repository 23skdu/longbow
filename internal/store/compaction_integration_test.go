package store


import (
	"github.com/rs/zerolog"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestVectorStoreCompactionConfig tests compaction config on VectorStore
func TestVectorStoreCompactionConfig(t *testing.T) {
	store := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<20, time.Hour)
	defer func() { _ = store.Close() }()

	cfg := store.GetCompactionConfig()
	if cfg.TargetBatchSize != 10000 {
		t.Errorf("expected default TargetBatchSize 10000, got %d", cfg.TargetBatchSize)
	}
	if cfg.MinBatchesToCompact != 10 {
		t.Errorf("expected default MinBatchesToCompact 10, got %d", cfg.MinBatchesToCompact)
	}
}

// TestVectorStoreCompactionWorkerStartStop tests worker lifecycle
func TestVectorStoreCompactionWorkerStartStop(t *testing.T) {
	store := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<20, time.Hour)

	// Worker should start automatically with store
	if !store.IsCompactionRunning() {
		t.Error("compaction worker should be running after store creation")
	}

	// Close should stop worker
	err := store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if store.IsCompactionRunning() {
		t.Error("compaction worker should stop after Close")
	}
}

// TestVectorStoreCompactionStats tests compaction statistics
func TestVectorStoreCompactionStats(t *testing.T) {
	store := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<20, time.Hour)
	defer func() { _ = store.Close() }()

	stats := store.GetCompactionStats()
	if stats.CompactionsRun < 0 {
		t.Error("CompactionsRun should be non-negative")
	}
}

// TestVectorStoreCompactDatasetNotFound tests compaction of non-existent dataset
func TestVectorStoreCompactDatasetNotFound(t *testing.T) {
	store := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 1<<20, time.Hour)
	defer func() { _ = store.Close() }()

	err := store.CompactDataset("non_existent")
	if err == nil {
		t.Error("expected error for non-existent dataset")
	}
}
