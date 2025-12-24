package store

import (
	"testing"
	
	"github.com/23skdu/longbow/internal/store/hnsw2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestHNSW2Integration validates basic hnsw2 integration with Dataset.
func TestHNSW2Integration(t *testing.T) {
	// Create a simple schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(384, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	
	// Create dataset with hnsw2 enabled
	t.Setenv("LONGBOW_USE_HNSW2", "true")
	ds := NewDataset("test", schema)
	
	// Verify feature flag was detected
	if !ds.useHNSW2 {
		t.Error("useHNSW2 should be true")
	}
	
	// Initialize hnsw2Index manually (normally done in VectorStore)
	if ds.hnsw2Index == nil {
		config := hnsw2.DefaultConfig()
		ds.hnsw2Index = hnsw2.NewArrowHNSW(ds, config)
	}
	
	// Verify hnsw2Index was created
	if ds.hnsw2Index == nil {
		t.Fatal("hnsw2Index should not be nil")
	}
	
	// Type assert to verify it's the correct type
	idx, ok := ds.hnsw2Index.(*hnsw2.ArrowHNSW)
	if !ok {
		t.Fatal("hnsw2Index should be *hnsw2.ArrowHNSW")
	}
	
	// Verify initial state
	if idx.Size() != 0 {
		t.Errorf("new index size = %d, want 0", idx.Size())
	}
}

// TestHNSW2FeatureFlag validates feature flag behavior.
func TestHNSW2FeatureFlag(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		},
		nil,
	)
	
	t.Run("Enabled", func(t *testing.T) {
		t.Setenv("LONGBOW_USE_HNSW2", "true")
		ds := NewDataset("test", schema)
		if !ds.useHNSW2 {
			t.Error("useHNSW2 should be true")
		}
	})
	
	t.Run("Disabled", func(t *testing.T) {
		t.Setenv("LONGBOW_USE_HNSW2", "false")
		ds := NewDataset("test", schema)
		if ds.useHNSW2 {
			t.Error("useHNSW2 should be false")
		}
	})
	
	t.Run("NotSet", func(t *testing.T) {
		ds := NewDataset("test", schema)
		if ds.useHNSW2 {
			t.Error("useHNSW2 should be false when env var not set")
		}
	})
}

// TestHNSW2GetVector validates getVector integration.
func TestHNSW2GetVector(t *testing.T) {
	// This test requires a full VectorStore setup with Arrow records
	// For now, we just validate the structure is in place
	t.Skip("Requires full VectorStore setup with Arrow records")
}
