package store


import (
	"testing"
)

// Config tests
func TestHybridPipelineConfigDefaults(t *testing.T) {
	cfg := DefaultHybridPipelineConfig()
	if cfg.Alpha != 0.5 {
		t.Errorf("expected Alpha=0.5, got %v", cfg.Alpha)
	}
	if cfg.RRFk != 60 {
		t.Errorf("expected RRFk=60, got %v", cfg.RRFk)
	}
	if cfg.FusionMode != FusionModeRRF {
		t.Errorf("expected FusionModeRRF")
	}
	if !cfg.UseColumnIndex {
		t.Error("expected UseColumnIndex=true")
	}
}

func TestHybridPipelineConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*HybridPipelineConfig)
		wantErr bool
	}{
		{"valid defaults", func(c *HybridPipelineConfig) {}, false},
		{"alpha below 0", func(c *HybridPipelineConfig) { c.Alpha = -0.1 }, true},
		{"alpha above 1", func(c *HybridPipelineConfig) { c.Alpha = 1.1 }, true},
		{"alpha=0 valid", func(c *HybridPipelineConfig) { c.Alpha = 0.0 }, false},
		{"alpha=1 valid", func(c *HybridPipelineConfig) { c.Alpha = 1.0 }, false},
		{"RRFk=0 invalid", func(c *HybridPipelineConfig) { c.RRFk = 0 }, true},
		{"invalid fusion mode", func(c *HybridPipelineConfig) { c.FusionMode = 99 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultHybridPipelineConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func TestFusionModeConstants(t *testing.T) {
	if FusionModeRRF != 0 {
		t.Error("FusionModeRRF should be 0")
	}
	if FusionModeLinear != 1 {
		t.Error("FusionModeLinear should be 1")
	}
	if FusionModeCascade != 2 {
		t.Error("FusionModeCascade should be 2")
	}
}

func TestHybridSearchQueryDefaults(t *testing.T) {
	q := DefaultHybridSearchQuery()
	if q.K != 10 {
		t.Errorf("expected K=10, got %d", q.K)
	}
}

func TestHybridSearchQueryValidation(t *testing.T) {
	tests := []struct {
		name    string
		query   HybridSearchQuery
		wantErr bool
	}{
		{"empty query invalid", HybridSearchQuery{K: 10}, true},
		{"vector only valid", HybridSearchQuery{Vector: []float32{1, 2, 3}, K: 10}, false},
		{"keyword only valid", HybridSearchQuery{KeywordQuery: "test", K: 10}, false},
		{"both valid", HybridSearchQuery{Vector: []float32{1}, KeywordQuery: "test", K: 10}, false},
		{"K=0 invalid", HybridSearchQuery{Vector: []float32{1}, K: 0}, true},
		{"K negative invalid", HybridSearchQuery{Vector: []float32{1}, K: -1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.query.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func TestNewHybridSearchPipeline(t *testing.T) {
	cfg := DefaultHybridPipelineConfig()
	p := NewHybridSearchPipeline(cfg)
	if p == nil {
		t.Fatal("expected non-nil pipeline")
	}
	if p.config.Alpha != 0.5 {
		t.Error("config not set")
	}
}

func TestPipelineSetters(t *testing.T) {
	p := NewHybridSearchPipeline(DefaultHybridPipelineConfig())

	// Set nil indexes - should not panic
	p.SetColumnIndex(nil)
	p.SetBM25Index(nil)
	p.SetHNSWIndex(nil)

	// Set real column index
	colIdx := NewColumnInvertedIndex()
	p.SetColumnIndex(colIdx)
	if p.columnIndex != colIdx {
		t.Error("column index not set")
	}

	// Set real BM25 index
	bm25 := NewBM25InvertedIndex(DefaultBM25Config())
	p.SetBM25Index(bm25)
	if p.bm25Index != bm25 {
		t.Error("BM25 index not set")
	}
}

func TestFuseRRF(t *testing.T) {
	dense := []SearchResult{
		{ID: 1, Score: 0.9},
		{ID: 2, Score: 0.8},
		{ID: 3, Score: 0.7},
	}
	sparse := []SearchResult{
		{ID: 2, Score: 5.0}, // overlap with dense
		{ID: 4, Score: 4.0},
		{ID: 5, Score: 3.0},
	}

	results := FuseRRF(dense, sparse, 60, 5)

	if len(results) == 0 {
		t.Fatal("expected results")
	}

	// ID 2 should rank higher (appears in both)
	found2 := false
	for i, r := range results {
		if r.ID == 2 {
			found2 = true
			if i > 1 {
				t.Errorf("ID 2 should rank high, got rank %d", i)
			}
		}
	}
	if !found2 {
		t.Error("ID 2 should be in results")
	}
}

func TestFuseRRFEmpty(t *testing.T) {
	results := FuseRRF(nil, nil, 60, 10)
	if len(results) != 0 {
		t.Error("expected empty results")
	}
}

func TestFuseLinear(t *testing.T) {
	dense := []SearchResult{
		{ID: 1, Score: 1.0},
		{ID: 2, Score: 0.5},
	}
	sparse := []SearchResult{
		{ID: 2, Score: 1.0}, // boost ID 2
		{ID: 3, Score: 0.5},
	}

	// alpha=0.5 means equal weight
	results := FuseLinear(dense, sparse, 0.5, 5)

	if len(results) == 0 {
		t.Fatal("expected results")
	}

	// Results should contain IDs 1,2,3
	ids := make(map[VectorID]bool)
	for _, r := range results {
		ids[r.ID] = true
	}
	if !ids[1] || !ids[2] || !ids[3] {
		t.Error("expected IDs 1,2,3 in results")
	}
}

func TestFuseCascade(t *testing.T) {
	exactIDs := map[VectorID]struct{}{1: {}, 2: {}}
	keyword := []SearchResult{{ID: 1, Score: 5.0}, {ID: 3, Score: 4.0}}
	vector := []SearchResult{{ID: 2, Score: 0.9}, {ID: 4, Score: 0.8}}

	results := FuseCascade(exactIDs, keyword, vector, 5)

	// Only IDs in exactIDs should appear
	for _, r := range results {
		if r.ID != 1 && r.ID != 2 {
			t.Errorf("unexpected ID %d (not in exact filter)", r.ID)
		}
	}
}

func TestFuseCascadeNoFilters(t *testing.T) {
	keyword := []SearchResult{{ID: 1, Score: 5.0}}
	vector := []SearchResult{{ID: 2, Score: 0.9}}

	// No exact filters - should combine all
	results := FuseCascade(nil, keyword, vector, 5)

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func BenchmarkFuseRRF(b *testing.B) {
	dense := make([]SearchResult, 100)
	sparse := make([]SearchResult, 100)
	for i := 0; i < 100; i++ {
		dense[i] = SearchResult{ID: VectorID(i), Score: float32(100 - i)}
		sparse[i] = SearchResult{ID: VectorID(i + 50), Score: float32(100 - i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FuseRRF(dense, sparse, 60, 10)
	}
}

func BenchmarkFuseLinear(b *testing.B) {
	dense := make([]SearchResult, 100)
	sparse := make([]SearchResult, 100)
	for i := 0; i < 100; i++ {
		dense[i] = SearchResult{ID: VectorID(i), Score: float32(100 - i)}
		sparse[i] = SearchResult{ID: VectorID(i + 50), Score: float32(100 - i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FuseLinear(dense, sparse, 0.5, 10)
	}
}
