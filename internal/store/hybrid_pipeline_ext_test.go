package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/query"
)

// ========== Search Method Tests ==========

func TestHybridSearchPipeline_SearchInvalidQuery(t *testing.T) {
	p := NewHybridSearchPipeline(DefaultHybridPipelineConfig())

	// Invalid query - no vector or keyword
	q := &HybridSearchQuery{K: 10}
	_, err := p.Search(q)
	if err == nil {
		t.Error("expected error for invalid query")
	}
}

func TestHybridSearchPipeline_SearchEmptyIndexes(t *testing.T) {
	p := NewHybridSearchPipeline(DefaultHybridPipelineConfig())

	q := &HybridSearchQuery{
		Vector:       []float32{1.0, 2.0, 3.0},
		KeywordQuery: "test",
		K:            10,
	}

	// Should return empty results when no indexes set
	results, err := p.Search(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty results, got %d", len(results))
	}
}

func TestHybridSearchPipeline_AlphaOverride(t *testing.T) {
	cfg := DefaultHybridPipelineConfig()
	cfg.Alpha = 0.5
	p := NewHybridSearchPipeline(cfg)

	// With override
	var override float32 = 0.98
	q := &HybridSearchQuery{
		Vector:        []float32{1.0, 2.0, 3.0},
		K:             10,
		AlphaOverride: &override,
	}

	_, err := p.Search(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Test passes if no panic - alpha override handled
}

func TestHybridSearchPipeline_SearchVectorOnly(t *testing.T) {
	cfg := DefaultHybridPipelineConfig()
	cfg.Alpha = 1.0 // Pure vector
	p := NewHybridSearchPipeline(cfg)

	q := &HybridSearchQuery{
		Vector: []float32{1.0, 2.0, 3.0},
		K:      10,
	}

	_, err := p.Search(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHybridSearchPipeline_SearchKeywordOnly(t *testing.T) {
	cfg := DefaultHybridPipelineConfig()
	cfg.Alpha = 0.0 // Pure keyword
	p := NewHybridSearchPipeline(cfg)

	q := &HybridSearchQuery{
		KeywordQuery: "hello world",
		K:            10,
	}

	_, err := p.Search(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHybridSearchPipeline_FusionModes(t *testing.T) {
	modes := []FusionMode{FusionModeRRF, FusionModeLinear, FusionModeCascade}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			cfg := DefaultHybridPipelineConfig()
			cfg.FusionMode = mode
			p := NewHybridSearchPipeline(cfg)

			q := &HybridSearchQuery{
				Vector:       []float32{1.0, 2.0},
				KeywordQuery: "test",
				K:            5,
			}

			_, err := p.Search(q)
			if err != nil {
				t.Fatalf("unexpected error for mode %d: %v", mode, err)
			}
		})
	}
}

// FusionMode.String helper for test names
func (m FusionMode) String() string {
	switch m {
	case FusionModeRRF:
		return "RRF"
	case FusionModeLinear:
		return "Linear"
	case FusionModeCascade:
		return "Cascade"
	default:
		return "Unknown"
	}
}

// ========== Fusion Edge Cases ==========

func TestFuseRRF_LimitTruncation(t *testing.T) {
	dense := make([]SearchResult, 20)
	for i := 0; i < 20; i++ {
		dense[i] = SearchResult{ID: VectorID(i), Score: float32(20 - i)}
	}

	results := FuseRRF(dense, nil, 60, 5)
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
}

func TestFuseRRF_SingleSource(t *testing.T) {
	dense := []SearchResult{
		{ID: 1, Score: 0.9},
		{ID: 2, Score: 0.8},
	}

	// Only dense, no sparse
	results := FuseRRF(dense, nil, 60, 10)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Only sparse, no dense
	results = FuseRRF(nil, dense, 60, 10)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestFuseLinear_Empty(t *testing.T) {
	results := FuseLinear(nil, nil, 0.5, 10)
	if len(results) != 0 {
		t.Error("expected empty results")
	}
}

func TestFuseLinear_PureKeyword(t *testing.T) {
	dense := []SearchResult{{ID: 1, Score: 1.0}}
	sparse := []SearchResult{{ID: 2, Score: 1.0}}

	// alpha=0.0 means pure keyword
	results := FuseLinear(dense, sparse, 0.0, 5)

	// Both should be present but sparse should have higher score
	var id1Score, id2Score float32
	for _, r := range results {
		if r.ID == 1 {
			id1Score = r.Score
		}
		if r.ID == 2 {
			id2Score = r.Score
		}
	}

	// ID 2 (sparse) should have full weight, ID 1 (dense) should have zero weight
	if id1Score >= id2Score {
		t.Errorf("expected id2 score > id1 score with alpha=0, got id1=%v, id2=%v", id1Score, id2Score)
	}
}

func TestFuseLinear_LimitTruncation(t *testing.T) {
	dense := make([]SearchResult, 10)
	sparse := make([]SearchResult, 10)
	for i := 0; i < 10; i++ {
		dense[i] = SearchResult{ID: VectorID(i), Score: float32(10 - i)}
		sparse[i] = SearchResult{ID: VectorID(i + 10), Score: float32(10 - i)}
	}

	results := FuseLinear(dense, sparse, 0.5, 3)
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestFuseCascade_EmptyExact(t *testing.T) {
	exactIDs := map[VectorID]struct{}{}
	keyword := []SearchResult{{ID: 1, Score: 5.0}}
	vector := []SearchResult{{ID: 2, Score: 0.9}}

	// Empty exact filter = use all
	results := FuseCascade(exactIDs, keyword, vector, 5)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestFuseCascade_NoMatches(t *testing.T) {
	exactIDs := map[VectorID]struct{}{99: {}}
	keyword := []SearchResult{{ID: 1, Score: 5.0}}
	vector := []SearchResult{{ID: 2, Score: 0.9}}

	// No matches with exact filter
	results := FuseCascade(exactIDs, keyword, vector, 5)
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestFuseCascade_Duplicates(t *testing.T) {
	exactIDs := map[VectorID]struct{}{1: {}}
	keyword := []SearchResult{{ID: 1, Score: 5.0}}
	vector := []SearchResult{{ID: 1, Score: 0.9}} // Same ID in both

	results := FuseCascade(exactIDs, keyword, vector, 5)

	// Should dedupe - only one result
	if len(results) != 1 {
		t.Errorf("expected 1 result (deduped), got %d", len(results))
	}

	// Should keep higher score
	if results[0].Score != 5.0 {
		t.Errorf("expected score 5.0, got %v", results[0].Score)
	}
}

// ========== dedupeAndSort Tests ==========

func TestDedupeAndSort_Empty(t *testing.T) {
	results := dedupeAndSort(nil, 10)
	if len(results) != 0 {
		t.Error("expected empty results")
	}
}

func TestDedupeAndSort_NoDuplicates(t *testing.T) {
	input := []SearchResult{
		{ID: 3, Score: 1.0},
		{ID: 1, Score: 3.0},
		{ID: 2, Score: 2.0},
	}

	results := dedupeAndSort(input, 10)

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	// Should be sorted by score descending
	if results[0].ID != 1 || results[1].ID != 2 || results[2].ID != 3 {
		t.Error("results not sorted by score descending")
	}
}

func TestDedupeAndSort_WithDuplicates(t *testing.T) {
	input := []SearchResult{
		{ID: 1, Score: 1.0},
		{ID: 1, Score: 3.0}, // Same ID, higher score
		{ID: 2, Score: 2.0},
	}

	results := dedupeAndSort(input, 10)

	if len(results) != 2 {
		t.Errorf("expected 2 results (deduped), got %d", len(results))
	}

	// ID 1 should have score 3.0 (highest)
	for _, r := range results {
		if r.ID == 1 && r.Score != 3.0 {
			t.Errorf("expected ID 1 score 3.0, got %v", r.Score)
		}
	}
}

func TestDedupeAndSort_LimitTruncation(t *testing.T) {
	input := make([]SearchResult, 20)
	for i := 0; i < 20; i++ {
		input[i] = SearchResult{ID: VectorID(i), Score: float32(i)}
	}

	results := dedupeAndSort(input, 5)
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
}

// ========== applyExactFilters Tests ==========

func TestApplyExactFilters_Empty(t *testing.T) {
	p := NewHybridSearchPipeline(DefaultHybridPipelineConfig())

	result := p.applyExactFilters(nil)
	if len(result) != 0 {
		t.Error("expected empty result for nil filters")
	}

	result = p.applyExactFilters([]query.Filter{})
	if len(result) != 0 {
		t.Error("expected empty result for empty filters")
	}
}

// ========== Benchmarks ==========

func BenchmarkFuseCascade(b *testing.B) {
	exactIDs := make(map[VectorID]struct{})
	for i := 0; i < 50; i++ {
		exactIDs[VectorID(i)] = struct{}{}
	}

	keyword := make([]SearchResult, 100)
	vector := make([]SearchResult, 100)
	for i := 0; i < 100; i++ {
		keyword[i] = SearchResult{ID: VectorID(i), Score: float32(100 - i)}
		vector[i] = SearchResult{ID: VectorID(i + 25), Score: float32(100 - i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FuseCascade(exactIDs, keyword, vector, 10)
	}
}

func BenchmarkDedupeAndSort(b *testing.B) {
	input := make([]SearchResult, 200)
	for i := 0; i < 200; i++ {
		input[i] = SearchResult{ID: VectorID(i % 100), Score: float32(i)} // 50% duplicates
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dedupeAndSort(input, 10)
	}
}
