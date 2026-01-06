package store


import (
	"testing"
)

func TestHybridQueryDefaults(t *testing.T) {
	q := DefaultHybridQuery()

	if q.Enabled {
		t.Error("expected Enabled=false by default")
	}
	if q.Alpha != 0.5 {
		t.Errorf("expected Alpha=0.5, got %f", q.Alpha)
	}
	if q.TextQuery != "" {
		t.Errorf("expected empty TextQuery, got %s", q.TextQuery)
	}
	if q.K != 10 {
		t.Errorf("expected K=10, got %d", q.K)
	}
}

func TestHybridQueryValidation(t *testing.T) {
	tests := []struct {
		name    string
		query   HybridQuery
		wantErr bool
	}{
		{
			name:    "valid defaults",
			query:   DefaultHybridQuery(),
			wantErr: false,
		},
		{
			name: "valid enabled query",
			query: HybridQuery{
				Enabled:   true,
				TextQuery: "search term",
				Alpha:     0.7,
				K:         20,
			},
			wantErr: false,
		},
		{
			name: "alpha below zero",
			query: HybridQuery{
				Enabled:   true,
				TextQuery: "test",
				Alpha:     -0.1,
				K:         10,
			},
			wantErr: true,
		},
		{
			name: "alpha above one",
			query: HybridQuery{
				Enabled:   true,
				TextQuery: "test",
				Alpha:     1.5,
				K:         10,
			},
			wantErr: true,
		},
		{
			name: "K zero",
			query: HybridQuery{
				Enabled:   true,
				TextQuery: "test",
				Alpha:     0.5,
				K:         0,
			},
			wantErr: true,
		},
		{
			name: "enabled but empty text",
			query: HybridQuery{
				Enabled:   true,
				TextQuery: "",
				Alpha:     0.5,
				K:         10,
			},
			wantErr: true, // text required when enabled
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.query.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSearchHybridPureVector(t *testing.T) {
	// Alpha=1.0 means pure vector search
	query := HybridQuery{
		Enabled:   true,
		TextQuery: "ignored",
		Alpha:     1.0, // Pure vector
		K:         5,
	}

	if !query.IsPureVector() {
		t.Error("expected Alpha=1.0 to be pure vector search")
	}
}

func TestSearchHybridPureKeyword(t *testing.T) {
	// Alpha=0.0 means pure keyword/BM25 search
	query := HybridQuery{
		Enabled:   true,
		TextQuery: "search term",
		Alpha:     0.0, // Pure keyword
		K:         5,
	}

	if !query.IsPureKeyword() {
		t.Error("expected Alpha=0.0 to be pure keyword search")
	}
}

func TestHybridResultCombination(t *testing.T) {
	// Test combining vector and BM25 results with alpha weighting
	vectorResults := []SearchResult{
		{ID: 1, Score: 0.1},
		{ID: 2, Score: 0.2},
		{ID: 3, Score: 0.3},
	}

	bm25Results := []SearchResult{
		{ID: 2, Score: 0.9}, // BM25 scores (higher is better)
		{ID: 4, Score: 0.8},
		{ID: 1, Score: 0.5},
	}

	alpha := 0.5 // Equal weighting

	combined := CombineHybridResults(vectorResults, bm25Results, float32(alpha), 5)

	if len(combined) == 0 {
		t.Error("expected combined results")
	}
}

func TestHybridResultRRF(t *testing.T) {
	// Test Reciprocal Rank Fusion specifically
	vectorResults := []SearchResult{
		{ID: 1, Score: 0.0}, // Rank 1
		{ID: 2, Score: 0.1}, // Rank 2
	}

	bm25Results := []SearchResult{
		{ID: 2, Score: 0.9}, // Rank 1
		{ID: 1, Score: 0.8}, // Rank 2
	}

	combined := CombineHybridResultsRRF(vectorResults, bm25Results, 60, 5)

	if len(combined) < 2 {
		t.Errorf("expected at least 2 results, got %d", len(combined))
	}
}
