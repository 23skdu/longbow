package store


import (
	"math"
	"testing"
)

// =============================================================================
// BM25Config Tests
// =============================================================================

func TestDefaultBM25Config(t *testing.T) {
	cfg := DefaultBM25Config()

	// Standard BM25 defaults
	if cfg.K1 != 1.2 {
		t.Errorf("expected K1=1.2, got %f", cfg.K1)
	}
	if cfg.B != 0.75 {
		t.Errorf("expected B=0.75, got %f", cfg.B)
	}
}

func TestBM25ConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		k1      float64
		b       float64
		wantErr bool
	}{
		{"valid defaults", 1.2, 0.75, false},
		{"valid custom", 2.0, 0.5, false},
		{"k1 at zero", 0.0, 0.75, false},
		{"b at zero", 1.2, 0.0, false},
		{"b at one", 1.2, 1.0, false},
		{"negative k1", -0.1, 0.75, true},
		{"negative b", 1.2, -0.1, true},
		{"b greater than one", 1.2, 1.1, true},
		{"k1 too large", 100.0, 0.75, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := BM25Config{K1: tt.k1, B: tt.b}
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// BM25Scorer Tests
// =============================================================================

func TestNewBM25Scorer(t *testing.T) {
	cfg := DefaultBM25Config()
	scorer := NewBM25Scorer(cfg)

	if scorer == nil {
		t.Fatal("NewBM25Scorer returned nil")
	}
	if scorer.Config().K1 != cfg.K1 {
		t.Errorf("config K1 mismatch")
	}
	if scorer.TotalDocs() != 0 {
		t.Errorf("expected 0 total docs initially")
	}
	if scorer.AvgDocLength() != 0 {
		t.Errorf("expected 0 avg doc length initially")
	}
}

func TestBM25ScorerAddDocument(t *testing.T) {
	scorer := NewBM25Scorer(DefaultBM25Config())

	// Add first document with length 100
	scorer.AddDocument(100)
	if scorer.TotalDocs() != 1 {
		t.Errorf("expected 1 doc, got %d", scorer.TotalDocs())
	}
	if scorer.AvgDocLength() != 100.0 {
		t.Errorf("expected avg=100, got %f", scorer.AvgDocLength())
	}

	// Add second document with length 200
	scorer.AddDocument(200)
	if scorer.TotalDocs() != 2 {
		t.Errorf("expected 2 docs, got %d", scorer.TotalDocs())
	}
	if scorer.AvgDocLength() != 150.0 {
		t.Errorf("expected avg=150, got %f", scorer.AvgDocLength())
	}
}

func TestBM25ScorerRemoveDocument(t *testing.T) {
	scorer := NewBM25Scorer(DefaultBM25Config())

	scorer.AddDocument(100)
	scorer.AddDocument(200)
	scorer.RemoveDocument(100)

	if scorer.TotalDocs() != 1 {
		t.Errorf("expected 1 doc after removal, got %d", scorer.TotalDocs())
	}
	if scorer.AvgDocLength() != 200.0 {
		t.Errorf("expected avg=200 after removal, got %f", scorer.AvgDocLength())
	}
}

func TestBM25ScorerIDF(t *testing.T) {
	scorer := NewBM25Scorer(DefaultBM25Config())

	// Add 10 documents
	for i := 0; i < 10; i++ {
		scorer.AddDocument(100)
	}

	// IDF formula: log((N - df + 0.5) / (df + 0.5) + 1)
	// With N=10, df=1: log((10 - 1 + 0.5) / (1 + 0.5) + 1) = log(9.5/1.5 + 1) = log(7.333)
	idf := scorer.IDF(1) // term appears in 1 document
	expectedIDF := math.Log((10.0-1.0+0.5)/(1.0+0.5) + 1.0)

	if math.Abs(idf-expectedIDF) > 0.0001 {
		t.Errorf("IDF(1) = %f, expected %f", idf, expectedIDF)
	}

	// Term in all documents should have low IDF
	idfAll := scorer.IDF(10)
	if idfAll >= idf {
		t.Errorf("IDF for common term (%f) should be less than rare term (%f)", idfAll, idf)
	}
}

func TestBM25ScorerScore(t *testing.T) {
	scorer := NewBM25Scorer(DefaultBM25Config())

	// Setup: 10 docs, avg length 100
	for i := 0; i < 10; i++ {
		scorer.AddDocument(100)
	}

	// BM25 score for a term:
	// score = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * docLen/avgDocLen))

	// Test case: tf=2, docLen=100, df=2
	score := scorer.Score(2, 100, 2) // tf=2, docLen=100, df=2
	if score <= 0 {
		t.Errorf("expected positive score, got %f", score)
	}

	// Higher TF should give higher score (same doc length)
	scoreHighTF := scorer.Score(5, 100, 2)
	if scoreHighTF <= score {
		t.Errorf("higher TF should give higher score: tf=5 got %f, tf=2 got %f", scoreHighTF, score)
	}

	// Longer document should give lower score (same TF) due to length normalization
	scoreLongDoc := scorer.Score(2, 200, 2)
	if scoreLongDoc >= score {
		t.Errorf("longer doc should have lower score: docLen=200 got %f, docLen=100 got %f", scoreLongDoc, score)
	}

	// Rarer term (lower df) should give higher score
	scoreRareTerm := scorer.Score(2, 100, 1)
	if scoreRareTerm <= score {
		t.Errorf("rarer term should have higher score: df=1 got %f, df=2 got %f", scoreRareTerm, score)
	}
}

func TestBM25ScorerScoreEdgeCases(t *testing.T) {
	scorer := NewBM25Scorer(DefaultBM25Config())

	// Empty corpus - should return 0
	score := scorer.Score(1, 100, 1)
	if score != 0 {
		t.Errorf("empty corpus should return 0 score, got %f", score)
	}

	// Add documents
	for i := 0; i < 10; i++ {
		scorer.AddDocument(100)
	}

	// Zero TF should return 0
	scoreZeroTF := scorer.Score(0, 100, 1)
	if scoreZeroTF != 0 {
		t.Errorf("zero TF should return 0 score, got %f", scoreZeroTF)
	}

	// Zero doc length should not panic
	scoreZeroLen := scorer.Score(1, 0, 1)
	if math.IsNaN(scoreZeroLen) || math.IsInf(scoreZeroLen, 0) {
		t.Errorf("zero doc length should not produce NaN/Inf, got %f", scoreZeroLen)
	}
}

func TestBM25ScorerConcurrency(t *testing.T) {
	scorer := NewBM25Scorer(DefaultBM25Config())

	// Concurrent document additions
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(docLen int) {
			scorer.AddDocument(docLen)
			done <- true
		}(100 + i)
	}

	for i := 0; i < 100; i++ {
		<-done
	}

	if scorer.TotalDocs() != 100 {
		t.Errorf("expected 100 docs after concurrent adds, got %d", scorer.TotalDocs())
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkBM25Score(b *testing.B) {
	scorer := NewBM25Scorer(DefaultBM25Config())
	for i := 0; i < 10000; i++ {
		scorer.AddDocument(100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scorer.Score(5, 120, 50)
	}
}

func BenchmarkBM25IDF(b *testing.B) {
	scorer := NewBM25Scorer(DefaultBM25Config())
	for i := 0; i < 10000; i++ {
		scorer.AddDocument(100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scorer.IDF(50)
	}
}
