package store

import (
	"fmt"
	"math"
	"sync/atomic"
)

// =============================================================================
// BM25Config - Configuration for BM25 scoring
// =============================================================================

// BM25Config holds BM25 algorithm parameters.
type BM25Config struct {
	K1 float64 // Term frequency saturation parameter (default: 1.2)
	B  float64 // Length normalization parameter (default: 0.75)
}

// DefaultBM25Config returns standard BM25 parameters.
func DefaultBM25Config() BM25Config {
	return BM25Config{
		K1: 1.2,
		B:  0.75,
	}
}

// Validate checks if the BM25 configuration is valid.
func (c BM25Config) Validate() error {
	if c.K1 < 0 {
		return fmt.Errorf("K1 must be non-negative, got %f", c.K1)
	}
	if c.K1 > 10.0 {
		return fmt.Errorf("K1 must be <= 10.0, got %f", c.K1)
	}
	if c.B < 0 {
		return fmt.Errorf("b must be non-negative, got %f", c.B)
	}
	if c.B > 1.0 {
		return fmt.Errorf("b must be <= 1.0, got %f", c.B)
	}
	return nil
}

// =============================================================================
// BM25Scorer - Lock-free BM25 scoring engine
// =============================================================================

// BM25Scorer computes BM25 relevance scores for documents.
// It uses atomic operations to maintain corpus statistics without locks.
type BM25Scorer struct {
	config      atomic.Pointer[BM25Config]
	totalDocs   atomic.Int64
	totalLength atomic.Int64 // Sum of all document lengths
}

// NewBM25Scorer creates a new BM25 scorer with the given configuration.
func NewBM25Scorer(config BM25Config) *BM25Scorer {
	s := &BM25Scorer{}
	s.config.Store(&config)
	return s
}

// Config returns the BM25 configuration.
func (s *BM25Scorer) Config() BM25Config {
	return *s.config.Load()
}

// TotalDocs returns the number of documents in the corpus.
func (s *BM25Scorer) TotalDocs() int {
	return int(s.totalDocs.Load())
}

// AvgDocLength returns the average document length in the corpus.
func (s *BM25Scorer) AvgDocLength() float64 {
	docs := s.totalDocs.Load()
	if docs <= 0 {
		return 0
	}
	return float64(s.totalLength.Load()) / float64(docs)
}

// AddDocument registers a document with the given length to the corpus.
func (s *BM25Scorer) AddDocument(docLength int) {
	s.totalDocs.Add(1)
	s.totalLength.Add(int64(docLength))
}

// RemoveDocument removes a document with the given length from the corpus.
func (s *BM25Scorer) RemoveDocument(docLength int) {
	s.totalDocs.Add(-1)
	s.totalLength.Add(-int64(docLength))
}

// IDF computes the Inverse Document Frequency for a term.
// Uses the BM25 IDF formula: log((N - df + 0.5) / (df + 0.5) + 1)
func (s *BM25Scorer) IDF(docFreq int) float64 {
	n := float64(s.totalDocs.Load())
	if n <= 0 {
		return 0
	}
	df := float64(docFreq)
	return math.Log((n-df+0.5)/(df+0.5) + 1.0)
}

// Score computes the BM25 score for a term in a document.
func (s *BM25Scorer) Score(tf, docLength, docFreq int) float64 {
	tfFloat := float64(tf)
	if tfFloat <= 0 {
		return 0
	}

	n := float64(s.totalDocs.Load())
	if n <= 0 {
		return 0
	}

	df := float64(docFreq)
	idf := math.Log((n-df+0.5)/(df+0.5) + 1.0)

	cfg := s.config.Load()
	k1 := cfg.K1
	b := cfg.B

	avgDL := s.AvgDocLength()
	if avgDL <= 0 {
		avgDL = 1.0
	}

	lengthNorm := 1.0 - b + b*(float64(docLength)/avgDL)
	if lengthNorm <= 0 {
		lengthNorm = 0.0001
	}

	numerator := tfFloat * (k1 + 1.0)
	denominator := tfFloat + k1*lengthNorm

	return idf * (numerator / denominator)
}

// ScoreMultiTerm computes the total BM25 score for multiple terms.
func (s *BM25Scorer) ScoreMultiTerm(docLength int, terms []struct{ TF, DocFreq int }) float64 {
	var totalScore float64
	for _, term := range terms {
		totalScore += s.Score(term.TF, docLength, term.DocFreq)
	}
	return totalScore
}
