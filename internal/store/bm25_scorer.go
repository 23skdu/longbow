package store

import (
"fmt"
"math"
"sync"
)

// =============================================================================
// BM25Config - Configuration for BM25 scoring
// =============================================================================

// BM25Config holds BM25 algorithm parameters.
// K1 controls term frequency saturation (typically 1.2-2.0)
// B controls document length normalization (0=no normalization, 1=full normalization)
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
// BM25Scorer - Thread-safe BM25 scoring engine
// =============================================================================

// BM25Scorer computes BM25 relevance scores for documents.
// It maintains corpus statistics for IDF calculation and length normalization.
type BM25Scorer struct {
mu           sync.RWMutex
config       BM25Config
totalDocs    int
totalLength  int64   // Sum of all document lengths
avgDocLength float64 // Cached average document length
}

// NewBM25Scorer creates a new BM25 scorer with the given configuration.
func NewBM25Scorer(config BM25Config) *BM25Scorer {
return &BM25Scorer{
config: config,
}
}

// Config returns the BM25 configuration.
func (s *BM25Scorer) Config() BM25Config {
s.mu.RLock()
defer s.mu.RUnlock()
return s.config
}

// TotalDocs returns the number of documents in the corpus.
func (s *BM25Scorer) TotalDocs() int {
s.mu.RLock()
defer s.mu.RUnlock()
return s.totalDocs
}

// AvgDocLength returns the average document length in the corpus.
func (s *BM25Scorer) AvgDocLength() float64 {
s.mu.RLock()
defer s.mu.RUnlock()
return s.avgDocLength
}

// AddDocument registers a document with the given length to the corpus.
func (s *BM25Scorer) AddDocument(docLength int) {
s.mu.Lock()
defer s.mu.Unlock()

s.totalDocs++
s.totalLength += int64(docLength)
s.updateAvgDocLength()
}

// RemoveDocument removes a document with the given length from the corpus.
func (s *BM25Scorer) RemoveDocument(docLength int) {
s.mu.Lock()
defer s.mu.Unlock()

if s.totalDocs > 0 {
s.totalDocs--
s.totalLength -= int64(docLength)
if s.totalLength < 0 {
s.totalLength = 0
}
s.updateAvgDocLength()
}
}

// updateAvgDocLength recalculates the average document length.
// Must be called with lock held.
func (s *BM25Scorer) updateAvgDocLength() {
if s.totalDocs > 0 {
s.avgDocLength = float64(s.totalLength) / float64(s.totalDocs)
} else {
s.avgDocLength = 0
}
}

// IDF computes the Inverse Document Frequency for a term.
// Uses the BM25 IDF formula: log((N - df + 0.5) / (df + 0.5) + 1)
// where N is total documents and df is document frequency.
func (s *BM25Scorer) IDF(docFreq int) float64 {
s.mu.RLock()
defer s.mu.RUnlock()

if s.totalDocs == 0 {
return 0
}

n := float64(s.totalDocs)
df := float64(docFreq)

// BM25 IDF formula with smoothing to avoid negative values
return math.Log((n-df+0.5)/(df+0.5) + 1.0)
}

// Score computes the BM25 score for a term in a document.
// Parameters:
//   - tf: term frequency in the document
//   - docLength: length of the document (number of terms)
//   - docFreq: number of documents containing the term
//
// Returns the BM25 score component for this term.
func (s *BM25Scorer) Score(tf, docLength, docFreq int) float64 {
s.mu.RLock()
defer s.mu.RUnlock()

// Handle edge cases
if s.totalDocs == 0 || tf == 0 {
return 0
}

// Get IDF (without lock since we already hold it)
n := float64(s.totalDocs)
df := float64(docFreq)
idf := math.Log((n-df+0.5)/(df+0.5) + 1.0)

// BM25 term frequency saturation
// score = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * docLen/avgDocLen))
tfFloat := float64(tf)
docLenFloat := float64(docLength)
k1 := s.config.K1
b := s.config.B

// Avoid division by zero for avgDocLength
avgDL := s.avgDocLength
if avgDL == 0 {
avgDL = 1.0 // Prevent division by zero
}

// Length normalization factor
lengthNorm := 1.0 - b + b*(docLenFloat/avgDL)
if lengthNorm <= 0 {
lengthNorm = 0.0001 // Prevent division by zero
}

// BM25 formula
numerator := tfFloat * (k1 + 1.0)
denominator := tfFloat + k1*lengthNorm

return idf * (numerator / denominator)
}

// ScoreMultiTerm computes the total BM25 score for multiple terms.
// Each term is represented as (tf, docFreq) pair.
func (s *BM25Scorer) ScoreMultiTerm(docLength int, terms []struct{ TF, DocFreq int }) float64 {
var totalScore float64
for _, term := range terms {
totalScore += s.Score(term.TF, docLength, term.DocFreq)
}
return totalScore
}
