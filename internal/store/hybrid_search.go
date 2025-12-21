package store

import (
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/coder/hnsw"

	"github.com/23skdu/longbow/internal/simd"
)

// SearchResult represents a search result with ID and score
type SearchResult struct {
	ID    VectorID
	Score float32
}

// =============================================================================
// Inverted Index for Sparse/Keyword Search
// =============================================================================

// InvertedIndex provides keyword-based search using term frequency scoring
type InvertedIndex struct {
	mu       sync.RWMutex
	index    map[string]map[VectorID]float32 // term -> docID -> TF score
	docTerms map[VectorID][]string           // docID -> terms (for deletion)
}

// NewInvertedIndex creates a new inverted index
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		index:    make(map[string]map[VectorID]float32),
		docTerms: make(map[VectorID][]string),
	}
}

// tokenize splits text into lowercase terms
func tokenize(text string) []string {
	text = strings.ToLower(text)
	// Split on whitespace and punctuation
	fields := strings.FieldsFunc(text, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	return fields
}

// Add indexes a document's text content
func (idx *InvertedIndex) Add(id VectorID, text string) {
	tokens := tokenize(text)
	if len(tokens) == 0 {
		return
	}

	// Calculate term frequencies
	tf := make(map[string]int)
	for _, token := range tokens {
		tf[token]++
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Store terms for this document (for deletion)
	idx.docTerms[id] = make([]string, 0, len(tf))

	// Add to inverted index with TF score
	for term, count := range tf {
		if idx.index[term] == nil {
			idx.index[term] = make(map[VectorID]float32)
		}
		// TF score: 1 + log(count)
		idx.index[term][id] = 1.0 + float32(math.Log(float64(count)))
		idx.docTerms[id] = append(idx.docTerms[id], term)
	}
}

// Delete removes a document from the index
func (idx *InvertedIndex) Delete(id VectorID) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	terms, exists := idx.docTerms[id]
	if !exists {
		return
	}

	for _, term := range terms {
		if postings, ok := idx.index[term]; ok {
			delete(postings, id)
			if len(postings) == 0 {
				delete(idx.index, term)
			}
		}
	}
	delete(idx.docTerms, id)
}

// Search finds documents matching the query terms
func (idx *InvertedIndex) Search(query string, limit int) []SearchResult {
	tokens := tokenize(query)
	if len(tokens) == 0 {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Aggregate scores across all query terms
	scores := make(map[VectorID]float32)
	for _, token := range tokens {
		if postings, ok := idx.index[token]; ok {
			for docID, tf := range postings {
				// Simple IDF approximation: log(total_terms / docs_with_term)
				idf := float32(math.Log(float64(len(idx.index)+1) / float64(len(postings)+1)))
				scores[docID] += tf * (idf + 1) // TF-IDF score
			}
		}
	}

	// Convert to sorted results
	results := make([]SearchResult, 0, len(scores))
	for id, score := range scores {
		results = append(results, SearchResult{ID: id, Score: score})
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results
}

// =============================================================================
// Reciprocal Rank Fusion (RRF)
// =============================================================================

// ReciprocalRankFusion combines multiple ranked lists using RRF algorithm
// k is the ranking constant (typically 60)
// Formula: RRF(d) = sum(1 / (k + rank(d)))
func ReciprocalRankFusion(k, limit int, resultSets ...[]SearchResult) []SearchResult {
	if k <= 0 {
		k = 60 // default
	}

	if len(resultSets) == 0 {
		return nil
	}

	// For small numbers of result sets, we can use a single map
	// For larger sets, we could use partitioned maps to reduce lock contention
	var mu sync.Mutex
	scores := make(map[VectorID]float64)

	var wg sync.WaitGroup
	for _, resultSet := range resultSets {
		if len(resultSet) == 0 {
			continue
		}
		wg.Add(1)
		go func(results []SearchResult) {
			defer wg.Done()
			localScores := make(map[VectorID]float64)
			for rank, result := range results {
				localScores[result.ID] += 1.0 / float64(k+rank+1)
			}

			mu.Lock()
			for id, score := range localScores {
				scores[id] += score
			}
			mu.Unlock()
		}(resultSet)
	}
	wg.Wait()

	// Convert to sorted results
	results := make([]SearchResult, 0, len(scores))
	for id, score := range scores {
		results = append(results, SearchResult{ID: id, Score: float32(score)})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results
}

// =============================================================================
// Hybrid Searcher (Dense + Sparse)
// =============================================================================

// HybridSearcher combines HNSW (dense) and inverted index (sparse) search
type HybridSearcher struct {
	mu       sync.RWMutex
	graph    *hnsw.Graph[VectorID]
	vectors  map[VectorID][]float32
	inverted *InvertedIndex
}

// NewHybridSearcher creates a new hybrid searcher
func NewHybridSearcher() *HybridSearcher {
	hs := &HybridSearcher{
		graph:    hnsw.NewGraph[VectorID](),
		vectors:  make(map[VectorID][]float32),
		inverted: NewInvertedIndex(),
	}
	return hs
}

// Add adds a vector with associated text to both indexes
func (hs *HybridSearcher) Add(id VectorID, vector []float32, text string) {
	hs.mu.Lock()
	// Store vector copy
	vec := make([]float32, len(vector))
	copy(vec, vector)
	hs.vectors[id] = vec
	hs.mu.Unlock()

	// Add to HNSW graph
	hs.graph.Add(hnsw.MakeNode(id, vec))

	// Add to inverted index
	hs.inverted.Add(id, text)
}

// Delete removes a vector from both indexes
func (hs *HybridSearcher) Delete(id VectorID) {
	hs.mu.Lock()
	delete(hs.vectors, id)
	hs.mu.Unlock()

	// Note: HNSW graph doesn't support deletion in coder/hnsw
	// In production, we'd need to rebuild or use a different library

	hs.inverted.Delete(id)
}

// SearchDense performs vector similarity search only
func (hs *HybridSearcher) SearchDense(query []float32, k int) []SearchResult {
	neighbors := hs.graph.Search(query, k)

	results := make([]SearchResult, 0, len(neighbors))
	hs.mu.RLock()
	for _, n := range neighbors {
		if vec, ok := hs.vectors[n.Key]; ok {
			dist := simd.EuclideanDistance(query, vec)
			results = append(results, SearchResult{
				ID:    n.Key,
				Score: 1.0 / (1.0 + dist),
			})
		}
	}
	hs.mu.RUnlock()
	return results
}

// SearchSparse performs keyword search only
func (hs *HybridSearcher) SearchSparse(query string, k int) []SearchResult {
	return hs.inverted.Search(query, k)
}

// SearchHybrid performs hybrid search with RRF fusion
// alpha is not used here (RRF handles fusion)
func (hs *HybridSearcher) SearchHybrid(vectorQuery []float32, textQuery string, k int, alpha float32, rrfK int) []SearchResult {
	// Get more candidates than needed for better fusion
	candidateK := k * 3
	if candidateK < 20 {
		candidateK = 20
	}

	denseResults := hs.SearchDense(vectorQuery, candidateK)
	sparseResults := hs.SearchSparse(textQuery, candidateK)

	return ReciprocalRankFusion(rrfK, k, denseResults, sparseResults)
}

// SearchHybridWeighted performs hybrid search with weighted combination
// alpha: 1.0 = dense only, 0.0 = sparse only
func (hs *HybridSearcher) SearchHybridWeighted(vectorQuery []float32, textQuery string, k int, alpha float32, rrfK int) []SearchResult {
	if alpha >= 1.0 {
		return hs.SearchDense(vectorQuery, k)
	}
	if alpha <= 0.0 {
		return hs.SearchSparse(textQuery, k)
	}

	// Get more candidates for fusion
	candidateK := k * 3
	if candidateK < 20 {
		candidateK = 20
	}

	denseResults := hs.SearchDense(vectorQuery, candidateK)
	sparseResults := hs.SearchSparse(textQuery, candidateK)

	// Weighted score combination
	scores := make(map[VectorID]float32)

	// Normalize and weight dense scores
	if len(denseResults) > 0 {
		maxDense := denseResults[0].Score
		for _, r := range denseResults {
			normalized := r.Score / maxDense
			scores[r.ID] += alpha * normalized
		}
	}

	// Normalize and weight sparse scores
	if len(sparseResults) > 0 {
		maxSparse := sparseResults[0].Score
		for _, r := range sparseResults {
			normalized := r.Score / maxSparse
			scores[r.ID] += (1 - alpha) * normalized
		}
	}

	// Convert to sorted results
	results := make([]SearchResult, 0, len(scores))
	for id, score := range scores {
		results = append(results, SearchResult{ID: id, Score: score})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if k > 0 && len(results) > k {
		results = results[:k]
	}

	return results
}
