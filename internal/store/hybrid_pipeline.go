package store

import (
	"errors"
	"sort"
)

func (c *HybridPipelineConfig) Validate() error {
	if c.Alpha < 0 || c.Alpha > 1 {
		return errors.New("alpha must be between 0 and 1")
	}
	if c.RRFk <= 0 {
		return errors.New("rrf k must be positive")
	}
	if int(c.FusionMode) < 0 || int(c.FusionMode) > 2 {
		return errors.New("invalid fusion mode")
	}
	return nil
}

// FusionMode defines how to combine dense and sparse results
type FusionMode int

const (
	FusionModeRRF     FusionMode = iota // Reciprocal Rank Fusion
	FusionModeLinear                    // Linear weighted combination
	FusionModeCascade                   // Cascade: filters -> keyword -> vector
)

// HybridPipelineConfig configures the hybrid search pipeline
type HybridPipelineConfig struct {
	Alpha          float32    // 0.0=pure keyword, 1.0=pure vector, 0.5=balanced
	RRFk           int        // RRF parameter k (typically 60)
	FusionMode     FusionMode // How to combine results
	UseColumnIndex bool       // Use column index for exact filters
}

// DefaultHybridPipelineConfig returns sensible defaults
func DefaultHybridPipelineConfig() HybridPipelineConfig {
	return HybridPipelineConfig{
		Alpha:          0.5,
		RRFk:           60,
		FusionMode:     FusionModeRRF,
		UseColumnIndex: true,
	}
}

// HybridSearchQuery extends search with vector, keyword, and filter options
type HybridSearchQuery struct {
	Vector        []float32 // Query vector for dense search
	KeywordQuery  string    // Text query for BM25 search
	K             int       // Number of results
	AlphaOverride *float32  // Override pipeline alpha if set
	ExactFilters  []Filter  // Exact match filters (for column index)
}

func DefaultHybridSearchQuery() HybridSearchQuery {
	return HybridSearchQuery{K: 10}
}

func (q *HybridSearchQuery) Validate() error {
	if q.K <= 0 {
		return errors.New("k must be positive")
	}
	if len(q.Vector) == 0 && q.KeywordQuery == "" {
		return errors.New("query must have vector or keyword")
	}
	return nil
}

// HybridSearchPipeline combines column index, BM25, and HNSW search
type HybridSearchPipeline struct {
	config      HybridPipelineConfig
	columnIndex *ColumnInvertedIndex
	bm25Index   *BM25InvertedIndex
	hnswIndex   *HNSWIndex
}

// NewHybridSearchPipeline creates a new hybrid search pipeline
func NewHybridSearchPipeline(cfg HybridPipelineConfig) *HybridSearchPipeline {
	return &HybridSearchPipeline{
		config: cfg,
	}
}

// SetColumnIndex sets the column inverted index for exact filters
func (p *HybridSearchPipeline) SetColumnIndex(idx *ColumnInvertedIndex) {
	p.columnIndex = idx
}

// SetBM25Index sets the BM25 inverted index for keyword search
func (p *HybridSearchPipeline) SetBM25Index(idx *BM25InvertedIndex) {
	p.bm25Index = idx
}

// SetHNSWIndex sets the HNSW index for vector search
func (p *HybridSearchPipeline) SetHNSWIndex(idx *HNSWIndex) {
	p.hnswIndex = idx
}

// Search performs hybrid search combining all configured indexes
func (p *HybridSearchPipeline) Search(query *HybridSearchQuery) ([]SearchResult, error) {
	if query == nil {
		return nil, errors.New("query cannot be nil")
	}
	if err := query.Validate(); err != nil {
		return nil, err
	}
	if query.K <= 0 {
		return nil, errors.New("k must be positive")
	}

	alpha := p.config.Alpha
	if query.AlphaOverride != nil {
		alpha = *query.AlphaOverride
	}

	// 1. Get exact filter IDs if column index enabled
	exactIDs := p.applyExactFilters(query.ExactFilters)

	// 2. Vector search (dense) using HNSW
	var denseResults []SearchResult
	if len(query.Vector) > 0 && p.hnswIndex != nil && alpha > 0 {
		// Use SearchWithArena if available (restored from bak logic)
		arena := GetArena()
		defer PutArena(arena)

		// SearchWithArena returns []uint32 (internal IDs)
		ids := p.hnswIndex.SearchWithArena(query.Vector, query.K*2, arena)
		for rank, id := range ids {
			// Convert rank to score (higher rank = lower score)
			score := 1.0 / float32(rank+1)
			denseResults = append(denseResults, SearchResult{
				ID:    VectorID(id),
				Score: score,
			})
		}
	}

	// 3. Keyword search (sparse) using BM25
	var sparseResults []SearchResult
	if query.KeywordQuery != "" && p.bm25Index != nil && alpha < 1 {
		sparseResults = p.bm25Index.SearchBM25(query.KeywordQuery, query.K*2)
	}

	// 4. Fuse results based on mode
	var fused []SearchResult
	switch p.config.FusionMode {
	case FusionModeRRF:
		fused = ReciprocalRankFusion(denseResults, sparseResults, p.config.RRFk, query.K)
	case FusionModeLinear:
		fused = FuseLinear(denseResults, sparseResults, alpha, query.K)
	case FusionModeCascade:
		fused = FuseCascade(exactIDs, sparseResults, denseResults, query.K)
	default:
		fused = ReciprocalRankFusion(denseResults, sparseResults, p.config.RRFk, query.K)
	}

	// 5. Limit to K
	if len(fused) > query.K {
		fused = fused[:query.K]
	}

	return fused, nil
}

// FuseLinear combines results using linear weighted combination
func FuseLinear(dense, sparse []SearchResult, alpha float32, limit int) []SearchResult {
	scores := make(map[VectorID]float32)

	// Normalize dense scores and apply alpha weight
	var maxDense float32 = 0.0001
	for _, r := range dense {
		if r.Score > maxDense {
			maxDense = r.Score
		}
	}
	for _, r := range dense {
		scores[r.ID] += alpha * (r.Score / maxDense)
	}

	// Normalize sparse scores and apply (1-alpha) weight
	var maxSparse float32 = 0.0001
	for _, r := range sparse {
		if r.Score > maxSparse {
			maxSparse = r.Score
		}
	}
	for _, r := range sparse {
		scores[r.ID] += (1.0 - alpha) * (r.Score / maxSparse)
	}

	// Convert to slice and sort
	results := make([]SearchResult, 0, len(scores))
	for id, score := range scores {
		results = append(results, SearchResult{ID: id, Score: score})
	}
	return dedupeAndSort(results, limit)
}

// FuseRRF is an alias for ReciprocalRankFusion (legacy alignment)
func FuseRRF(dense, sparse []SearchResult, k, limit int) []SearchResult {
	return ReciprocalRankFusion(dense, sparse, k, limit)
}

// FuseCascade implements cascade-style filtering: exact -> keyword -> vector
func FuseCascade(exact map[VectorID]struct{}, keyword, vector []SearchResult, limit int) []SearchResult {
	var filtered []SearchResult

	// If exact filters provided, only keep results present in exact set
	if len(exact) > 0 {
		for _, r := range keyword {
			if _, ok := exact[r.ID]; ok {
				filtered = append(filtered, r)
			}
		}
		for _, r := range vector {
			if _, ok := exact[r.ID]; ok {
				// Avoid duplicates if already in keyword results
				found := false
				for _, fr := range filtered {
					if fr.ID == r.ID {
						found = true
						break
					}
				}
				if !found {
					filtered = append(filtered, r)
				}
			}
		}
	} else {
		// No exact filters, combine all and de-duplicate
		seen := make(map[VectorID]struct{})
		for _, r := range keyword {
			filtered = append(filtered, r)
			seen[r.ID] = struct{}{}
		}
		for _, r := range vector {
			if _, ok := seen[r.ID]; !ok {
				filtered = append(filtered, r)
			}
		}
	}

	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	// Re-sort to be safe, though cascade logic might not fully guarantee order if mixed
	return dedupeAndSort(filtered, limit)
}

// applyExactFilters applies exact match filters using column index
func (p *HybridSearchPipeline) applyExactFilters(filters []Filter) map[VectorID]struct{} {
	if !p.config.UseColumnIndex || p.columnIndex == nil || len(filters) == 0 {
		return nil
	}

	// This would be implemented when integrating with VectorStore
	return nil
}

// dedupeAndSort removes duplicates (keeping highest score) and sorts by score descending
func dedupeAndSort(results []SearchResult, limit int) []SearchResult {
	if len(results) == 0 {
		return nil
	}

	seen := make(map[VectorID]int)
	var unique []SearchResult

	for _, r := range results {
		if idx, ok := seen[r.ID]; ok {
			if r.Score > unique[idx].Score {
				unique[idx].Score = r.Score
			}
		} else {
			seen[r.ID] = len(unique)
			unique = append(unique, r)
		}
	}

	sort.Slice(unique, func(i, j int) bool {
		return unique[i].Score > unique[j].Score
	})

	if len(unique) > limit {
		unique = unique[:limit]
	}
	return unique
}
