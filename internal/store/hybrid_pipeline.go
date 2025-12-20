package store

import (
	"errors"
	"sort"
)

// FusionMode defines how to combine dense and sparse results
type FusionMode int

const (
	FusionModeRRF     FusionMode = iota // Reciprocal Rank Fusion
	FusionModeLinear                    // Linear weighted combination
	FusionModeCascade                   // Cascade: filters -> keyword -> vector
)

// HybridPipelineConfig configures the hybrid search pipeline
type HybridPipelineConfig struct {
	Alpha          float64    // 0.0=pure keyword, 1.0=pure vector, 0.5=balanced
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

// Validate checks configuration
func (c HybridPipelineConfig) Validate() error {
	if c.Alpha < 0 || c.Alpha > 1 {
		return errors.New("alpha must be between 0 and 1")
	}
	if c.RRFk <= 0 {
		return errors.New("RRFk must be positive")
	}
	if c.FusionMode < FusionModeRRF || c.FusionMode > FusionModeCascade {
		return errors.New("invalid fusion mode")
	}
	return nil
}

// HybridResult represents a search result with ID and score
type HybridResult struct {
	ID    uint32
	Score float64
}

// HybridSearchQuery extends search with vector, keyword, and filter options
type HybridSearchQuery struct {
	Vector        []float32 // Query vector for dense search
	KeywordQuery  string    // Text query for BM25 search
	K             int       // Number of results
	AlphaOverride *float64  // Override pipeline alpha if set
	ExactFilters  []Filter  // Exact match filters
}

// DefaultHybridSearchQuery returns a query with defaults
func DefaultHybridSearchQuery() HybridSearchQuery {
	return HybridSearchQuery{
		K: 10,
	}
}

// Validate checks query validity
func (q *HybridSearchQuery) Validate() error {
	if len(q.Vector) == 0 && q.KeywordQuery == "" {
		return errors.New("query requires vector and/or keyword")
	}
	if q.K <= 0 {
		return errors.New("k must be positive")
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
func (p *HybridSearchPipeline) Search(query *HybridSearchQuery) ([]HybridResult, error) {
	if err := query.Validate(); err != nil {
		return nil, err
	}

	alpha := p.config.Alpha
	if query.AlphaOverride != nil {
		alpha = *query.AlphaOverride
	}

	// Get exact filter IDs if column index enabled
	var exactIDs map[uint32]struct{}
	if p.config.UseColumnIndex && p.columnIndex != nil && len(query.ExactFilters) > 0 {
		exactIDs = p.applyExactFilters(query.ExactFilters)
	}

	// Vector search (dense) using HNSW
	var denseResults []HybridResult
	if len(query.Vector) > 0 && p.hnswIndex != nil && alpha > 0 {
		// Use SearchWithArena - create temporary arena
		arena := GetArena()
		defer PutArena(arena)
		ids := p.hnswIndex.SearchWithArena(query.Vector, query.K*2, arena)
		for rank, id := range ids {
			// Convert rank to score (higher rank = lower score)
			score := 1.0 / float64(rank+1)
			denseResults = append(denseResults, HybridResult{
				ID:    uint32(id),
				Score: score,
			})
		}
	}

	// Keyword search (sparse) using BM25
	var sparseResults []HybridResult
	if query.KeywordQuery != "" && p.bm25Index != nil && alpha < 1 {
		bm25Results := p.bm25Index.SearchBM25(query.KeywordQuery, query.K*2)
		for _, r := range bm25Results {
			sparseResults = append(sparseResults, HybridResult{
				ID:    uint32(r.ID),
				Score: float64(r.Score),
			})
		}
	}

	// Fuse results based on mode
	var fused []HybridResult
	switch p.config.FusionMode {
	case FusionModeRRF:
		fused = FuseRRF(denseResults, sparseResults, p.config.RRFk, query.K)
	case FusionModeLinear:
		fused = FuseLinear(denseResults, sparseResults, alpha, query.K)
	case FusionModeCascade:
		fused = FuseCascade(exactIDs, sparseResults, denseResults, query.K)
	default:
		fused = FuseRRF(denseResults, sparseResults, p.config.RRFk, query.K)
	}

	// Apply exact filters if present (post-filter)
	if len(exactIDs) > 0 && p.config.FusionMode != FusionModeCascade {
		filtered := make([]HybridResult, 0, len(fused))
		for _, r := range fused {
			if _, ok := exactIDs[r.ID]; ok {
				filtered = append(filtered, r)
			}
		}
		fused = filtered
	}

	// Limit to K
	if len(fused) > query.K {
		fused = fused[:query.K]
	}

	return fused, nil
}

func (p *HybridSearchPipeline) applyExactFilters(filters []Filter) map[uint32]struct{} {
	result := make(map[uint32]struct{})
	// Would integrate with ColumnInvertedIndex.Lookup for O(1) equality filters
	// For now, return empty to allow graceful pass-through
	return result
}

// FuseRRF combines results using Reciprocal Rank Fusion
func FuseRRF(dense, sparse []HybridResult, k, limit int) []HybridResult {
	scores := make(map[uint32]float64)

	// Add dense results RRF scores
	for rank, r := range dense {
		scores[r.ID] += 1.0 / float64(k+rank+1)
	}

	// Add sparse results RRF scores
	for rank, r := range sparse {
		scores[r.ID] += 1.0 / float64(k+rank+1)
	}

	// Convert to slice and sort
	results := make([]HybridResult, 0, len(scores))
	for id, score := range scores {
		results = append(results, HybridResult{ID: id, Score: score})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if len(results) > limit {
		return results[:limit]
	}
	return results
}

// FuseLinear combines results using linear weighted combination
func FuseLinear(dense, sparse []HybridResult, alpha float64, limit int) []HybridResult {
	scores := make(map[uint32]float64)

	// Normalize dense scores and apply alpha weight
	maxDense := 1.0
	for _, r := range dense {
		if r.Score > maxDense {
			maxDense = r.Score
		}
	}
	for _, r := range dense {
		scores[r.ID] += alpha * (r.Score / maxDense)
	}

	// Normalize sparse scores and apply (1-alpha) weight
	maxSparse := 1.0
	for _, r := range sparse {
		if r.Score > maxSparse {
			maxSparse = r.Score
		}
	}
	for _, r := range sparse {
		scores[r.ID] += (1 - alpha) * (r.Score / maxSparse)
	}

	// Convert to slice and sort
	results := make([]HybridResult, 0, len(scores))
	for id, score := range scores {
		results = append(results, HybridResult{ID: id, Score: score})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if len(results) > limit {
		return results[:limit]
	}
	return results
}

// FuseCascade filters: exact -> keyword -> vector
func FuseCascade(exactIDs map[uint32]struct{}, keywordResults, vectorResults []HybridResult, limit int) []HybridResult {
	// If exact filters provided, use them as first-level filter
	if len(exactIDs) > 0 {
		filtered := make([]HybridResult, 0)
		// First try keyword results in exact set
		for _, r := range keywordResults {
			if _, ok := exactIDs[r.ID]; ok {
				filtered = append(filtered, r)
			}
		}
		// Then try vector results in exact set
		for _, r := range vectorResults {
			if _, ok := exactIDs[r.ID]; ok {
				filtered = append(filtered, r)
			}
		}
		// Dedupe and sort
		return dedupeAndSort(filtered, limit)
	}

	// No exact filters - combine keyword and vector
	all := make([]HybridResult, 0, len(keywordResults)+len(vectorResults))
	all = append(all, keywordResults...)
	all = append(all, vectorResults...)
	return dedupeAndSort(all, limit)
}

func dedupeAndSort(results []HybridResult, limit int) []HybridResult {
	seen := make(map[uint32]float64)
	for _, r := range results {
		if existing, ok := seen[r.ID]; !ok || r.Score > existing {
			seen[r.ID] = r.Score
		}
	}

	deduped := make([]HybridResult, 0, len(seen))
	for id, score := range seen {
		deduped = append(deduped, HybridResult{ID: id, Score: score})
	}
	sort.Slice(deduped, func(i, j int) bool {
		return deduped[i].Score > deduped[j].Score
	})

	if len(deduped) > limit {
		return deduped[:limit]
	}
	return deduped
}
