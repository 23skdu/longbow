package store

import (
	"errors"
	"sort"

	"context"

	"github.com/23skdu/longbow/internal/query"
)

// HybridPipelineConfig configures the hybrid search pipeline
type HybridPipelineConfig struct {
	Alpha          float32    // 0.0=pure keyword, 1.0=pure vector, 0.5=balanced
	RRFk           int        // RRF parameter k (typically 60)
	FusionMode     FusionMode // How to combine results
	UseColumnIndex bool       // Use column index for exact filters
}

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
	Vector        []float32      // Query vector for dense search
	KeywordQuery  string         // Text query for BM25 search
	K             int            // Number of results
	AlphaOverride *float32       // Override pipeline alpha if set
	ExactFilters  []query.Filter // Exact match filters (for column index)
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
	reranker    Reranker
	dataset     *Dataset
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

// SetReranker sets the second-stage reranker
func (p *HybridSearchPipeline) SetReranker(r Reranker) {
	p.reranker = r
}

// SetDataset sets the dataset for content lookups
func (p *HybridSearchPipeline) SetDataset(ds *Dataset) {
	p.dataset = ds
}

// Search performs hybrid search combining all configured indexes
func (p *HybridSearchPipeline) Search(q *HybridSearchQuery) ([]SearchResult, error) {
	if q == nil {
		return nil, errors.New("query cannot be nil")
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if q.K <= 0 {
		return nil, errors.New("k must be positive")
	}

	alpha := p.config.Alpha
	if q.AlphaOverride != nil {
		alpha = *q.AlphaOverride
	}

	// 1. Get exact filter IDs if column index enabled
	exactIDs := p.applyExactFilters(q.ExactFilters)

	// 2. Vector search (dense) using HNSW
	var denseResults []SearchResult
	if len(q.Vector) > 0 && p.hnswIndex != nil && alpha > 0 {
		// Use SearchWithArena if available (restored from bak logic)
		arena := GetArena()
		defer PutArena(arena)

		// SearchWithArena returns []uint32 (internal IDs)
		ids := p.hnswIndex.SearchWithArena(q.Vector, q.K*2, arena)
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
	if q.KeywordQuery != "" && p.bm25Index != nil && alpha < 1 {
		sparseResults = p.bm25Index.SearchBM25(q.KeywordQuery, q.K*2)
	}

	// 4. Fuse results based on mode
	var fused []SearchResult
	switch p.config.FusionMode {
	case FusionModeRRF:
		fused = ReciprocalRankFusion(denseResults, sparseResults, p.config.RRFk, q.K)
	case FusionModeLinear:
		fused = FuseLinear(denseResults, sparseResults, alpha, q.K)
	case FusionModeCascade:
		fused = FuseCascade(exactIDs, sparseResults, denseResults, q.K)
	default:
		fused = ReciprocalRankFusion(denseResults, sparseResults, p.config.RRFk, q.K)
	}

	// 5. Re-ranking stage (Stage 2)
	// We re-rank the fused results using a more expensive model if available
	// Usually we re-rank more than K and then truncate
	if p.reranker != nil {
		reranked, err := p.reranker.Rerank(context.Background(), q.KeywordQuery, fused)
		if err == nil {
			fused = reranked
		}
	}

	// 6. Limit to K
	if len(fused) > q.K {
		fused = fused[:q.K]
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
func (p *HybridSearchPipeline) applyExactFilters(filters []query.Filter) map[VectorID]struct{} {
	if len(filters) == 0 || p.columnIndex == nil || p.dataset == nil {
		return nil
	}

	// For now, we take the intersection of all filters
	var result map[VectorID]struct{}

	for i, f := range filters {
		if f.Operator != "=" {
			continue // Only exact matches supported for now
		}

		positions := p.columnIndex.Lookup(p.dataset.Name, f.Field, f.Value)
		if len(positions) == 0 {
			return make(map[VectorID]struct{}) // Empty intersection
		}

		// Convert RowPositions to VectorIDs
		// This is a naive implementation: we need a way to map RowPosition back to VectorID.
		// Since HybridSearchPipeline is integrated with Dataset, we can ideally use
		// Dataset's own inverted indexes if available.

		currentIDs := make(map[VectorID]struct{})
		for _, pos := range positions {
			// Naive: scan or heuristic?
			// In HNSWIndex, VectorID matches index in locationStore.
			// Let's use a helper if possible.
			id, ok := p.findVectorID(pos)
			if ok {
				currentIDs[id] = struct{}{}
			}
		}

		if i == 0 {
			result = currentIDs
		} else {
			// Intersection
			for id := range result {
				if _, ok := currentIDs[id]; !ok {
					delete(result, id)
				}
			}
		}

		if len(result) == 0 {
			break
		}
	}

	return result
}

// findVectorID maps RowPosition back to internal VectorID
func (p *HybridSearchPipeline) findVectorID(pos RowPosition) (VectorID, bool) {
	if p.hnswIndex == nil {
		return 0, false
	}
	// O(1) Reverse Lookup relying on reverseMap in ChunkedLocationStore
	return p.hnswIndex.GetVectorID(Location{BatchIdx: pos.RecordIdx, RowIdx: pos.RowIdx})
}

// Reranker defines the interface for the second-stage re-ranking
type Reranker interface {
	Rerank(ctx context.Context, query string, results []SearchResult) ([]SearchResult, error)
}

// CrossEncoderReranker is a stub implementation of a cross-encoder model re-ranker
type CrossEncoderReranker struct {
	ModelName string
}

func (r *CrossEncoderReranker) Rerank(ctx context.Context, q string, results []SearchResult) ([]SearchResult, error) {
	// TODO: Implement actual cross-encoder scoring
	// For now, this is a stub that keeps results as-is
	return results, nil
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
