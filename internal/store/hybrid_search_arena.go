package store

import (
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
)

// searchBM25Arena performs BM25 search using the arena-based index
func searchBM25Arena(idx *BM25ArenaIndex, queryText string, k int, filter *roaring.Bitmap) []SearchResult {
	// Tokenize query using existing tokenize function from bm25_inverted_index.go
	tokens := tokenize(queryText)
	if len(tokens) == 0 {
		return nil
	}

	// Get total documents from the index
	docCount := idx.DocumentCount()
	if docCount == 0 {
		return nil
	}

	// Create candidate list (filtered documents)
	var candidates []uint32
	if filter != nil {
		// Use filter to populate candidates (but only up to docCount)
		candidates = make([]uint32, 0, filter.GetCardinality())
		it := filter.Iterator()
		for it.HasNext() {
			id := it.Next()
			if id < docCount {
				candidates = append(candidates, id)
			}
		}
	} else {
		// All documents
		candidates = make([]uint32, docCount)
		for i := uint32(0); i < docCount; i++ {
			candidates[i] = i
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Score documents
	scores := idx.Score(tokens, candidates)

	// Convert to SearchResults and filter out zero scores
	results := make([]SearchResult, 0, len(scores))
	for docID, score := range scores {
		if score > 0 {
			results = append(results, SearchResult{
				ID:    VectorID(docID),
				Score: score,
			})
		}
	}

	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Return top-k
	if len(results) > k {
		results = results[:k]
	}

	return results
}
