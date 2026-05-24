package store
import "github.com/23skdu/longbow/internal/store/index"

import (
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
)

// searchBM25Arena performs BM25 search using the arena-based index
func searchBM25Arena(idx *BM25ArenaIndex, queryText string, k int, filter *roaring.Bitmap) []SearchResult {
	// Tokenize query using existing tokenize function from bm25_inverted_index.go
	tokens := index.Tokenize(queryText)
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
		// Optimize candidate gathering: only score documents that match at least one query token
		docMap := make(map[uint32]struct{})
		for _, token := range tokens {
			if tokenID, exists := idx.GetTokenID(token); exists {
				if postings, ok := idx.GetPostingList(tokenID); ok {
					for _, docID := range postings {
						if docID < docCount {
							docMap[docID] = struct{}{}
						}
					}
				}
			}
		}
		if len(docMap) > 0 {
			candidates = make([]uint32, 0, len(docMap))
			for docID := range docMap {
				candidates = append(candidates, docID)
			}
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
