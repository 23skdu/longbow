package store

// searchBM25Arena performs BM25 search using the arena-based index
func searchBM25Arena(idx *BM25ArenaIndex, queryText string, k int) []SearchResult {
	// Tokenize query using existing tokenize function from bm25_inverted_index.go
	tokens := tokenize(queryText)
	if len(tokens) == 0 {
		return nil
	}

	// Get all document IDs from the index
	docCount := idx.DocumentCount()
	if docCount == 0 {
		return nil
	}

	// Create candidate list (all documents)
	candidates := make([]uint32, docCount)
	for i := uint32(0); i < docCount; i++ {
		candidates[i] = i
	}

	// Score documents
	scores := idx.Score(tokens, candidates)

	// Convert to SearchResults and sort
	results := make([]SearchResult, 0, len(scores))
	for docID, score := range scores {
		if score > 0 {
			results = append(results, SearchResult{
				ID:    VectorID(docID), // Convert uint32 to VectorID
				Score: score,
			})
		}
	}

	// Sort by score descending
	sortSearchResults(results)

	// Return top-k
	if len(results) > k {
		results = results[:k]
	}

	return results
}

// sortSearchResults sorts results by score descending
func sortSearchResults(results []SearchResult) {
	n := len(results)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if results[j].Score < results[j+1].Score {
				results[j], results[j+1] = results[j+1], results[j]
			}
		}
	}
}
