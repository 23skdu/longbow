package store

import (
	"hash/fnv"
	"sort"
	"sync"
)

const invertedIndexShards = 32

// termShard holds the inverted index for a subset of terms
type termShard struct {
	bloom *BloomFilter
	mu    sync.RWMutex
	index map[string]map[VectorID]float32 // term -> docID -> score
}

// docShard holds the document terms for a subset of documents
type docShard struct {
	mu    sync.RWMutex
	terms map[VectorID][]string // docID -> terms
}

// ShardedInvertedIndex is a concurrent-safe inverted index using sharding
type ShardedInvertedIndex struct {
	termShards [invertedIndexShards]termShard
	docShards  [invertedIndexShards]docShard
}

// NewShardedInvertedIndex creates a new sharded inverted index
func NewShardedInvertedIndex() *ShardedInvertedIndex {
	idx := &ShardedInvertedIndex{}
	for i := 0; i < invertedIndexShards; i++ {
		idx.termShards[i].bloom = NewBloomFilter(10000, 0.01)
		idx.termShards[i].index = make(map[string]map[VectorID]float32)
		idx.docShards[i].terms = make(map[VectorID][]string)
	}
	return idx
}

// termShardIndex returns the shard index for a term
func (idx *ShardedInvertedIndex) termShardIndex(term string) int {
	h := fnv.New32a()
	h.Write([]byte(term))
	return int(h.Sum32() % invertedIndexShards)
}

// docShardIndex returns the shard index for a document
func (idx *ShardedInvertedIndex) docShardIndex(id VectorID) int {
	return int(uint64(id) % invertedIndexShards) //nolint:gosec // G115 - invertedIndexShards is small const
}

// Add indexes a document with the given text
func (idx *ShardedInvertedIndex) Add(id VectorID, text string) {
	terms := tokenize(text)
	if len(terms) == 0 {
		return
	}

	// Calculate TF scores
	termFreq := make(map[string]int)
	for _, term := range terms {
		termFreq[term]++
	}

	termScores := make(map[string]float32)
	for term, freq := range termFreq {
		termScores[term] = float32(freq) / float32(len(terms))
	}

	// Store document terms
	docShardIdx := idx.docShardIndex(id)
	idx.docShards[docShardIdx].mu.Lock()
	idx.docShards[docShardIdx].terms[id] = terms
	idx.docShards[docShardIdx].mu.Unlock()

	// Update term index - group by shard to minimize lock acquisitions
	shardTerms := make(map[int]map[string]float32)
	for term, score := range termScores {
		shardIdx := idx.termShardIndex(term)
		if shardTerms[shardIdx] == nil {
			shardTerms[shardIdx] = make(map[string]float32)
		}
		shardTerms[shardIdx][term] = score
	}

	// Apply updates per shard
	for shardIdx, terms := range shardTerms {
		shard := &idx.termShards[shardIdx]
		shard.mu.Lock()
		for term, score := range terms {
			if shard.index[term] == nil {
				shard.index[term] = make(map[VectorID]float32)
			}
			shard.bloom.Add(term)
			shard.index[term][id] = score
		}
		shard.mu.Unlock()
	}
}

// Delete removes a document from the index
func (idx *ShardedInvertedIndex) Delete(id VectorID) {
	// Get document terms first
	docShardIdx := idx.docShardIndex(id)
	idx.docShards[docShardIdx].mu.Lock()
	terms, exists := idx.docShards[docShardIdx].terms[id]
	if exists {
		delete(idx.docShards[docShardIdx].terms, id)
	}
	idx.docShards[docShardIdx].mu.Unlock()

	if !exists {
		return
	}

	// Group terms by shard
	shardTerms := make(map[int][]string)
	for _, term := range terms {
		shardIdx := idx.termShardIndex(term)
		shardTerms[shardIdx] = append(shardTerms[shardIdx], term)
	}

	// Remove from each term shard
	for shardIdx, termList := range shardTerms {
		shard := &idx.termShards[shardIdx]
		shard.mu.Lock()
		for _, term := range termList {
			if docs, ok := shard.index[term]; ok {
				delete(docs, id)
				if len(docs) == 0 {
					delete(shard.index, term)
				}
			}
		}
		shard.mu.Unlock()
	}
}

// Search returns documents matching any of the query terms, scored by BM25-like TF
func (idx *ShardedInvertedIndex) Search(query string, limit int) []SearchResult {
	queryTerms := tokenize(query)
	if len(queryTerms) == 0 {
		return nil
	}

	// Group query terms by shard for efficient lookup
	shardTerms := make(map[int][]string)
	for _, term := range queryTerms {
		shardIdx := idx.termShardIndex(term)
		shardTerms[shardIdx] = append(shardTerms[shardIdx], term)
	}

	// Aggregate scores across all matching terms
	scores := make(map[VectorID]float32)
	for shardIdx, terms := range shardTerms {
		shard := &idx.termShards[shardIdx]
		shard.mu.RLock()
		for _, term := range terms {
			// Bloom filter pre-check - skip if term definitely not present
			if !shard.bloom.Contains(term) {
				continue
			}
			if docs, ok := shard.index[term]; ok {
				for id, score := range docs {
					scores[id] += score
				}
			}
		}
		shard.mu.RUnlock()
	}

	if len(scores) == 0 {
		return nil
	}

	// Convert to results and sort
	results := make([]SearchResult, 0, len(scores))
	for id, score := range scores {
		results = append(results, SearchResult{ID: id, Score: score})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results
}
