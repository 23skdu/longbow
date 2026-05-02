package store

import (
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/simd"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
)

// BM25InvertedIndex is a sharded inverted index with proper BM25 scoring
type BM25InvertedIndex struct {
	config     BM25Config
	scorer     *BM25Scorer
	termShards [invertedIndexShards]bm25TermShard
	docShards  [invertedIndexShards]bm25DocShard
	docCount   atomic.Int64
	totalLen   atomic.Int64 // total document length for avgdl
}

// BM25PostingList stores parallel slices for SIMD-accelerated scoring
type BM25PostingList struct {
	DocIDs []VectorID
	TFs    []int
}

// bm25TermShard holds the inverted index for a subset of terms
type bm25TermShard struct {
	bloom *BloomFilter
	mu    sync.RWMutex
	// term -> posting list
	index map[string]*BM25PostingList
}

// bm25DocShard holds document metadata for a subset of documents
type bm25DocShard struct {
	mu     sync.RWMutex
	terms  map[VectorID][]string // docID -> terms
	length map[VectorID]int      // docID -> document length (term count)
}

// NewBM25InvertedIndex creates a new BM25 inverted index
func NewBM25InvertedIndex(config BM25Config) *BM25InvertedIndex {
	idx := &BM25InvertedIndex{
		config: config,
		scorer: NewBM25Scorer(config),
	}
	for i := 0; i < invertedIndexShards; i++ {
		idx.termShards[i].bloom = NewBloomFilter(10000, 0.01)
		idx.termShards[i].index = make(map[string]*BM25PostingList)
		idx.docShards[i].terms = make(map[VectorID][]string)
		idx.docShards[i].length = make(map[VectorID]int)
	}
	return idx
}

// termShardIndex returns the shard index for a term
func (idx *BM25InvertedIndex) termShardIndex(term string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(term)) // nosec G104
	return int(h.Sum32() % invertedIndexShards)
}

// docShardIndex returns the shard index for a document
func (idx *BM25InvertedIndex) docShardIndex(id VectorID) int {
	return int(uint64(id) % invertedIndexShards) //nolint:gosec // G115 - invertedIndexShards is small const
}

// DocCount returns the total number of documents in the index
func (idx *BM25InvertedIndex) DocCount() int {
	return int(idx.docCount.Load())
}

// GetDocLength returns the length (term count) of a document
func (idx *BM25InvertedIndex) GetDocLength(id VectorID) int {
	shardIdx := idx.docShardIndex(id)
	shard := &idx.docShards[shardIdx]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.length[id]
}

// GetTermDocFreq returns the number of documents containing the term
func (idx *BM25InvertedIndex) GetTermDocFreq(term string) int {
	shardIdx := idx.termShardIndex(term)
	shard := &idx.termShards[shardIdx]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if pl, ok := shard.index[term]; ok {
		return len(pl.DocIDs)
	}
	return 0
}

// Add indexes a document with the given text
func (idx *BM25InvertedIndex) Add(id VectorID, text string) {
	terms := tokenize(text)
	if len(terms) == 0 {
		return
	}

	docLen := len(terms)

	// Calculate term frequencies
	termFreq := make(map[string]int)
	for _, term := range terms {
		termFreq[term]++
	}

	// Store document metadata
	docShardIdx := idx.docShardIndex(id)
	idx.docShards[docShardIdx].mu.Lock()
	_, existed := idx.docShards[docShardIdx].terms[id]
	oldLen := idx.docShards[docShardIdx].length[id]
	idx.docShards[docShardIdx].terms[id] = terms
	idx.docShards[docShardIdx].length[id] = docLen
	idx.docShards[docShardIdx].mu.Unlock()

	// Update doc count and total length
	if !existed {
		idx.docCount.Add(1)
		idx.totalLen.Add(int64(docLen))
	} else {
		// Update total length delta
		idx.totalLen.Add(int64(docLen - oldLen))
	}

	// Update scorer corpus stats
	idx.scorer.AddDocument(docLen)

	// Group terms by shard
	shardTerms := make(map[int]map[string]int)
	for term, freq := range termFreq {
		shardIdx := idx.termShardIndex(term)
		if shardTerms[shardIdx] == nil {
			shardTerms[shardIdx] = make(map[string]int)
		}
		shardTerms[shardIdx][term] = freq
	}

	// Apply updates per shard
	for shardIdx, terms := range shardTerms {
		shard := &idx.termShards[shardIdx]
		shard.mu.Lock()
		for term, freq := range terms {
			pl := shard.index[term]
			if pl == nil {
				pl = &BM25PostingList{
					DocIDs: make([]VectorID, 0, 1),
					TFs:    make([]int, 0, 1),
				}
				shard.index[term] = pl
			}
			shard.bloom.Add(term)
			
			// Maintain sorted order by DocID
			n := len(pl.DocIDs)
			idx := sort.Search(n, func(i int) bool {
				return pl.DocIDs[i] >= id
			})
			if idx < n && pl.DocIDs[idx] == id {
				pl.TFs[idx] = freq
			} else {
				// Insert at idx
				pl.DocIDs = append(pl.DocIDs, 0)
				copy(pl.DocIDs[idx+1:], pl.DocIDs[idx:n])
				pl.DocIDs[idx] = id

				pl.TFs = append(pl.TFs, 0)
				copy(pl.TFs[idx+1:], pl.TFs[idx:n])
				pl.TFs[idx] = freq
			}
		}
		shard.mu.Unlock()
	}
}

// Delete removes a document from the index
func (idx *BM25InvertedIndex) Delete(id VectorID) {
	// Get document terms and length first
	docShardIdx := idx.docShardIndex(id)
	idx.docShards[docShardIdx].mu.Lock()
	terms, exists := idx.docShards[docShardIdx].terms[id]
	docLen := idx.docShards[docShardIdx].length[id]
	if exists {
		delete(idx.docShards[docShardIdx].terms, id)
		delete(idx.docShards[docShardIdx].length, id)
	}
	idx.docShards[docShardIdx].mu.Unlock()

	if !exists {
		return
	}

	// Update counts
	idx.docCount.Add(-1)
	idx.totalLen.Add(-int64(docLen))

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
			if pl, ok := shard.index[term]; ok {
				n := len(pl.DocIDs)
				idx := sort.Search(n, func(i int) bool {
					return pl.DocIDs[i] >= id
				})
				if idx < n && pl.DocIDs[idx] == id {
					copy(pl.DocIDs[idx:], pl.DocIDs[idx+1:])
					pl.DocIDs = pl.DocIDs[:n-1]

					copy(pl.TFs[idx:], pl.TFs[idx+1:])
					pl.TFs = pl.TFs[:n-1]
					
					if len(pl.DocIDs) == 0 {
						delete(shard.index, term)
					}
				}
			}
		}
		shard.mu.Unlock()
	}
}

// SearchBM25 returns documents matching the query, scored by BM25
func (idx *BM25InvertedIndex) SearchBM25(query string, limit int, filter *roaring.Bitmap) []SearchResult {
	queryTerms := tokenize(query)
	if len(queryTerms) == 0 {
		return nil
	}

	totalDocs := idx.DocCount()
	if totalDocs == 0 {
		return nil
	}

	// Group query terms by shard for efficient lookup
	shardTerms := make(map[int][]string)
	for _, term := range queryTerms {
		shardIdx := idx.termShardIndex(term)
		shardTerms[shardIdx] = append(shardTerms[shardIdx], term)
	}

	// Collect term frequencies and document frequencies
	// docScores: docID -> accumulated BM25 score
	docScores := make(map[VectorID]float32)
	// Track which docs we need lengths for
	docSet := make(map[VectorID]struct{})

	// Temporary storage: term -> filtered posting list
	termDocTF := make(map[string]*BM25PostingList)
	termDF := make(map[string]int) // term -> document frequency

	// Gather all term data from shards
	for shardIdx, terms := range shardTerms {
		shard := &idx.termShards[shardIdx]
		shard.mu.RLock()
		for _, term := range terms {
			// Bloom filter pre-check
			if !shard.bloom.Contains(term) {
				continue
			}
			if pl, ok := shard.index[term]; ok {
				termDF[term] = len(pl.DocIDs)
				filteredPL := &BM25PostingList{
					DocIDs: make([]VectorID, 0, len(pl.DocIDs)),
					TFs:    make([]int, 0, len(pl.TFs)),
				}
				for i, docID := range pl.DocIDs {
					if filter != nil && !filter.Contains(uint32(docID)) {
						continue
					}
					filteredPL.DocIDs = append(filteredPL.DocIDs, docID)
					filteredPL.TFs = append(filteredPL.TFs, pl.TFs[i])
					docSet[docID] = struct{}{}
				}
				termDocTF[term] = filteredPL
			}
		}
		shard.mu.RUnlock()
	}

	if len(docSet) == 0 {
		return nil
	}

	// Get document lengths in batch
	docLengths := make(map[VectorID]int)
	for docID := range docSet {
		docLengths[docID] = idx.GetDocLength(docID)
	}

	// Calculate BM25 scores
	avgDL := idx.scorer.AvgDocLength()
	for term, pl := range termDocTF {
		df := termDF[term]

		docLengthsList := make([]int, len(pl.DocIDs))
		for i, docID := range pl.DocIDs {
			docLengthsList[i] = docLengths[docID]
		}
		
		// Fallback to sequential for now until SIMD kernel is implemented
		k1 := float32(idx.scorer.Config().K1)
		b := float32(idx.scorer.Config().B)
		idf := float32(idx.scorer.IDF(df))
		scores := simd.BM25ScoreBatch(pl.TFs, docLengthsList, float32(avgDL), idf, k1, b)

		for i, docID := range pl.DocIDs {
			docScores[docID] += scores[i]
		}
	}

	// Convert to results and sort
	results := make([]SearchResult, 0, len(docScores))
	for id, score := range docScores {
		results = append(results, SearchResult{ID: lbtypes.VectorID(id), Score: score})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results
}

// tokenize is a simple whitespace tokenizer with basic cleaning
func tokenize(text string) []string {
	// Use Fields to split by any whitespace
	fields := strings.Fields(strings.ToLower(text))
	tokens := make([]string, 0, len(fields))
	for _, f := range fields {
		clean := strings.Trim(f, ".,!?;:()[]{}'\"><")
		if clean != "" {
			tokens = append(tokens, clean)
		}
	}
	return tokens
}

// Close releases resources associated with the BM25 index.
func (idx *BM25InvertedIndex) Close() error {
	for i := 0; i < invertedIndexShards; i++ {
		idx.termShards[i].mu.Lock()
		idx.termShards[i].index = nil
		idx.termShards[i].mu.Unlock()

		idx.docShards[i].mu.Lock()
		idx.docShards[i].terms = nil
		idx.docShards[i].length = nil
		idx.docShards[i].mu.Unlock()
	}
	idx.scorer = nil
	return nil
}
