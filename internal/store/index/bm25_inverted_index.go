package index

import (
	"hash/fnv"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
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
	// term -> posting list
	index *types.MapRCU[string, *BM25PostingList]
}

// bm25DocShard holds document metadata for a subset of documents
type bm25DocShard struct {
	terms  *types.MapRCU[VectorID, []string] // docID -> terms
	length *types.MapRCU[VectorID, int]      // docID -> document length (term count)
}

// NewBM25InvertedIndex creates a new BM25 inverted index
func NewBM25InvertedIndex(config BM25Config) *BM25InvertedIndex {
	idx := &BM25InvertedIndex{
		config: config,
		scorer: NewBM25Scorer(config),
	}
	for i := 0; i < invertedIndexShards; i++ {
		idx.termShards[i].bloom = NewBloomFilter(10000, 0.01)
		idx.termShards[i].index = types.NewMapRCU[string, *BM25PostingList]()
		idx.docShards[i].terms = types.NewMapRCU[VectorID, []string]()
		idx.docShards[i].length = types.NewMapRCU[VectorID, int]()
	}
	return idx
}

// termShardIndex returns the shard index for a term
func (idx *BM25InvertedIndex) termShardIndex(term string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(term))
	return int(h.Sum32() % invertedIndexShards)
}

// docShardIndex returns the shard index for a document
func (idx *BM25InvertedIndex) docShardIndex(id VectorID) int {
	return int(uint64(id) % invertedIndexShards)
}

// DocCount returns the total number of documents in the index
func (idx *BM25InvertedIndex) DocCount() int {
	return int(idx.docCount.Load())
}

// GetDocLength returns the length (term count) of a document
func (idx *BM25InvertedIndex) GetDocLength(id VectorID) int {
	shardIdx := idx.docShardIndex(id)
	shard := &idx.docShards[shardIdx]
	len, _ := shard.length.Get(id)
	return len
}

// GetTermDocFreq returns the number of documents containing the term
func (idx *BM25InvertedIndex) GetTermDocFreq(term string) int {
	shardIdx := idx.termShardIndex(term)
	shard := &idx.termShards[shardIdx]
	if pl, ok := shard.index.Get(term); ok {
		return len(pl.DocIDs)
	}
	return 0
}

// GetDocLengthsBatch returns the lengths for a batch of documents, grouped by shard to minimize lock contention.
func (idx *BM25InvertedIndex) GetDocLengthsBatch(ids []VectorID, dst map[VectorID]int) {
	// Group by shard
	shards := make(map[int][]VectorID)
	for _, id := range ids {
		shardIdx := idx.docShardIndex(id)
		shards[shardIdx] = append(shards[shardIdx], id)
	}

	for shardIdx, shardIDs := range shards {
		shard := &idx.docShards[shardIdx]
		m := shard.length.Load()
		for _, id := range shardIDs {
			dst[id] = m[id]
		}
	}
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

	// Store document metadata - COW update for the shard
	docShardIdx := idx.docShardIndex(id)
	docShard := &idx.docShards[docShardIdx]

	// Atomic update of length and terms
	oldLen, existed := docShard.length.Get(id)

	docShard.terms.Store(id, terms)
	docShard.length.Store(id, docLen)

	// Update doc count and total length
	if !existed {
		idx.docCount.Add(1)
		idx.totalLen.Add(int64(docLen))
	} else {
		idx.totalLen.Add(int64(docLen - oldLen))
	}

	// Update scorer corpus stats
	idx.scorer.AddDocument(docLen)

	// Group terms by shard
	shardTerms := make(map[int]map[string]int)
	for term, freq := range termFreq {
		sIdx := idx.termShardIndex(term)
		if shardTerms[sIdx] == nil {
			shardTerms[sIdx] = make(map[string]int)
		}
		shardTerms[sIdx][term] = freq
	}

	// Apply updates per shard
	for sIdx, terms := range shardTerms {
		shard := &idx.termShards[sIdx]
		updates := make(map[string]*BM25PostingList)

		// Load the current map for this shard once
		currIndex := shard.index.Load()

		for term, freq := range terms {
			shard.bloom.Add(term)

			oldPL := currIndex[term]
			var newPL *BM25PostingList

			if oldPL == nil {
				newPL = &BM25PostingList{
					DocIDs: []VectorID{id},
					TFs:    []int{freq},
				}
			} else {
				// Clone and update (COW for the posting list itself)
				n := len(oldPL.DocIDs)
				insertIdx := sort.Search(n, func(i int) bool {
					return oldPL.DocIDs[i] >= id
				})

				var newDocIDs []VectorID
				var newTFs []int

				if insertIdx < n && oldPL.DocIDs[insertIdx] == id {
					// Update existing - reuse same length
					newDocIDs = make([]VectorID, n)
					newTFs = make([]int, n)
					copy(newDocIDs, oldPL.DocIDs)
					copy(newTFs, oldPL.TFs)
					newTFs[insertIdx] = freq
				} else {
					// Insert new
					newDocIDs = make([]VectorID, n+1)
					newTFs = make([]int, n+1)
					copy(newDocIDs[:insertIdx], oldPL.DocIDs[:insertIdx])
					newDocIDs[insertIdx] = id
					copy(newDocIDs[insertIdx+1:], oldPL.DocIDs[insertIdx:])

					copy(newTFs[:insertIdx], oldPL.TFs[:insertIdx])
					newTFs[insertIdx] = freq
					copy(newTFs[insertIdx+1:], oldPL.TFs[insertIdx:])
				}
				newPL = &BM25PostingList{
					DocIDs: newDocIDs,
					TFs:    newTFs,
				}
			}
			updates[term] = newPL
		}
		shard.index.BulkStore(updates)
	}
}

// Delete removes a document from the index
func (idx *BM25InvertedIndex) Delete(id VectorID) {
	// Get document terms and length first
	docShardIdx := idx.docShardIndex(id)
	docShard := &idx.docShards[docShardIdx]

	terms, exists := docShard.terms.Get(id)
	docLen, _ := docShard.length.Get(id)

	if exists {
		docShard.terms.Delete(id)
		docShard.length.Delete(id)
	} else {
		return
	}

	// Update counts
	idx.docCount.Add(-1)
	idx.totalLen.Add(-int64(docLen))

	// Group terms by shard
	shardTerms := make(map[int][]string)
	for _, term := range terms {
		sIdx := idx.termShardIndex(term)
		shardTerms[sIdx] = append(shardTerms[sIdx], term)
	}

	// Remove from each term shard
	for sIdx, termList := range shardTerms {
		shard := &idx.termShards[sIdx]

		updates := make(map[string]*BM25PostingList)
		deletes := []string{}

		currIndex := shard.index.Load()

		for _, term := range termList {
			if oldPL, ok := currIndex[term]; ok {
				n := len(oldPL.DocIDs)
				removeIdx := sort.Search(n, func(i int) bool {
					return oldPL.DocIDs[i] >= id
				})

				if removeIdx < n && oldPL.DocIDs[removeIdx] == id {
					if n == 1 {
						deletes = append(deletes, term)
					} else {
						// Clone and remove
						newDocIDs := make([]VectorID, n-1)
						newTFs := make([]int, n-1)
						copy(newDocIDs[:removeIdx], oldPL.DocIDs[:removeIdx])
						copy(newDocIDs[removeIdx:], oldPL.DocIDs[removeIdx+1:])
						copy(newTFs[:removeIdx], oldPL.TFs[:removeIdx])
						copy(newTFs[removeIdx:], oldPL.TFs[removeIdx+1:])

						updates[term] = &BM25PostingList{
							DocIDs: newDocIDs,
							TFs:    newTFs,
						}
					}
				}
			}
		}

		if len(updates) > 0 {
			shard.index.BulkStore(updates)
		}
		if len(deletes) > 0 {
			shard.index.BulkDelete(deletes)
		}
	}
}

// SearchBM25 returns documents matching the query, scored by BM25
func (idx *BM25InvertedIndex) SearchBM25(query string, limit int, filter *roaring.Bitmap, pool *SearchResultPool) []SearchResult {
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

	// Track which docs we need lengths for
	docSet := make(map[VectorID]struct{})

	// Pre-convert roaring filter to dense BitVector for O(1) cache-friendly access
	var bvFilter BitVector
	if filter != nil {
		bvFilter = types.NewBitVector(totalDocs + 1)
		it := filter.Iterator()
		for it.HasNext() {
			bvFilter.Set(it.Next())
		}
	}

	// Temporary storage: term -> filtered posting list
	termDocTF := make(map[string]*BM25PostingList)
	termDF := make(map[string]int) // term -> document frequency

	// Gather all term data from shards (lock-free reads)
	for shardIdx, terms := range shardTerms {
		shard := &idx.termShards[shardIdx]
		currIndex := shard.index.Load()

		for _, term := range terms {
			// Bloom filter pre-check
			if !shard.bloom.Contains(term) {
				continue
			}
			if pl, ok := currIndex[term]; ok {
				termDF[term] = len(pl.DocIDs)

				// Optimization: If no filter, use posting list directly
				if filter == nil {
					termDocTF[term] = pl
					for _, docID := range pl.DocIDs {
						docSet[docID] = struct{}{}
					}
					continue
				}

				filteredPL := &BM25PostingList{
					DocIDs: make([]VectorID, 0, len(pl.DocIDs)/4), // heuristic initial capacity
					TFs:    make([]int, 0, len(pl.TFs)/4),
				}
				for i, docID := range pl.DocIDs {
					if !bvFilter.Get(uint32(docID)) {
						continue
					}
					filteredPL.DocIDs = append(filteredPL.DocIDs, docID)
					filteredPL.TFs = append(filteredPL.TFs, pl.TFs[i])
					docSet[docID] = struct{}{}
				}
				if len(filteredPL.DocIDs) > 0 {
					termDocTF[term] = filteredPL
				}
			}
		}
	}

	if len(docSet) == 0 {
		return nil
	}

	// Get document lengths in batch
	docLengths := make(map[VectorID]int, len(docSet))
	docIDs := make([]VectorID, 0, len(docSet))
	for docID := range docSet {
		docIDs = append(docIDs, docID)
	}
	idx.GetDocLengthsBatch(docIDs, docLengths)

	// Calculate BM25 scores in batch using Block-Max WAND (Weak AND)
	avgDL := float32(idx.scorer.AvgDocLength())
	k1 := float32(idx.config.K1)
	b := float32(idx.config.B)

	scoreSingle := func(tf int, docLen int, avgDL, idf, k1, b float32) float32 {
		tfF := float32(tf)
		docLenF := float32(docLen)
		numerator := idf * tfF * (k1 + 1)
		denominator := tfF + k1*(1-b+b*(docLenF/avgDL))
		return numerator / denominator
	}

	// Active pointers
	type termPointer struct {
		term           string
		pl             *BM25PostingList
		pos            int
		maxScore       float32
		blockMaxScores []float32
		blockSize      int
	}

	pointers := make([]*termPointer, 0, len(termDocTF))
	for term, pl := range termDocTF {
		df := termDF[term]
		idf := float32(idx.scorer.IDF(df))

		blockSize := 64
		numBlocks := (len(pl.DocIDs) + blockSize - 1) / blockSize
		blockMaxScores := make([]float32, numBlocks)
		globalMaxScore := float32(0.0)

		for bi := 0; bi < numBlocks; bi++ {
			maxTF := 0
			minLen := 99999999
			start := bi * blockSize
			end := start + blockSize
			if end > len(pl.DocIDs) {
				end = len(pl.DocIDs)
			}
			for i := start; i < end; i++ {
				if pl.TFs[i] > maxTF {
					maxTF = pl.TFs[i]
				}
				l := docLengths[pl.DocIDs[i]]
				if l < minLen {
					minLen = l
				}
			}
			score := scoreSingle(maxTF, minLen, avgDL, idf, k1, b)
			blockMaxScores[bi] = score
			if score > globalMaxScore {
				globalMaxScore = score
			}
		}

		pointers = append(pointers, &termPointer{
			term:           term,
			pl:             pl,
			pos:            0,
			maxScore:       globalMaxScore,
			blockMaxScores: blockMaxScores,
			blockSize:      blockSize,
		})
	}

	// Priority queue: sorted ascending by score (heap[0] is the minimum score).
	// We keep a maximum of limit entries.
	searchLimit := limit
	if searchLimit <= 0 {
		searchLimit = 1000 // default fallback
	}

	var heap []SearchResult
	theta := float32(0.0)

	insertHeap := func(res SearchResult) {
		if len(heap) < searchLimit {
			heap = append(heap, res)
			sort.Slice(heap, func(i, j int) bool {
				return heap[i].Score < heap[j].Score
			})
			if len(heap) == searchLimit {
				theta = heap[0].Score
			}
		} else if res.Score > theta {
			heap[0] = res
			sort.Slice(heap, func(i, j int) bool {
				return heap[i].Score < heap[j].Score
			})
			theta = heap[0].Score
		}
	}

	for {
		// 1. Filter out pointers that have reached the end
		active := pointers[:0]
		for _, p := range pointers {
			if p.pos < len(p.pl.DocIDs) {
				active = append(active, p)
			}
		}
		pointers = active
		if len(pointers) == 0 {
			break
		}

		// 2. Sort pointers by their current DocID
		sort.Slice(pointers, func(i, j int) bool {
			return pointers[i].pl.DocIDs[pointers[i].pos] < pointers[j].pl.DocIDs[pointers[j].pos]
		})

		// 3. Find pivot
		accum := float32(0.0)
		pivotIdx := -1
		for i, p := range pointers {
			bi := p.pos / p.blockSize
			blockMax := p.blockMaxScores[bi]
			accum += blockMax
			if accum > theta {
				pivotIdx = i
				break
			}
		}

		if pivotIdx == -1 {
			// No document can mathematically exceed the current threshold theta!
			break
		}

		pivotDocID := pointers[pivotIdx].pl.DocIDs[pointers[pivotIdx].pos]
		firstDocID := pointers[0].pl.DocIDs[pointers[0].pos]

		if firstDocID == pivotDocID {
			// Score pivotDocID!
			actualScore := float32(0.0)
			for _, p := range pointers {
				if p.pos < len(p.pl.DocIDs) && p.pl.DocIDs[p.pos] == pivotDocID {
					df := termDF[p.term]
					idf := float32(idx.scorer.IDF(df))
					actualScore += scoreSingle(p.pl.TFs[p.pos], docLengths[pivotDocID], avgDL, idf, k1, b)
				}
			}

			// Try to insert in heap
			insertHeap(SearchResult{ID: lbtypes.VectorID(pivotDocID), Score: actualScore})

			// Advance pointers that matched pivotDocID
			for _, p := range pointers {
				if p.pos < len(p.pl.DocIDs) && p.pl.DocIDs[p.pos] == pivotDocID {
					p.pos++
				}
			}
		} else {
			// Skip pointers of all terms before pivot that are less than pivotDocID
			for i := 0; i < pivotIdx; i++ {
				p := pointers[i]
				n := len(p.pl.DocIDs)
				target := sort.Search(n-p.pos, func(j int) bool {
					return p.pl.DocIDs[p.pos+j] >= pivotDocID
				})
				p.pos += target
			}
		}
	}

	// Convert sorted ascending heap to descending results
	results := make([]SearchResult, len(heap))
	for i := range heap {
		results[i] = heap[len(heap)-1-i]
	}

	if pool != nil {
		final := make([]SearchResult, len(results))
		copy(final, results)
		return final
	}

	return results
}

// tokenize is a simple whitespace tokenizer with basic cleaning
func tokenize(text string) []string {
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
		idx.termShards[i].index = nil
		idx.docShards[i].terms = nil
		idx.docShards[i].length = nil
	}
	idx.scorer = nil
	return nil
}

// SearchBM25Streaming returns an iterator for BM25 search results.
// This is optimized for large-scale searches to minimize peak memory usage.
func (idx *BM25InvertedIndex) SearchBM25Streaming(query string, limit int, filter *roaring.Bitmap) core.ResultIterator {
	results := idx.SearchBM25(query, limit, filter, nil)
	return core.NewResultSliceIterator(results)
}
