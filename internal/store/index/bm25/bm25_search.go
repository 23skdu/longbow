package bm25

import (
	"sort"
	"strings"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
)

// SearchBM25 returns documents matching the query, scored by BM25
func (idx *BM25InvertedIndex) SearchBM25(query string, limit int, filter *roaring.Bitmap, pool *types.SearchResultPool) []types.SearchResult {
	queryTerms := Tokenize(query)
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

	// Pre-convert roaring filter to dense BitVector for O(1) cache-friendly access
	var bvFilter types.BitVector
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

		for _, term := range terms {
			// Bloom filter pre-check
			if !shard.bloom.Contains(term) {
				continue
			}
			if pl, ok := shard.index.Get(term); ok {
				termDF[term] = len(pl.DocIDs)

				// Optimization: If no filter, use posting list directly
				if filter == nil {
					termDocTF[term] = pl
					continue
				}

				filteredPL := &BM25PostingList{
					DocIDs:  make([]types.VectorID, 0, len(pl.DocIDs)/4), // heuristic initial capacity
					TFs:     make([]int, 0, len(pl.TFs)/4),
					DocLens: make([]int, 0, len(pl.DocLens)/4),
				}
				for i, docID := range pl.DocIDs {
					if !bvFilter.Get(uint32(docID)) {
						continue
					}
					filteredPL.DocIDs = append(filteredPL.DocIDs, docID)
					filteredPL.TFs = append(filteredPL.TFs, pl.TFs[i])
					if i < len(pl.DocLens) {
						filteredPL.DocLens = append(filteredPL.DocLens, pl.DocLens[i])
					} else {
						filteredPL.DocLens = append(filteredPL.DocLens, idx.GetDocLength(docID))
					}
				}
				if len(filteredPL.DocIDs) > 0 {
					termDocTF[term] = filteredPL
				}
			}
		}
	}

	if len(termDocTF) == 0 {
		return nil
	}

	avgDL := float32(idx.scorer.AvgDocLength())
	k1 := float32(idx.config.K1)
	b := float32(idx.config.B)

	searchLimit := limit
	if searchLimit <= 0 {
		searchLimit = 1000 // default fallback
	}

	var heap []types.SearchResult
	theta := float32(0.0)

	siftUp := func(i int) {
		for i > 0 {
			parent := (i - 1) / 2
			if heap[parent].Score <= heap[i].Score {
				break
			}
			heap[parent], heap[i] = heap[i], heap[parent]
			i = parent
		}
	}

	siftDown := func(i int) {
		n := len(heap)
		for {
			left := 2*i + 1
			if left >= n {
				break
			}
			smallest := left
			right := left + 1
			if right < n && heap[right].Score < heap[left].Score {
				smallest = right
			}
			if heap[i].Score <= heap[smallest].Score {
				break
			}
			heap[i], heap[smallest] = heap[smallest], heap[i]
			i = smallest
		}
	}

	// Fast path: single-term query evaluated with SIMD vector scoring
	if len(termDocTF) == 1 {
		var singleTerm string
		var pl *BM25PostingList
		for t, p := range termDocTF {
			singleTerm = t
			pl = p
		}
		if len(pl.DocIDs) == 0 {
			return nil
		}

		df := termDF[singleTerm]
		idf := float32(idx.scorer.IDF(df))

		docLens := pl.DocLens
		if len(docLens) < len(pl.DocIDs) {
			docLens = make([]int, len(pl.DocIDs))
			for i, id := range pl.DocIDs {
				docLens[i] = idx.GetDocLength(id)
			}
		}

		scores := simd.BM25ScoreBatch(pl.TFs, docLens, avgDL, idf, k1, b)
		for i, sc := range scores {
			if sc <= 0 {
				continue
			}
			if len(heap) < searchLimit {
				heap = append(heap, types.SearchResult{ID: types.VectorID(pl.DocIDs[i]), Score: sc})
				siftUp(len(heap) - 1)
				if len(heap) == searchLimit {
					theta = heap[0].Score
				}
			} else if sc > theta {
				heap[0] = types.SearchResult{ID: types.VectorID(pl.DocIDs[i]), Score: sc}
				siftDown(0)
				theta = heap[0].Score
			}
		}

		results := make([]types.SearchResult, len(heap))
		for i := len(heap) - 1; i >= 0; i-- {
			results[i] = heap[0]
			heap[0] = heap[len(heap)-1]
			heap = heap[:len(heap)-1]
			if len(heap) > 0 {
				siftDown(0)
			}
		}
		if pool != nil {
			final := make([]types.SearchResult, len(results))
			copy(final, results)
			return final
		}
		return results
	}

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

		blockSize := bm25BlockSize
		numBlocks := (len(pl.DocIDs) + blockSize - 1) / blockSize
		blockMaxScores := make([]float32, numBlocks)
		globalMaxScore := float32(0.0)

		for bi := 0; bi < numBlocks; bi++ {
			var maxTF, minLen int
			if bi < len(pl.BlockMaxTF) && bi < len(pl.BlockMinLen) && pl.BlockMinLen[bi] < 99999999 {
				maxTF = pl.BlockMaxTF[bi]
				minLen = pl.BlockMinLen[bi]
			} else {
				maxTF = 0
				minLen = 99999999
				start := bi * blockSize
				end := start + blockSize
				if end > len(pl.DocIDs) {
					end = len(pl.DocIDs)
				}
				for i := start; i < end; i++ {
					if pl.TFs[i] > maxTF {
						maxTF = pl.TFs[i]
					}
					l := 0
					if i < len(pl.DocLens) {
						l = pl.DocLens[i]
					} else {
						l = idx.GetDocLength(pl.DocIDs[i])
					}
					if l < minLen {
						minLen = l
					}
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
					l := 0
					if p.pos < len(p.pl.DocLens) {
						l = p.pl.DocLens[p.pos]
					} else {
						l = idx.GetDocLength(pivotDocID)
					}
					actualScore += scoreSingle(p.pl.TFs[p.pos], l, avgDL, idf, k1, b)
				}
			}

			// Try to insert in heap
			if len(heap) < searchLimit {
				heap = append(heap, types.SearchResult{ID: types.VectorID(pivotDocID), Score: actualScore})
				siftUp(len(heap) - 1)
				if len(heap) == searchLimit {
					theta = heap[0].Score
				}
			} else if actualScore > theta {
				heap[0] = types.SearchResult{ID: types.VectorID(pivotDocID), Score: actualScore}
				siftDown(0)
				theta = heap[0].Score
			}

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

	// Convert min-heap to descending results
	results := make([]types.SearchResult, len(heap))
	for i := len(heap) - 1; i >= 0; i-- {
		results[i] = heap[0]
		heap[0] = heap[len(heap)-1]
		heap = heap[:len(heap)-1]
		if len(heap) > 0 {
			siftDown(0)
		}
	}

	if pool != nil {
		final := make([]types.SearchResult, len(results))
		copy(final, results)
		return final
	}

	return results
}

// Tokenize is a simple whitespace tokenizer with basic cleaning
func Tokenize(text string) []string {
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
