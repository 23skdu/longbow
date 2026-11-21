package bm25

import (
	"hash/fnv"
	"sort"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
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

const bm25BlockSize = 64

// BM25PostingList stores parallel slices for SIMD-accelerated scoring
// along with precomputed per-block (maxTF, minDocLen) upper-bound metadata for Block-Max WAND.
type BM25PostingList struct {
	DocIDs      []types.VectorID
	TFs         []int
	DocLens     []int
	BlockMaxTF  []int
	BlockMinLen []int
}

func newBM25PostingList(docIDs []types.VectorID, tfs, docLens []int) *BM25PostingList {
	numBlocks := (len(docIDs) + bm25BlockSize - 1) / bm25BlockSize
	blockMaxTF := make([]int, numBlocks)
	blockMinLen := make([]int, numBlocks)
	for bi := 0; bi < numBlocks; bi++ {
		start := bi * bm25BlockSize
		end := start + bm25BlockSize
		if end > len(docIDs) {
			end = len(docIDs)
		}
		maxTF := 0
		minLen := 99999999
		for i := start; i < end; i++ {
			if i < len(tfs) && tfs[i] > maxTF {
				maxTF = tfs[i]
			}
			if i < len(docLens) && docLens[i] > 0 && docLens[i] < minLen {
				minLen = docLens[i]
			}
		}
		blockMaxTF[bi] = maxTF
		blockMinLen[bi] = minLen
	}
	return &BM25PostingList{
		DocIDs:      docIDs,
		TFs:         tfs,
		DocLens:     docLens,
		BlockMaxTF:  blockMaxTF,
		BlockMinLen: blockMinLen,
	}
}

// bm25TermShard holds the inverted index for a subset of terms
type bm25TermShard struct {
	bloom *BloomFilter
	// term -> posting list
	index *types.MapRCU[string, *BM25PostingList]
}

// bm25DocShard holds document metadata for a subset of documents
type bm25DocShard struct {
	terms  *types.MapRCU[types.VectorID, []string] // docID -> terms
	length *types.MapRCU[types.VectorID, int]      // docID -> document length (term count)
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
		idx.docShards[i].terms = types.NewMapRCU[types.VectorID, []string]()
		idx.docShards[i].length = types.NewMapRCU[types.VectorID, int]()
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
func (idx *BM25InvertedIndex) docShardIndex(id types.VectorID) int {
	return int(uint64(id) % invertedIndexShards)
}

// DocCount returns the total number of documents in the index.
//
// This counts distinct documents, not insertions: re-indexing a document
// updates it in place. ScorerDocs exposes the scorer's own corpus count,
// which is what the IDF of every term is computed against - the two must
// agree, and a divergence between them is a scoring bug.
func (idx *BM25InvertedIndex) DocCount() int {
	return int(idx.docCount.Load())
}

// ScorerDocs returns the document count the BM25 scorer derives IDF from.
func (idx *BM25InvertedIndex) ScorerDocs() int {
	return idx.scorer.TotalDocs()
}

// GetDocLength returns the length (term count) of a document
func (idx *BM25InvertedIndex) GetDocLength(id types.VectorID) int {
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
func (idx *BM25InvertedIndex) GetDocLengthsBatch(ids []types.VectorID, dst map[types.VectorID]int) {
	for _, id := range ids {
		shardIdx := idx.docShardIndex(id)
		if l, ok := idx.docShards[shardIdx].length.Get(id); ok {
			dst[id] = l
		}
	}
}

// BM25Document is a pre-tokenised document ready for bulk insertion.
//
// Callers that already have the token slice - the ingestion pipeline, which
// tokenises every text cell exactly once and feeds both the legacy and the
// arena index - use this to avoid re-tokenising inside Add.
type BM25Document struct {
	ID     types.VectorID
	Tokens []string
}

// Add indexes a document with the given text.
func (idx *BM25InvertedIndex) Add(id types.VectorID, text string) {
	terms := Tokenize(text)
	if len(terms) == 0 {
		return
	}
	idx.AddBatch([]BM25Document{{ID: id, Tokens: terms}})
}

// AddBatch indexes a group of documents, coalescing every write into one
// copy-on-write per shard.
//
// Add rewrites a whole shard map and every touched posting list once per
// document. Doing that for a batch of D documents costs O(S * |shard| * D) map
// clones plus O(D) posting-list reallocations, and profiling a pure-vector
// benchmark attributed ~943 GB of process allocation and ~120 s of mutex
// delay to exactly this path. Grouping by shard first collapses the whole
// batch into a single BulkStore per shard and one posting-list rebuild per
// touched term, so the same work costs O(S) map clones regardless of D.
//
// Callers must not retain or mutate Tokens after the call returns.
func (idx *BM25InvertedIndex) AddBatch(docs []BM25Document) {
	if len(docs) == 0 {
		return
	}

	// shardTerms[shard][term] collects every (docID, tf) contributed by this
	// batch. A term can legitimately be touched by many documents in the same
	// batch, and all of them must land in a single rebuilt posting list.
	type posting struct {
		id  types.VectorID
		tf  int
		len int
	}
	shardTerms := make(map[int]map[string][]posting)

	// docShardWrites groups the per-document metadata by doc shard so the
	// length/terms maps are also rewritten once per shard, not once per doc.
	docShardTerms := make(map[int]map[types.VectorID][]string)
	docShardLengths := make(map[int]map[types.VectorID]int)

	for _, doc := range docs {
		if len(doc.Tokens) == 0 {
			continue
		}
		docLen := len(doc.Tokens)

		docShardIdx := idx.docShardIndex(doc.ID)
		docShard := &idx.docShards[docShardIdx]
		oldLen, existed := docShard.length.Get(doc.ID)

		if docShardTerms[docShardIdx] == nil {
			docShardTerms[docShardIdx] = make(map[types.VectorID][]string)
			docShardLengths[docShardIdx] = make(map[types.VectorID]int)
		}
		docShardTerms[docShardIdx][doc.ID] = doc.Tokens
		docShardLengths[docShardIdx][doc.ID] = docLen

		if !existed {
			idx.docCount.Add(1)
			idx.totalLen.Add(int64(docLen))
			// Only genuinely new documents belong in the scorer corpus: the
			// previous per-document path called AddDocument unconditionally,
			// so re-indexing a document inflated totalDocs and with it the
			// IDF of every term.
			idx.scorer.AddDocument(docLen)
		} else {
			idx.totalLen.Add(int64(docLen - oldLen))
		}

		// Term frequencies for this document.
		for _, term := range doc.Tokens {
			sIdx := idx.termShardIndex(term)
			if shardTerms[sIdx] == nil {
				shardTerms[sIdx] = make(map[string][]posting)
			}
			shardTerms[sIdx][term] = append(shardTerms[sIdx][term], posting{id: doc.ID, tf: 1, len: docLen})
		}
	}

	// Fold the repeated (term, docID) pairs Add emitted one at a time into a
	// single posting entry per term per document.
	for _, byTerm := range shardTerms {
		for term, ps := range byTerm {
			if len(ps) < 2 {
				continue
			}
			// Documents within one batch arrive in ascending order for the
			// ingestion pipeline but AddBatch is a public API, so sort by doc
			// ID before collapsing duplicates.
			sort.Slice(ps, func(a, b int) bool { return ps[a].id < ps[b].id })
			merged := ps[:1]
			for _, p := range ps[1:] {
				last := &merged[len(merged)-1]
				if last.id == p.id {
					last.tf += p.tf
					continue
				}
				merged = append(merged, p)
			}
			byTerm[term] = merged
		}
	}

	for sIdx, byTerm := range shardTerms {
		shard := &idx.termShards[sIdx]
		updates := make(map[string]*BM25PostingList, len(byTerm))

		for term, ps := range byTerm {
			shard.bloom.Add(term)

			oldPL, _ := shard.index.Get(term)
			var newDocIDs []types.VectorID
			var newTFs []int
			var newDocLens []int

			if oldPL == nil {
				newDocIDs = make([]types.VectorID, len(ps))
				newTFs = make([]int, len(ps))
				newDocLens = make([]int, len(ps))
				for k, p := range ps {
					newDocIDs[k] = p.id
					newTFs[k] = p.tf
					newDocLens[k] = p.len
				}
			} else {
				// A sorted merge of the existing posting list and the batch's
				// contributions: one pass over each, instead of reallocating
				// and re-copying the whole list once per document.
				newDocIDs = make([]types.VectorID, 0, len(oldPL.DocIDs)+len(ps))
				newTFs = make([]int, 0, len(oldPL.DocIDs)+len(ps))
				newDocLens = make([]int, 0, len(oldPL.DocIDs)+len(ps))

				i, j := 0, 0
				for i < len(oldPL.DocIDs) && j < len(ps) {
					switch {
					case oldPL.DocIDs[i] < ps[j].id:
						newDocIDs = append(newDocIDs, oldPL.DocIDs[i])
						newTFs = append(newTFs, oldPL.TFs[i])
						if i < len(oldPL.DocLens) {
							newDocLens = append(newDocLens, oldPL.DocLens[i])
						} else {
							newDocLens = append(newDocLens, idx.GetDocLength(oldPL.DocIDs[i]))
						}
						i++
					case oldPL.DocIDs[i] > ps[j].id:
						newDocIDs = append(newDocIDs, ps[j].id)
						newTFs = append(newTFs, ps[j].tf)
						newDocLens = append(newDocLens, ps[j].len)
						j++
					default:
						// Document already indexed: the batch wins.
						newDocIDs = append(newDocIDs, ps[j].id)
						newTFs = append(newTFs, ps[j].tf)
						newDocLens = append(newDocLens, ps[j].len)
						i++
						j++
					}
				}
				for ; i < len(oldPL.DocIDs); i++ {
					newDocIDs = append(newDocIDs, oldPL.DocIDs[i])
					newTFs = append(newTFs, oldPL.TFs[i])
					if i < len(oldPL.DocLens) {
						newDocLens = append(newDocLens, oldPL.DocLens[i])
					} else {
						newDocLens = append(newDocLens, idx.GetDocLength(oldPL.DocIDs[i]))
					}
				}
				for ; j < len(ps); j++ {
					newDocIDs = append(newDocIDs, ps[j].id)
					newTFs = append(newTFs, ps[j].tf)
					newDocLens = append(newDocLens, ps[j].len)
				}
			}

			updates[term] = newBM25PostingList(newDocIDs, newTFs, newDocLens)
		}

		shard.index.BulkStore(updates)
	}

	for sIdx, termsByDoc := range docShardTerms {
		docShard := &idx.docShards[sIdx]
		docShard.terms.BulkStore(termsByDoc)
		docShard.length.BulkStore(docShardLengths[sIdx])
	}
}

// Delete removes a document from the index
func (idx *BM25InvertedIndex) Delete(id types.VectorID) {
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

		for _, term := range termList {
			if oldPL, ok := shard.index.Get(term); ok {
				n := len(oldPL.DocIDs)
				removeIdx := sort.Search(n, func(i int) bool {
					return oldPL.DocIDs[i] >= id
				})

				if removeIdx < n && oldPL.DocIDs[removeIdx] == id {
					if n == 1 {
						deletes = append(deletes, term)
					} else {
						// Clone and remove
						newDocIDs := make([]types.VectorID, n-1)
						newTFs := make([]int, n-1)
						newDocLens := make([]int, n-1)
						copy(newDocIDs[:removeIdx], oldPL.DocIDs[:removeIdx])
						copy(newDocIDs[removeIdx:], oldPL.DocIDs[removeIdx+1:])
						copy(newTFs[:removeIdx], oldPL.TFs[:removeIdx])
						copy(newTFs[removeIdx:], oldPL.TFs[removeIdx+1:])
						if len(oldPL.DocLens) == n {
							copy(newDocLens[:removeIdx], oldPL.DocLens[:removeIdx])
							copy(newDocLens[removeIdx:], oldPL.DocLens[removeIdx+1:])
						}
						updates[term] = newBM25PostingList(newDocIDs, newTFs, newDocLens)
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
