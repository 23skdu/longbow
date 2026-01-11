package store

import (
	"math"
	"sync"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
)

// BM25ArenaIndex implements BM25 text indexing using arena-based storage
// for improved cache locality and reduced GC pressure.
type BM25ArenaIndex struct {
	mu sync.RWMutex

	arena        *memory.SlabArena
	tokenArena   *memory.TypedArena[byte]
	postingArena *memory.TypedArena[uint32]

	// Token dictionary: token string -> tokenID
	tokenDict   map[string]uint32
	nextTokenID uint32

	// Inverted index: tokenID -> posting list (docIDs)
	postings *PackedAdjacency

	// Term frequencies: (docID << 32 | tokenID) -> frequency
	termFreqs map[uint64]uint32

	// Document frequencies: tokenID -> DF
	docFreqs []uint32

	// Document lengths for normalization
	docLengths   []uint32
	totalDocs    uint32
	avgDocLength float64

	// BM25 parameters
	k1 float64
	b  float64
}

// NewBM25ArenaIndex creates a new arena-based BM25 index
func NewBM25ArenaIndex(arena *memory.SlabArena, initialCapacity int) *BM25ArenaIndex {
	return &BM25ArenaIndex{
		arena:        arena,
		tokenArena:   memory.NewTypedArena[byte](arena),
		postingArena: memory.NewTypedArena[uint32](arena),
		tokenDict:    make(map[string]uint32),
		nextTokenID:  1, // Start from 1, 0 is reserved
		postings:     NewPackedAdjacency(arena, initialCapacity),
		termFreqs:    make(map[uint64]uint32),
		docFreqs:     make([]uint32, 0, 1000),
		docLengths:   make([]uint32, 0, initialCapacity),
		k1:           1.2,
		b:            0.75,
	}
}

// IndexDocument indexes a document with its tokens
func (idx *BM25ArenaIndex) IndexDocument(docID uint32, tokens []string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Ensure docLengths array is large enough
	for uint32(len(idx.docLengths)) <= docID {
		idx.docLengths = append(idx.docLengths, 0)
	}

	// Count term frequencies for this document
	localTF := make(map[uint32]uint32)
	for _, token := range tokens {
		tokenID := idx.getOrCreateTokenID(token)
		localTF[tokenID]++
	}

	// Update document length
	idx.docLengths[docID] = uint32(len(tokens))
	idx.totalDocs++
	idx.avgDocLength = (idx.avgDocLength*float64(idx.totalDocs-1) + float64(len(tokens))) / float64(idx.totalDocs)

	// Update term frequencies and posting lists
	for tokenID, freq := range localTF {
		// Store term frequency
		key := (uint64(docID) << 32) | uint64(tokenID)
		idx.termFreqs[key] = freq

		// Update posting list
		postings, exists := idx.postings.GetNeighbors(tokenID)
		if !exists {
			postings = []uint32{}
		}

		// Add docID if not already in posting list
		found := false
		for _, id := range postings {
			if id == docID {
				found = true
				break
			}
		}
		if !found {
			postings = append(postings, docID)
			idx.postings.EnsureCapacity(tokenID)
			idx.postings.SetNeighbors(tokenID, postings)

			// Update document frequency
			idx.ensureDocFreqCapacity(tokenID)
			idx.docFreqs[tokenID]++
		}
	}

	// Update metrics
	metrics.HybridBM25TokensTotal.WithLabelValues("default").Add(float64(len(tokens)))
	// Note: arena size tracking would need arena.Size() method

	return nil
}

// getOrCreateTokenID gets or creates a token ID
func (idx *BM25ArenaIndex) getOrCreateTokenID(token string) uint32 {
	if tokenID, exists := idx.tokenDict[token]; exists {
		return tokenID
	}

	tokenID := idx.nextTokenID
	idx.nextTokenID++
	idx.tokenDict[token] = tokenID
	return tokenID
}

// ensureDocFreqCapacity ensures docFreqs array is large enough
func (idx *BM25ArenaIndex) ensureDocFreqCapacity(tokenID uint32) {
	for uint32(len(idx.docFreqs)) <= tokenID {
		idx.docFreqs = append(idx.docFreqs, 0)
	}
}

// GetTokenID returns the token ID for a given token
func (idx *BM25ArenaIndex) GetTokenID(token string) (uint32, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	tokenID, exists := idx.tokenDict[token]
	return tokenID, exists
}

// GetPostingList returns the posting list for a token ID
func (idx *BM25ArenaIndex) GetPostingList(tokenID uint32) ([]uint32, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.postings.GetNeighbors(tokenID)
}

// GetTermFrequency returns the term frequency for a document and token
func (idx *BM25ArenaIndex) GetTermFrequency(docID, tokenID uint32) uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := (uint64(docID) << 32) | uint64(tokenID)
	return idx.termFreqs[key]
}

// GetDocumentFrequency returns the document frequency for a token
func (idx *BM25ArenaIndex) GetDocumentFrequency(tokenID uint32) uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if int(tokenID) >= len(idx.docFreqs) {
		return 0
	}
	return idx.docFreqs[tokenID]
}

// Score computes BM25 scores for documents given a query
func (idx *BM25ArenaIndex) Score(query []string, docIDs []uint32) map[uint32]float32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	scores := make(map[uint32]float32)

	// Convert query tokens to IDs
	queryTokenIDs := make([]uint32, 0, len(query))
	for _, token := range query {
		if tokenID, exists := idx.tokenDict[token]; exists {
			queryTokenIDs = append(queryTokenIDs, tokenID)
		}
	}

	// Compute BM25 score for each document
	for _, docID := range docIDs {
		score := float32(0.0)

		if int(docID) >= len(idx.docLengths) {
			scores[docID] = 0.0
			continue
		}

		docLen := float64(idx.docLengths[docID])

		for _, tokenID := range queryTokenIDs {
			// Get term frequency
			key := (uint64(docID) << 32) | uint64(tokenID)
			tf := float64(idx.termFreqs[key])

			if tf == 0 {
				continue
			}

			// Get document frequency
			df := float64(idx.docFreqs[tokenID])
			if df == 0 {
				continue
			}

			// Compute IDF
			idf := math.Log((float64(idx.totalDocs)-df+0.5)/(df+0.5) + 1.0)

			// Compute BM25 component
			numerator := tf * (idx.k1 + 1.0)
			denominator := tf + idx.k1*(1.0-idx.b+idx.b*(docLen/idx.avgDocLength))
			score += float32(idf * (numerator / denominator))
		}

		scores[docID] = score
	}

	return scores
}

// TokenCount returns the number of unique tokens
func (idx *BM25ArenaIndex) TokenCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return len(idx.tokenDict)
}

// DocumentCount returns the number of indexed documents
func (idx *BM25ArenaIndex) DocumentCount() uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.totalDocs
}
