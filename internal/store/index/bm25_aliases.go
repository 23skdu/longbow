package index

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/index/bm25"
)

// BM25ArenaDocument represents a bm25 arena document value.
type BM25ArenaDocument = bm25.BM25ArenaDocument

// BM25ArenaIndex represents a bm25 arena index value.
type BM25ArenaIndex = bm25.BM25ArenaIndex

// BM25Config represents a bm25 config value.
type BM25Config = bm25.BM25Config

// BM25Document represents a bm25 document value.
type BM25Document = bm25.BM25Document

// BM25InvertedIndex represents a bm25 inverted index value.
type BM25InvertedIndex = bm25.BM25InvertedIndex

// BM25PostingList represents a bm25 posting list value.
type BM25PostingList = bm25.BM25PostingList

// BM25Scorer represents a bm25 scorer value.
type BM25Scorer = bm25.BM25Scorer

// BitmapIndex represents a bitmap index value.
type BitmapIndex = bm25.BitmapIndex

// BloomFilter represents a bloom filter value.
type BloomFilter = bm25.BloomFilter

// ColumnInvertedIndex represents a column inverted index value.
type ColumnInvertedIndex = bm25.ColumnInvertedIndex

// ColumnInvertedIndexStats represents a column inverted index stats value.
type ColumnInvertedIndexStats = bm25.ColumnInvertedIndexStats

// InvertedIndex represents a inverted index value.
type InvertedIndex = bm25.InvertedIndex

// PostingsStore represents a postings store value.
type PostingsStore = bm25.PostingsStore

// RowPosition represents a row position value.
type RowPosition = bm25.RowPosition

// ShardedInvertedIndex represents a sharded inverted index value.
type ShardedInvertedIndex = bm25.ShardedInvertedIndex

// StringContainsIndex represents a string contains index value.
type StringContainsIndex = bm25.StringContainsIndex

// TrigramIndex represents a trigram index value.
type TrigramIndex = bm25.TrigramIndex

// DefaultBM25Config returns the default BM25Config.
func DefaultBM25Config() BM25Config {
	return bm25.DefaultBM25Config()
}

// NewBM25ArenaIndex constructs a new BM25ArenaIndex.
func NewBM25ArenaIndex(arena *memory.SlabArena, initialCapacity int) *BM25ArenaIndex {
	return bm25.NewBM25ArenaIndexWithStore(arena, initialCapacity, NewPackedAdjacency(arena, initialCapacity))
}

// NewBM25ArenaIndexWithStore constructs a new BM25ArenaIndexWithStore.
func NewBM25ArenaIndexWithStore(arena *memory.SlabArena, initialCapacity int, store PostingsStore) *BM25ArenaIndex {
	return bm25.NewBM25ArenaIndexWithStore(arena, initialCapacity, store)
}

// NewBM25InvertedIndex constructs a new BM25InvertedIndex.
func NewBM25InvertedIndex(config BM25Config) *BM25InvertedIndex {
	return bm25.NewBM25InvertedIndex(config)
}

// NewBM25Scorer constructs a new BM25Scorer.
func NewBM25Scorer(config BM25Config) *BM25Scorer {
	return bm25.NewBM25Scorer(config)
}

// NewBitmapIndex constructs a new BitmapIndex.
func NewBitmapIndex() *BitmapIndex {
	return bm25.NewBitmapIndex()
}

// NewBloomFilter constructs a new BloomFilter.
func NewBloomFilter(n int, p float64) *BloomFilter {
	return bm25.NewBloomFilter(n, p)
}

// NewColumnInvertedIndex constructs a new ColumnInvertedIndex.
func NewColumnInvertedIndex() *ColumnInvertedIndex {
	return bm25.NewColumnInvertedIndex()
}

// NewInvertedIndex constructs a new InvertedIndex.
func NewInvertedIndex() *InvertedIndex {
	return bm25.NewInvertedIndex()
}

// NewShardedInvertedIndex constructs a new ShardedInvertedIndex.
func NewShardedInvertedIndex() *ShardedInvertedIndex {
	return bm25.NewShardedInvertedIndex()
}

// NewStringContainsIndex constructs a new StringContainsIndex.
func NewStringContainsIndex() *StringContainsIndex {
	return bm25.NewStringContainsIndex()
}

// NewTrigramIndex constructs a new TrigramIndex.
func NewTrigramIndex() *TrigramIndex {
	return bm25.NewTrigramIndex()
}

// Tokenize runs the tokenize operation.
func Tokenize(text string) []string {
	return bm25.Tokenize(text)
}
