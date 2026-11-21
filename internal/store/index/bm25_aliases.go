package index

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/index/bm25"
)

type BM25ArenaDocument = bm25.BM25ArenaDocument
type BM25ArenaIndex = bm25.BM25ArenaIndex
type BM25Config = bm25.BM25Config
type BM25Document = bm25.BM25Document
type BM25InvertedIndex = bm25.BM25InvertedIndex
type BM25PostingList = bm25.BM25PostingList
type BM25Scorer = bm25.BM25Scorer
type BitmapIndex = bm25.BitmapIndex
type BloomFilter = bm25.BloomFilter
type ColumnInvertedIndex = bm25.ColumnInvertedIndex
type ColumnInvertedIndexStats = bm25.ColumnInvertedIndexStats
type InvertedIndex = bm25.InvertedIndex
type PostingsStore = bm25.PostingsStore
type RowPosition = bm25.RowPosition
type ShardedInvertedIndex = bm25.ShardedInvertedIndex
type StringContainsIndex = bm25.StringContainsIndex
type TrigramIndex = bm25.TrigramIndex

func DefaultBM25Config() BM25Config {
	return bm25.DefaultBM25Config()
}

func NewBM25ArenaIndex(arena *memory.SlabArena, initialCapacity int) *BM25ArenaIndex {
	return bm25.NewBM25ArenaIndexWithStore(arena, initialCapacity, NewPackedAdjacency(arena, initialCapacity))
}

func NewBM25ArenaIndexWithStore(arena *memory.SlabArena, initialCapacity int, store PostingsStore) *BM25ArenaIndex {
	return bm25.NewBM25ArenaIndexWithStore(arena, initialCapacity, store)
}

func NewBM25InvertedIndex(config BM25Config) *BM25InvertedIndex {
	return bm25.NewBM25InvertedIndex(config)
}

func NewBM25Scorer(config BM25Config) *BM25Scorer {
	return bm25.NewBM25Scorer(config)
}

func NewBitmapIndex() *BitmapIndex {
	return bm25.NewBitmapIndex()
}

func NewBloomFilter(n int, p float64) *BloomFilter {
	return bm25.NewBloomFilter(n, p)
}

func NewColumnInvertedIndex() *ColumnInvertedIndex {
	return bm25.NewColumnInvertedIndex()
}

func NewInvertedIndex() *InvertedIndex {
	return bm25.NewInvertedIndex()
}

func NewShardedInvertedIndex() *ShardedInvertedIndex {
	return bm25.NewShardedInvertedIndex()
}

func NewStringContainsIndex() *StringContainsIndex {
	return bm25.NewStringContainsIndex()
}

func NewTrigramIndex() *TrigramIndex {
	return bm25.NewTrigramIndex()
}

func Tokenize(text string) []string {
	return bm25.Tokenize(text)
}
