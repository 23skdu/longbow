package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBM25Arena_TokenStorage verifies token storage in SlabArena
func TestBM25Arena_TokenStorage(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	idx := NewBM25ArenaIndex(arena, 100)

	// Index a document with tokens
	tokens := []string{"hello", "world", "test"}
	err := idx.IndexDocument(1, tokens)
	require.NoError(t, err)

	// Verify tokens are stored
	assert.Equal(t, 3, idx.TokenCount())

	// Verify token IDs are assigned
	tokenID, exists := idx.GetTokenID("hello")
	assert.True(t, exists)
	assert.Greater(t, tokenID, uint32(0))
}

// TestBM25Arena_PostingLists verifies inverted index uses PackedAdjacency
func TestBM25Arena_PostingLists(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	idx := NewBM25ArenaIndex(arena, 100)

	// Index multiple documents with overlapping tokens
	_ = idx.IndexDocument(1, []string{"hello", "world"})
	_ = idx.IndexDocument(2, []string{"hello", "test"})
	_ = idx.IndexDocument(3, []string{"world", "test"})

	// Verify posting lists
	tokenID, _ := idx.GetTokenID("hello")
	postings, ok := idx.GetPostingList(tokenID)
	require.True(t, ok)
	assert.ElementsMatch(t, []uint32{1, 2}, postings)

	tokenID, _ = idx.GetTokenID("world")
	postings, ok = idx.GetPostingList(tokenID)
	require.True(t, ok)
	assert.ElementsMatch(t, []uint32{1, 3}, postings)
}

// TestBM25Arena_TermFrequency verifies TF calculation with arena data
func TestBM25Arena_TermFrequency(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	idx := NewBM25ArenaIndex(arena, 100)

	// Index document with repeated tokens
	tokens := []string{"hello", "hello", "world", "hello"}
	_ = idx.IndexDocument(1, tokens)

	// Verify term frequencies
	helloID, _ := idx.GetTokenID("hello")
	tf := idx.GetTermFrequency(1, helloID)
	assert.Equal(t, uint32(3), tf)

	worldID, _ := idx.GetTokenID("world")
	tf = idx.GetTermFrequency(1, worldID)
	assert.Equal(t, uint32(1), tf)
}

// TestBM25Arena_DocumentFrequency verifies DF calculation
func TestBM25Arena_DocumentFrequency(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	idx := NewBM25ArenaIndex(arena, 100)

	// Index multiple documents
	_ = idx.IndexDocument(1, []string{"hello", "world"})
	_ = idx.IndexDocument(2, []string{"hello", "test"})
	_ = idx.IndexDocument(3, []string{"test", "data"})

	// Verify document frequencies
	helloID, _ := idx.GetTokenID("hello")
	df := idx.GetDocumentFrequency(helloID)
	assert.Equal(t, uint32(2), df) // appears in docs 1 and 2

	testID, _ := idx.GetTokenID("test")
	df = idx.GetDocumentFrequency(testID)
	assert.Equal(t, uint32(2), df) // appears in docs 2 and 3
}

// TestBM25Arena_Scoring verifies BM25 score computation
func TestBM25Arena_Scoring(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	idx := NewBM25ArenaIndex(arena, 100)

	// Index documents
	_ = idx.IndexDocument(1, []string{"machine", "learning", "algorithms"})
	_ = idx.IndexDocument(2, []string{"deep", "learning", "neural", "networks"})
	_ = idx.IndexDocument(3, []string{"machine", "learning", "models"})

	// Search for "machine learning"
	query := []string{"machine", "learning"}
	scores := idx.Score(query, []uint32{1, 2, 3})

	// Verify scores
	assert.Greater(t, scores[1], float32(0.0)) // has both "machine" and "learning"
	assert.Greater(t, scores[2], float32(0.0)) // has "learning" only
	assert.Greater(t, scores[3], float32(0.0)) // has both "machine" and "learning"

	// Doc 1 and 3 should have higher scores than doc 2 (they have both query terms)
	assert.Greater(t, scores[1], scores[2])
	assert.Greater(t, scores[3], scores[2])
}

// TestBM25Arena_ConcurrentAccess verifies thread safety
func TestBM25Arena_ConcurrentAccess(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	idx := NewBM25ArenaIndex(arena, 1000)

	// Index documents concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(docID uint32) {
			tokens := []string{"concurrent", "test", "document"}
			_ = idx.IndexDocument(docID, tokens)
			done <- true
		}(uint32(i))
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all documents indexed
	tokenID, _ := idx.GetTokenID("concurrent")
	postings, ok := idx.GetPostingList(tokenID)
	require.True(t, ok)
	assert.Len(t, postings, 10)
}

// TestBM25Arena_Memory verifies memory efficiency
func TestBM25Arena_Memory(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	idx := NewBM25ArenaIndex(arena, 100)

	// Index many documents
	for i := 0; i < 1000; i++ {
		tokens := []string{"token", "document", "test", "data"}
		_ = idx.IndexDocument(uint32(i), tokens)
	}

	// Verify arena usage is reasonable
	// (This is a smoke test - actual memory usage depends on implementation)
	assert.Greater(t, idx.TokenCount(), 0)
	assert.Greater(t, idx.DocumentCount(), uint32(0))
}
