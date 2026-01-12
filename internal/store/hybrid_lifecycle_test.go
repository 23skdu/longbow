package store

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHybridSearch_Lifecycle tests full lifecycle: create, index, search, close
func TestHybridSearch_Lifecycle(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	bm25 := NewBM25ArenaIndex(arena, 100)

	// 1. Create and index documents
	docs := []struct {
		id     uint32
		tokens []string
	}{
		{1, []string{"machine", "learning", "algorithms"}},
		{2, []string{"deep", "learning", "neural", "networks"}},
		{3, []string{"machine", "learning", "models"}},
	}

	for _, doc := range docs {
		err := bm25.IndexDocument(doc.id, doc.tokens)
		require.NoError(t, err)
	}

	// 2. Verify indexing
	assert.Equal(t, uint32(3), bm25.DocumentCount())
	assert.Greater(t, bm25.TokenCount(), 0)

	// 3. Search
	query := []string{"machine", "learning"}
	scores := bm25.Score(query, []uint32{1, 2, 3})

	assert.Greater(t, scores[1], float32(0.0))
	assert.Greater(t, scores[3], float32(0.0))

	// 4. Close (no explicit close needed for arena-based, but verify no panics)
	// Arena cleanup happens when arena is garbage collected
}

// TestHybridSearch_ArenaGrowth verifies arena grows correctly
func TestHybridSearch_ArenaGrowth(t *testing.T) {
	arena := memory.NewSlabArena(1024) // Small arena to force growth
	bm25 := NewBM25ArenaIndex(arena, 10)

	// Index many documents to force arena growth
	for i := 0; i < 1000; i++ {
		tokens := []string{"token", "document", "test", "data", "growth"}
		err := bm25.IndexDocument(uint32(i), tokens)
		require.NoError(t, err)
	}

	// Verify all documents indexed
	assert.Equal(t, uint32(1000), bm25.DocumentCount())

	// Verify search still works after growth
	query := []string{"token", "test"}
	scores := bm25.Score(query, []uint32{0, 500, 999})
	assert.Greater(t, scores[0], float32(0.0))
	assert.Greater(t, scores[500], float32(0.0))
	assert.Greater(t, scores[999], float32(0.0))
}

// TestHybridSearch_ConcurrentLifecycle tests concurrent create/destroy
func TestHybridSearch_ConcurrentLifecycle(t *testing.T) {
	done := make(chan bool)

	// Create multiple BM25 indexes concurrently
	for i := 0; i < 5; i++ {
		go func() {
			arena := memory.NewSlabArena(1024 * 1024)
			bm25 := NewBM25ArenaIndex(arena, 100)

			// Index documents
			for j := 0; j < 100; j++ {
				tokens := []string{"concurrent", "test", "document"}
				_ = bm25.IndexDocument(uint32(j), tokens)
			}

			// Search
			query := []string{"concurrent", "test"}
			scores := bm25.Score(query, []uint32{0, 50, 99})
			assert.Greater(t, scores[0], float32(0.0))

			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 5; i++ {
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent lifecycle test")
		}
	}
}

// TestHybridSearch_UpdateDocument tests updating an existing document
func TestHybridSearch_UpdateDocument(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	bm25 := NewBM25ArenaIndex(arena, 100)

	// Index initial document
	_ = bm25.IndexDocument(1, []string{"initial", "content"})

	// Update document
	_ = bm25.IndexDocument(1, []string{"updated", "content", "new"})

	// Verify updated content is searchable
	query := []string{"updated", "new"}
	scores := bm25.Score(query, []uint32{1})
	assert.Greater(t, scores[1], float32(0.0))

	// Verify old content still contributes (cumulative indexing)
	query2 := []string{"initial"}
	scores2 := bm25.Score(query2, []uint32{1})
	assert.Greater(t, scores2[1], float32(0.0))
}

// TestHybridSearch_EmptyQuery tests handling of empty queries
func TestHybridSearch_EmptyQuery(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	bm25 := NewBM25ArenaIndex(arena, 100)

	bm25.IndexDocument(1, []string{"test", "document"})

	// Empty query should return zero scores
	scores := bm25.Score([]string{}, []uint32{1})
	assert.Equal(t, float32(0.0), scores[1])
}

// TestHybridSearch_NonExistentTokens tests queries with tokens not in index
func TestHybridSearch_NonExistentTokens(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	bm25 := NewBM25ArenaIndex(arena, 100)

	bm25.IndexDocument(1, []string{"test", "document"})

	// Query with non-existent tokens
	query := []string{"nonexistent", "tokens"}
	scores := bm25.Score(query, []uint32{1})
	assert.Equal(t, float32(0.0), scores[1])
}
