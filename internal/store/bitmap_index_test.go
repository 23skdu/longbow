package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBitmapIndex_AddAndFilter(t *testing.T) {
	idx := NewBitmapIndex()

	// Add some data
	// ID 1: category=A, tag=X
	// ID 2: category=A, tag=Y
	// ID 3: category=B, tag=X
	require.NoError(t, idx.Add(1, "category", "A"))
	require.NoError(t, idx.Add(1, "tag", "X"))
	require.NoError(t, idx.Add(2, "category", "A"))
	require.NoError(t, idx.Add(2, "tag", "Y"))
	require.NoError(t, idx.Add(3, "category", "B"))
	require.NoError(t, idx.Add(3, "tag", "X"))

	t.Run("Filter Exact Match", func(t *testing.T) {
		// category=A -> {1, 2}
		bm, err := idx.Filter(map[string]string{"category": "A"})
		require.NoError(t, err)
		assert.Equal(t, uint64(2), bm.GetCardinality())
		assert.True(t, bm.Contains(1))
		assert.True(t, bm.Contains(2))
		assert.False(t, bm.Contains(3))
	})

	t.Run("Filter AND Logic", func(t *testing.T) {
		// category=A AND tag=X -> {1}
		bm, err := idx.Filter(map[string]string{
			"category": "A",
			"tag":      "X",
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(1), bm.GetCardinality())
		assert.True(t, bm.Contains(1))
	})

	t.Run("Filter No Match", func(t *testing.T) {
		// category=C -> {}
		bm, err := idx.Filter(map[string]string{"category": "C"})
		require.NoError(t, err)
		assert.Equal(t, uint64(0), bm.GetCardinality())
	})

	t.Run("Filter One Field match, one miss", func(t *testing.T) {
		// category=A AND tag=Z -> {}
		bm, err := idx.Filter(map[string]string{
			"category": "A",
			"tag":      "Z",
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(0), bm.GetCardinality())
	})
}

func TestBitmapIndex_Remove(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "cat", "A")
	idx.Add(2, "cat", "A")

	bm, _ := idx.Filter(map[string]string{"cat": "A"})
	assert.Equal(t, uint64(2), bm.GetCardinality())

	idx.Remove(1, "cat", "A")
	bm, _ = idx.Filter(map[string]string{"cat": "A"})
	assert.Equal(t, uint64(1), bm.GetCardinality())
	assert.True(t, bm.Contains(2))
	assert.False(t, bm.Contains(1))
}

func FuzzBitmapIndex(f *testing.F) {
	f.Add(uint32(1), "cat", "A")
	f.Add(uint32(2), "cat", "B")
	f.Add(uint32(3), "tag", "X")

	f.Fuzz(func(t *testing.T, id uint32, field string, value string) {
		if field == "" || value == "" {
			return
		}
		idx := NewBitmapIndex()
		err := idx.Add(id, field, value)
		if err != nil {
			return
		}

		bm, err := idx.Filter(map[string]string{field: value})
		if err != nil {
			t.Errorf("Filter failed: %v", err)
		}
		if !bm.Contains(id) {
			t.Errorf("Bitmap should contain %d for %s=%s", id, field, value)
		}
	})
}

// Benchmarking the happy path
func BenchmarkBitmapIndex_Filter(b *testing.B) {
	idx := NewBitmapIndex()
	// Setup: 10k docs, 50% cat=A, 50% cat=B. 50% tag=X, 50% tag=Y.
	for i := uint32(0); i < 10000; i++ {
		if i%2 == 0 {
			idx.Add(i, "cat", "A")
		} else {
			idx.Add(i, "cat", "B")
		}
		if i%2 == 0 {
			idx.Add(i, "tag", "X")
		} else {
			idx.Add(i, "tag", "Y")
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Filter cat=A AND tag=X (should be indices divisible by 2.. wait,
		// i%2==0 -> cat=A, tag=X. So all evens match both.
		// i%2!=0 -> cat=B, tag=Y.
		// So cardinality should be 5000)
		bm, _ := idx.Filter(map[string]string{"cat": "A", "tag": "X"})
		if bm.GetCardinality() != 5000 {
			b.Fatalf("expected 5000 results, got %d", bm.GetCardinality())
		}
	}
}
