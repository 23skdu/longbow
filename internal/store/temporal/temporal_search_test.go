package temporal

import (
	"context"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/assert"
)

// TestTemporalTree_Insert runs the test temporal tree_insert operation.
func TestTemporalTree_Insert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	arena := memory.NewSlabArena(1024 * 1024)
	tt := NewTemporalTree(arena)

	now := time.Now().UnixNano()
	tt.Insert(now, 1, 0.0)
	tt.Insert(now+1, 2, 0.0)

	assert.Equal(t, 2, tt.Len())
}

// TestTemporalTree_GetRange runs the test temporal tree_get range operation.
func TestTemporalTree_GetRange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	arena := memory.NewSlabArena(1024 * 1024)
	tt := NewTemporalTree(arena)

	now := time.Now().UnixNano()
	tt.Insert(now-1000, 1, 0.0)
	tt.Insert(now, 2, 0.0)
	tt.Insert(now+1000, 3, 0.0)

	results := tt.GetRange(now-500, now+500)

	assert.GreaterOrEqual(t, len(results), 1)
}

// TestTemporalTree_GetRangeReversed runs the test temporal tree_get range reversed operation.
func TestTemporalTree_GetRangeReversed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	arena := memory.NewSlabArena(1024 * 1024)
	tt := NewTemporalTree(arena)

	now := time.Now().UnixNano()
	tt.Insert(now-1000, 1, 0.0)
	tt.Insert(now, 2, 0.0)
	tt.Insert(now+1000, 3, 0.0)

	results := tt.GetRangeReversed(now-500, now+500)

	assert.GreaterOrEqual(t, len(results), 1)
}

// TestTemporalTree_GetBefore runs the test temporal tree_get before operation.
func TestTemporalTree_GetBefore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	arena := memory.NewSlabArena(1024 * 1024)
	tt := NewTemporalTree(arena)

	now := time.Now().UnixNano()
	tt.Insert(now-1000, 1, 0.0)
	tt.Insert(now, 2, 0.0)
	tt.Insert(now+1000, 3, 0.0)

	results := tt.GetBefore(now)

	assert.Equal(t, 1, len(results))
}

// TestTemporalTree_GetLatest runs the test temporal tree_get latest operation.
func TestTemporalTree_GetLatest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	arena := memory.NewSlabArena(1024 * 1024)
	tt := NewTemporalTree(arena)

	now := time.Now().UnixNano()
	tt.Insert(now-1000, 1, 0.0)
	tt.Insert(now, 2, 0.0)
	tt.Insert(now+1000, 3, 0.0)

	results := tt.GetLatest(2)

	assert.Equal(t, 2, len(results))
}

// TestTemporalIndex_New runs the test temporal index_new operation.
func TestTemporalIndex_New(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	assert.NotNil(t, ti)
	assert.Equal(t, 128, ti.dimension)
	assert.NotNil(t, ti.temporalTree.Load())
}

// TestTemporalIndex_Add runs the test temporal index_add operation.
func TestTemporalIndex_Add(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	err := ti.Add(1, make([]float32, 128), time.Now().UnixNano(), nil)
	assert.NoError(t, err)
}

// TestTemporalIndex_AddDimensionMismatch runs the test temporal index_add dimension mismatch operation.
func TestTemporalIndex_AddDimensionMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	err := ti.Add(1, make([]float32, 64), time.Now().UnixNano(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dimension mismatch")
}

// TestTemporalIndex_Delete runs the test temporal index_delete operation.
func TestTemporalIndex_Delete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	_ = ti.Add(1, make([]float32, 128), now, nil)

	err := ti.Delete(1)
	assert.NoError(t, err)
}

// TestTemporalIndex_Update runs the test temporal index_update operation.
func TestTemporalIndex_Update(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	_ = ti.Add(1, make([]float32, 128), now, nil)

	newVec := make([]float32, 128)
	for i := range newVec {
		newVec[i] = 1.0
	}
	err := ti.Update(1, newVec, now+1000, nil)
	assert.NoError(t, err)
}

// TestTemporalIndex_SearchAsOf runs the test temporal index_search as of operation.
func TestTemporalIndex_SearchAsOf(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	_ = ti.Add(1, make([]float32, 128), now-1000, nil)
	_ = ti.Add(2, make([]float32, 128), now, nil)

	results, err := ti.SearchAsOf(context.Background(), now, 10)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)
}

// TestTemporalIndex_SearchRange runs the test temporal index_search range operation.
func TestTemporalIndex_SearchRange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	_ = ti.Add(1, make([]float32, 128), now-1000, nil)
	_ = ti.Add(2, make([]float32, 128), now, nil)
	_ = ti.Add(3, make([]float32, 128), now+1000, nil)

	results, err := ti.SearchRange(context.Background(), now-500, now+500, 10)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)
}

// TestTemporalIndex_SearchSlidingWindow runs the test temporal index_search sliding window operation.
func TestTemporalIndex_SearchSlidingWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		_ = ti.Add(uint64(i), make([]float32, 128), now+int64(i)*1000, nil)
	}

	results, err := ti.SearchSlidingWindow(context.Background(), 3, 10)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)
}

// TestTemporalVector_Structure runs the test temporal vector_structure operation.
func TestTemporalVector_Structure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	m, _ := core.EncodeMetadata(map[string]interface{}{"key": "value"})
	vec := &TemporalVector{
		ID:        1,
		Vector:    []float32{1.0, 2.0, 3.0},
		Timestamp: time.Now().UnixNano(),
		Metadata:  m,
		Tombstone: false,
	}

	assert.Equal(t, uint64(1), vec.ID)
	assert.Equal(t, 3, len(vec.Vector))
	assert.False(t, vec.Tombstone)
}

// TestTemporalIndex_DeleteNotFound runs the test temporal index_delete not found operation.
func TestTemporalIndex_DeleteNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	err := ti.Delete(999)
	assert.Error(t, err)
}

// TestTemporalIndex_UpdateNotFound runs the test temporal index_update not found operation.
func TestTemporalIndex_UpdateNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(128)

	err := ti.Update(999, make([]float32, 128), time.Now().UnixNano(), nil)
	assert.Error(t, err)
}

// FuzzTemporalTree exercises insertion and range queries on TemporalTree.
func FuzzTemporalTree(f *testing.F) {
	f.Add(int64(100), uint64(1), int64(50), int64(200))
	f.Add(int64(0), uint64(0), int64(-10), int64(10))
	f.Fuzz(func(t *testing.T, ts int64, id uint64, start, end int64) {
		arena := memory.NewSlabArena(1024 * 1024)
		tt := NewTemporalTree(arena)
		defer tt.Release()
		tt.Insert(ts, id, 1.0)
		_ = tt.GetBefore(end)
		_ = tt.GetAfter(start)
		if start <= end {
			_ = tt.getRangeChunked(start, end)
		}
	})
}
