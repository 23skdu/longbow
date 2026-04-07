package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTemporalTree_Insert(t *testing.T) {
	tt := NewTemporalTree()

	tt.Insert(time.Now().UnixNano(), 1)
	tt.Insert(time.Now().UnixNano(), 2)

	assert.Equal(t, 2, tt.Len())
}

func TestTemporalTree_GetRange(t *testing.T) {
	tt := NewTemporalTree()

	now := time.Now().UnixNano()
	tt.Insert(now-1000, 1)
	tt.Insert(now, 2)
	tt.Insert(now+1000, 3)

	results := tt.GetRange(now-500, now+500)

	assert.GreaterOrEqual(t, len(results), 1)
}

func TestTemporalTree_GetRangeReversed(t *testing.T) {
	tt := NewTemporalTree()

	now := time.Now().UnixNano()
	tt.Insert(now-1000, 1)
	tt.Insert(now, 2)
	tt.Insert(now+1000, 3)

	results := tt.GetRangeReversed(now-500, now+500)

	assert.GreaterOrEqual(t, len(results), 1)
}

func TestTemporalTree_GetBefore(t *testing.T) {
	tt := NewTemporalTree()

	now := time.Now().UnixNano()
	tt.Insert(now-1000, 1)
	tt.Insert(now, 2)
	tt.Insert(now+1000, 3)

	results := tt.GetBefore(now)

	assert.Equal(t, 2, len(results))
}

func TestTemporalTree_GetLatest(t *testing.T) {
	tt := NewTemporalTree()

	now := time.Now().UnixNano()
	tt.Insert(now-1000, 1)
	tt.Insert(now, 2)
	tt.Insert(now+1000, 3)

	results := tt.GetLatest(2)

	assert.Equal(t, 2, len(results))
}

func TestTemporalIndex_New(t *testing.T) {
	ti := NewTemporalIndex(128)

	assert.NotNil(t, ti)
	assert.Equal(t, 128, ti.dimension)
	assert.NotNil(t, ti.temporalTree)
}

func TestTemporalIndex_Add(t *testing.T) {
	ti := NewTemporalIndex(128)

	err := ti.Add(1, make([]float32, 128), time.Now().UnixNano(), nil)
	assert.NoError(t, err)
}

func TestTemporalIndex_AddDimensionMismatch(t *testing.T) {
	ti := NewTemporalIndex(128)

	err := ti.Add(1, make([]float32, 64), time.Now().UnixNano(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dimension mismatch")
}

func TestTemporalIndex_Delete(t *testing.T) {
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	ti.Add(1, make([]float32, 128), now, nil)

	err := ti.Delete(1)
	assert.NoError(t, err)
}

func TestTemporalIndex_Update(t *testing.T) {
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	ti.Add(1, make([]float32, 128), now, nil)

	newVec := make([]float32, 128)
	for i := range newVec {
		newVec[i] = 1.0
	}
	err := ti.Update(1, newVec, now+1000, nil)
	assert.NoError(t, err)
}

func TestTemporalIndex_SearchAsOf(t *testing.T) {
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	ti.Add(1, make([]float32, 128), now-1000, nil)
	ti.Add(2, make([]float32, 128), now, nil)

	results, err := ti.SearchAsOf(context.Background(), now, 10)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)
}

func TestTemporalIndex_SearchRange(t *testing.T) {
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	ti.Add(1, make([]float32, 128), now-1000, nil)
	ti.Add(2, make([]float32, 128), now, nil)
	ti.Add(3, make([]float32, 128), now+1000, nil)

	results, err := ti.SearchRange(context.Background(), now-500, now+500, 10)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)
}

func TestTemporalIndex_SearchSlidingWindow(t *testing.T) {
	ti := NewTemporalIndex(128)

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		ti.Add(uint64(i), make([]float32, 128), now+int64(i)*1000, nil)
	}

	results, err := ti.SearchSlidingWindow(context.Background(), 3, 10)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 1)
}

func TestTemporalVector_Structure(t *testing.T) {
	vec := &TemporalVector{
		ID:        1,
		Vector:    []float32{1.0, 2.0, 3.0},
		Timestamp: time.Now().UnixNano(),
		Metadata:  map[string]interface{}{"key": "value"},
		Tombstone: false,
	}

	assert.Equal(t, uint64(1), vec.ID)
	assert.Equal(t, 3, len(vec.Vector))
	assert.False(t, vec.Tombstone)
}

func TestTemporalIndex_DeleteNotFound(t *testing.T) {
	ti := NewTemporalIndex(128)

	err := ti.Delete(999)
	assert.Error(t, err)
}

func TestTemporalIndex_UpdateNotFound(t *testing.T) {
	ti := NewTemporalIndex(128)

	err := ti.Update(999, make([]float32, 128), time.Now().UnixNano(), nil)
	assert.Error(t, err)
}
