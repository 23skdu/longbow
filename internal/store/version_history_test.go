package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionHistory_New(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := DefaultVersionHistoryConfig()
	vh := NewVersionHistory(cfg)

	assert.NotNil(t, vh)
	assert.Equal(t, 10, vh.maxVersions)
}

func TestVersionHistory_Add(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	vector := []float32{1.0, 2.0, 3.0}
	now := time.Now().UnixNano()
	vh.Add(1, vector, 0.0, now, nil)

	history := vh.GetHistory(1)
	assert.Len(t, history, 1)
	assert.Equal(t, 1, history[0].Version)
}

func TestVersionHistory_GetLatestVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	vh.Add(1, []float32{1.0}, 0.0, now, nil)
	vh.Add(1, []float32{2.0}, 0.0, now+1000, nil)

	latest, err := vh.GetLatestVersion(1)
	assert.NoError(t, err)
	assert.Equal(t, 2, latest.Version)
}

func TestVersionHistory_GetVersionAt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	vh.Add(1, []float32{1.0}, 0.0, now-1000, nil)
	vh.Add(1, []float32{2.0}, 0.0, now, nil)
	vh.Add(1, []float32{3.0}, 0.0, now+1000, nil)

	version, err := vh.GetVersionAt(1, now)
	assert.NoError(t, err)
	assert.Equal(t, []float32{2.0}, version.Vector)
}

func TestVersionHistory_Prune(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	vh.Add(1, []float32{1.0}, 0.0, now-2000, nil)
	vh.Add(1, []float32{2.0}, 0.0, now-1000, nil)
	vh.Add(1, []float32{3.0}, 0.0, now, nil)

	pruned := vh.Prune(context.Background(), now-500)

	assert.Equal(t, 2, pruned)
	history := vh.GetHistory(1)
	assert.Len(t, history, 1)
}

func TestVersionHistory_MaxVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := VersionHistoryConfig{
		MaxVersions:     3,
		RetentionPeriod: 7 * 24 * time.Hour,
	}
	vh := NewVersionHistory(cfg)

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		vh.Add(1, []float32{float32(i)}, 0.0, now+int64(i)*1000, nil)
	}

	history := vh.GetHistory(1)
	assert.Len(t, history, 3)
	assert.Equal(t, 5, history[2].Version)
}

func TestVersionHistory_GetVersionsAtBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	// id=1: two versions; id=2: one version; id=3: only future versions.
	vh.Add(1, []float32{1.0}, 0.0, now-2000, nil)
	vh.Add(1, []float32{2.0}, 0.0, now-1000, nil)
	vh.Add(2, []float32{9.0}, 0.0, now-500, nil)
	vh.Add(3, []float32{7.0}, 0.0, now+5000, nil)

	ids := []uint64{1, 2, 3, 99}
	out := make(map[uint64]VersionedVector, len(ids))
	vh.GetVersionsAtBatch(ids, now, out)

	require.Len(t, out, 2)
	assert.Equal(t, []float32{2.0}, out[1].Vector)
	assert.Equal(t, []float32{9.0}, out[2].Vector)
	_, has3 := out[3]
	assert.False(t, has3, "id 3 has no version at or before timestamp")
	_, has99 := out[99]
	assert.False(t, has99)
}

func BenchmarkVersionHistory_GetVersionsAtBatch(b *testing.B) {
	vh := NewVersionHistory(DefaultVersionHistoryConfig())
	now := time.Now().UnixNano()

	const nIDs = 1000
	ids := make([]uint64, nIDs)
	for id := uint64(0); id < nIDs; id++ {
		ids[id] = id
		for v := 0; v < 5; v++ {
			vh.Add(id, []float32{float32(v)}, 0.0, now+int64(v)*1000, nil)
		}
	}

	out := make(map[uint64]VersionedVector, nIDs)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		clear(out)
		vh.GetVersionsAtBatch(ids, now, out)
	}
}
