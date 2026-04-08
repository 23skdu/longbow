package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVersionHistory_New(t *testing.T) {
	cfg := DefaultVersionHistoryConfig()
	vh := NewVersionHistory(cfg)

	assert.NotNil(t, vh)
	assert.Equal(t, 10, vh.maxVersions)
}

func TestVersionHistory_Add(t *testing.T) {
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	vector := []float32{1.0, 2.0, 3.0}
	now := time.Now().UnixNano()
	vh.Add(1, vector, now, nil)

	history := vh.GetHistory(1)
	assert.Len(t, history, 1)
	assert.Equal(t, 1, history[0].Version)
}

func TestVersionHistory_GetLatestVersion(t *testing.T) {
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	vh.Add(1, []float32{1.0}, now, nil)
	vh.Add(1, []float32{2.0}, now+1000, nil)

	latest, err := vh.GetLatestVersion(1)
	assert.NoError(t, err)
	assert.Equal(t, 2, latest.Version)
}

func TestVersionHistory_GetVersionAt(t *testing.T) {
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	vh.Add(1, []float32{1.0}, now-1000, nil)
	vh.Add(1, []float32{2.0}, now, nil)
	vh.Add(1, []float32{3.0}, now+1000, nil)

	version, err := vh.GetVersionAt(1, now)
	assert.NoError(t, err)
	assert.Equal(t, []float32{2.0}, version.Vector)
}

func TestVersionHistory_Prune(t *testing.T) {
	vh := NewVersionHistory(DefaultVersionHistoryConfig())

	now := time.Now().UnixNano()
	vh.Add(1, []float32{1.0}, now-2000, nil)
	vh.Add(1, []float32{2.0}, now-1000, nil)
	vh.Add(1, []float32{3.0}, now, nil)

	pruned := vh.Prune(context.Background(), now-500)

	assert.Equal(t, 2, pruned)
	history := vh.GetHistory(1)
	assert.Len(t, history, 1)
}

func TestVersionHistory_MaxVersions(t *testing.T) {
	cfg := VersionHistoryConfig{
		MaxVersions:     3,
		RetentionPeriod: 7 * 24 * time.Hour,
	}
	vh := NewVersionHistory(cfg)

	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		vh.Add(1, []float32{float32(i)}, now+int64(i)*1000, nil)
	}

	history := vh.GetHistory(1)
	assert.Len(t, history, 3)
	assert.Equal(t, 5, history[2].Version)
}
