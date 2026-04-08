package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type VersionedVector struct {
	ID        uint64
	Vector    []float32
	Timestamp int64
	Metadata  map[string]interface{}
	Version   int
}

type VersionHistory struct {
	mu          sync.RWMutex
	maxVersions int
	retention   time.Duration
	history     map[uint64][]VersionedVector
}

type VersionHistoryConfig struct {
	MaxVersions     int
	RetentionPeriod time.Duration
}

func DefaultVersionHistoryConfig() VersionHistoryConfig {
	return VersionHistoryConfig{
		MaxVersions:     10,
		RetentionPeriod: 7 * 24 * time.Hour,
	}
}

func NewVersionHistory(cfg VersionHistoryConfig) *VersionHistory {
	return &VersionHistory{
		maxVersions: cfg.MaxVersions,
		retention:   cfg.RetentionPeriod,
		history:     make(map[uint64][]VersionedVector),
	}
}

func (vh *VersionHistory) Add(id uint64, vector []float32, timestamp int64, metadata map[string]interface{}) {
	vh.mu.Lock()
	defer vh.mu.Unlock()

	existing := vh.history[id]
	newVersion := 1
	if len(existing) > 0 {
		newVersion = existing[len(existing)-1].Version + 1
	}

	versioned := VersionedVector{
		ID:        id,
		Vector:    vector,
		Timestamp: timestamp,
		Metadata:  metadata,
		Version:   newVersion,
	}

	vh.history[id] = append(vh.history[id], versioned)

	if len(vh.history[id]) > vh.maxVersions {
		vh.history[id] = vh.history[id][len(vh.history[id])-vh.maxVersions:]
	}
}

func (vh *VersionHistory) GetVersion(id uint64, version int) (*VersionedVector, error) {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	versions, ok := vh.history[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].Version == version {
			return &versions[i], nil
		}
	}

	return nil, fmt.Errorf("version %d not found for vector %d", version, id)
}

func (vh *VersionHistory) GetVersionAt(id uint64, timestamp int64) (*VersionedVector, error) {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	versions, ok := vh.history[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].Timestamp <= timestamp {
			return &versions[i], nil
		}
	}

	return nil, fmt.Errorf("no version found at or before timestamp %d for vector %d", timestamp, id)
}

func (vh *VersionHistory) GetHistory(id uint64) []VersionedVector {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	versions, ok := vh.history[id]
	if !ok {
		return nil
	}

	result := make([]VersionedVector, len(versions))
	copy(result, versions)
	return result
}

func (vh *VersionHistory) GetLatestVersion(id uint64) (*VersionedVector, error) {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	versions, ok := vh.history[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	return &versions[len(versions)-1], nil
}

func (vh *VersionHistory) Prune(ctx context.Context, beforeTimestamp int64) int {
	vh.mu.Lock()
	defer vh.mu.Unlock()

	pruned := 0
	for id, versions := range vh.history {
		filtered := make([]VersionedVector, 0)
		for _, v := range versions {
			if v.Timestamp > beforeTimestamp {
				filtered = append(filtered, v)
			} else {
				pruned++
			}
		}
		if len(filtered) == 0 {
			delete(vh.history, id)
		} else {
			vh.history[id] = filtered
		}
	}

	return pruned
}

func (vh *VersionHistory) Size() int {
	vh.mu.RLock()
	defer vh.mu.RUnlock()
	return len(vh.history)
}

func (vh *VersionHistory) TotalVersions() int {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	total := 0
	for _, versions := range vh.history {
		total += len(versions)
	}
	return total
}
