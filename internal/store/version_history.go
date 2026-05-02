package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// VersionedVector represents a specific version of a vector with its metadata and timestamp.
type VersionedVector struct {
	ID        uint64
	Vector    []float32
	Timestamp int64
	Metadata  []byte
	Version   int
}

// VersionHistory manages the storage and retrieval of multiple versions for each vector ID.
type VersionHistory struct {
	mu          sync.RWMutex
	maxVersions int
	retention   time.Duration
	history     map[uint64][]VersionedVector
}

// VersionHistoryConfig defines the retention settings for version history.
type VersionHistoryConfig struct {
	// MaxVersions is the maximum number of versions to keep per vector ID.
	MaxVersions     int
	// RetentionPeriod is the duration for which older versions are kept.
	RetentionPeriod time.Duration
}

// DefaultVersionHistoryConfig returns a default configuration for version history.
func DefaultVersionHistoryConfig() VersionHistoryConfig {
	return VersionHistoryConfig{
		MaxVersions:     10,
		RetentionPeriod: 7 * 24 * time.Hour,
	}
}

// NewVersionHistory creates a new VersionHistory instance with the provided configuration.
func NewVersionHistory(cfg VersionHistoryConfig) *VersionHistory {
	return &VersionHistory{
		maxVersions: cfg.MaxVersions,
		retention:   cfg.RetentionPeriod,
		history:     make(map[uint64][]VersionedVector),
	}
}

// Add inserts a new version for a vector ID, pruning old versions if necessary.
func (vh *VersionHistory) Add(id uint64, vector []float32, timestamp int64, metadata []byte) {
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

// GetVersion retrieves a specific version of a vector by its ID and version number.
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

// GetVersionAt retrieves the version of a vector that was active at a specific timestamp.
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

// GetHistory returns the entire version history for a specific vector ID.
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

// GetLatestVersion retrieves the most recent version for a vector ID.
func (vh *VersionHistory) GetLatestVersion(id uint64) (*VersionedVector, error) {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	versions, ok := vh.history[id]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	return &versions[len(versions)-1], nil
}

// Prune removes all versions with timestamps before the specified value.
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

// Size returns the number of unique vector IDs tracked in the version history.
func (vh *VersionHistory) Size() int {
	vh.mu.RLock()
	defer vh.mu.RUnlock()
	return len(vh.history)
}

// TotalVersions returns the total number of versions across all vector IDs.
func (vh *VersionHistory) TotalVersions() int {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	total := 0
	for _, versions := range vh.history {
		total += len(versions)
	}
	return total
}
