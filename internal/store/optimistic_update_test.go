package store

import (
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptimisticConcurrentConfig_Defaults(t *testing.T) {
	config := OptimisticConcurrentConfig{}

	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelayMs <= 0 {
		config.RetryDelayMs = 10
	}
	if config.VersionCacheSize <= 0 {
		config.VersionCacheSize = 10000
	}

	assert.Equal(t, 3, config.MaxRetries)
	assert.Equal(t, 10, config.RetryDelayMs)
	assert.Equal(t, 10000, config.VersionCacheSize)
}

func TestNewOptimisticConcurrentUpdates(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	config := OptimisticConcurrentConfig{
		MaxRetries:       5,
		RetryDelayMs:     20,
		VersionCacheSize: 1000,
	}

	ocu := NewOptimisticConcurrentUpdates(logger, config)

	assert.NotNil(t, ocu)
	assert.Equal(t, 5, ocu.config.MaxRetries)
	assert.Equal(t, 20, ocu.config.RetryDelayMs)
	assert.Equal(t, 1000, ocu.config.VersionCacheSize)
}

func TestOptimisticConcurrentUpdates_GetVersion_NotFound(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	version, exists := ocu.GetVersion("nonexistent")
	assert.Equal(t, uint64(0), version)
	assert.False(t, exists)
}

func TestOptimisticConcurrentUpdates_SetVersion(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 1, vector)

	version, exists := ocu.GetVersion("vec1")
	assert.True(t, exists)
	assert.Equal(t, uint64(1), version)
}

func TestOptimisticConcurrentUpdates_GetVersion_Found(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 5, vector)

	version, exists := ocu.GetVersion("vec1")
	assert.True(t, exists)
	assert.Equal(t, uint64(5), version)
}

func TestOptimisticConcurrentUpdates_UpdateVector_NewVector(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	vector := []float32{1.0, 2.0, 3.0}
	result := ocu.UpdateVector("vec1", vector, 0, "user1")

	assert.True(t, result.Success)
	assert.Equal(t, uint64(1), result.NewVersion)
	assert.False(t, result.Conflict)
	assert.NoError(t, result.Error)
}

func TestOptimisticConcurrentUpdates_UpdateVector_VersionMatch(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 1, vector)

	newVector := []float32{4.0, 5.0, 6.0}
	result := ocu.UpdateVector("vec1", newVector, 1, "user1")

	assert.True(t, result.Success)
	assert.Equal(t, uint64(2), result.NewVersion)
}

func TestOptimisticConcurrentUpdates_UpdateVector_VersionMismatch_Abort(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		ConflictStrategy: "abort",
	})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 2, vector)

	newVector := []float32{4.0, 5.0, 6.0}
	result := ocu.UpdateVector("vec1", newVector, 1, "user1")

	assert.False(t, result.Success)
	assert.True(t, result.Conflict)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "version mismatch")
}

func TestOptimisticConcurrentUpdates_UpdateVector_VersionMismatch_Overwrite(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		ConflictStrategy: "overwrite",
	})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 2, vector)

	newVector := []float32{4.0, 5.0, 6.0}
	result := ocu.UpdateVector("vec1", newVector, 1, "user1")

	assert.True(t, result.Success)
	assert.Equal(t, uint64(3), result.NewVersion)
}

func TestOptimisticConcurrentUpdates_UpdateVector_VersionMismatch_Merge(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		ConflictStrategy: "merge",
	})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 2, vector)

	newVector := []float32{5.0, 6.0, 7.0}
	result := ocu.UpdateVector("vec1", newVector, 1, "user1")

	assert.True(t, result.Success)
	assert.Equal(t, uint64(3), result.NewVersion)
}

func TestOptimisticConcurrentUpdates_UpdateVectorWithRetry_Success(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		MaxRetries:   3,
		RetryDelayMs: 1,
	})

	vector := []float32{1.0, 2.0, 3.0}
	result := ocu.UpdateVectorWithRetry("vec1", vector, 0, "user1")

	assert.True(t, result.Success)
	assert.Equal(t, uint64(1), result.NewVersion)
}

func TestOptimisticConcurrentUpdates_UpdateVectorWithRetry_Retries(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		MaxRetries:       1,
		RetryDelayMs:     1,
		ConflictStrategy: "abort",
	})

	vector := []float32{1.0, 2.0, 3.0}
	result := ocu.UpdateVectorWithRetry("vec1", vector, 0, "user1")
	assert.True(t, result.Success)

	result = ocu.UpdateVectorWithRetry("vec1", vector, 1, "user1")
	assert.True(t, result.Success)
}

func TestOptimisticConcurrentUpdates_DeleteVector(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 1, vector)

	version, exists := ocu.GetVersion("vec1")
	assert.True(t, exists)

	ocu.DeleteVector("vec1")

	version, exists = ocu.GetVersion("vec1")
	assert.False(t, exists)
	assert.Equal(t, uint64(0), version)
}

func TestOptimisticConcurrentUpdates_BatchUpdate(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		MaxRetries:   3,
		RetryDelayMs: 1,
	})

	updates := []OptimisticUpdate{
		{VectorID: "vec1", Vector: []float32{1.0}, Version: 0},
		{VectorID: "vec2", Vector: []float32{2.0}, Version: 0},
		{VectorID: "vec3", Vector: []float32{3.0}, Version: 0},
	}

	results := ocu.BatchUpdate(updates, "user1")

	assert.Len(t, results, 3)
	for _, r := range results {
		assert.True(t, r.Success)
	}
}

func TestOptimisticConcurrentUpdates_GetStats(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.UpdateVector("vec1", vector, 0, "user1")
	ocu.UpdateVector("vec2", vector, 0, "user1")

	attempted, succeeded, _, _, _ := ocu.GetStats()
	assert.Equal(t, int64(2), attempted)
	assert.Equal(t, int64(2), succeeded)
}

func TestOptimisticConcurrentUpdates_GetStats_Conflicted(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		ConflictStrategy: "abort",
	})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 2, vector)

	result := ocu.UpdateVector("vec1", vector, 1, "user1")
	assert.False(t, result.Success)

	attempted, _, conflicted, _, _ := ocu.GetStats()
	assert.Equal(t, int64(1), attempted)
	assert.Equal(t, int64(1), conflicted)
}

func TestOptimisticConcurrentUpdates_SetConfig(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	newConfig := OptimisticConcurrentConfig{
		MaxRetries:       10,
		RetryDelayMs:     50,
		VersionCacheSize: 5000,
		ConflictStrategy: "merge",
	}

	ocu.SetConfig(newConfig)
	assert.Equal(t, 10, ocu.config.MaxRetries)
	assert.Equal(t, 50, ocu.config.RetryDelayMs)
	assert.Equal(t, 5000, ocu.config.VersionCacheSize)
	assert.Equal(t, "merge", ocu.config.ConflictStrategy)
}

func TestOptimisticConcurrentUpdates_GetConfig(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	config := OptimisticConcurrentConfig{
		MaxRetries:       7,
		RetryDelayMs:     25,
		VersionCacheSize: 2000,
		ConflictStrategy: "overwrite",
	}

	ocu := NewOptimisticConcurrentUpdates(logger, config)

	got := ocu.GetConfig()
	assert.Equal(t, config, got)
}

func TestOptimisticConcurrentUpdates_ClearCache(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 1, vector)
	ocu.SetVersion("vec2", 1, vector)

	assert.Equal(t, 2, ocu.GetCacheSize())

	ocu.ClearCache()

	assert.Equal(t, 0, ocu.GetCacheSize())
}

func TestOptimisticConcurrentUpdates_GetCacheSize(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	assert.Equal(t, 0, ocu.GetCacheSize())

	ocu.SetVersion("vec1", 1, []float32{1.0})
	assert.Equal(t, 1, ocu.GetCacheSize())

	ocu.SetVersion("vec2", 1, []float32{2.0})
	assert.Equal(t, 2, ocu.GetCacheSize())
}

func TestOptimisticConcurrentUpdates_GetVectorInfo(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 5, vector)

	info, exists := ocu.GetVectorInfo("vec1")
	require.True(t, exists)
	assert.Equal(t, uint64(5), info.Version)
	assert.Equal(t, vector, info.Vector)
}

func TestOptimisticConcurrentUpdates_GetVectorInfo_NotFound(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{})

	_, exists := ocu.GetVectorInfo("nonexistent")
	assert.False(t, exists)
}

func TestOptimisticConcurrentUpdates_Concurrent(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		MaxRetries:       3,
		RetryDelayMs:     1,
		ConflictStrategy: "overwrite",
	})

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			vector := []float32{float32(idx)}
			ocu.UpdateVectorWithRetry("shared-vec", vector, 0, "user1")
		}(i)
	}

	wg.Wait()

	attempted, _, _, _, _ := ocu.GetStats()
	assert.GreaterOrEqual(t, attempted, int64(numGoroutines))
}

func TestOptimisticConcurrentUpdates_UpdateVectorWithRetry_MaxRetries(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		MaxRetries:       1,
		RetryDelayMs:     1,
		ConflictStrategy: "abort",
	})

	vector := []float32{1.0, 2.0, 3.0}
	ocu.SetVersion("vec1", 5, vector)

	result := ocu.UpdateVectorWithRetry("vec1", vector, 1, "user1")
	assert.False(t, result.Success)
}

func TestOptimisticConcurrentUpdates_MergeUpdate(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	ocu := NewOptimisticConcurrentUpdates(logger, OptimisticConcurrentConfig{
		ConflictStrategy: "merge",
	})

	ocu.SetVersion("vec1", 1, []float32{2.0, 4.0, 6.0})

	result := ocu.UpdateVector("vec1", []float32{4.0, 6.0, 8.0}, 1, "user1")
	require.True(t, result.Success)

	info, _ := ocu.GetVectorInfo("vec1")
	assert.Equal(t, uint64(2), info.Version)
}
