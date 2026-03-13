package storage

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type RemoteStorage interface {
	Put(ctx context.Context, key string, r io.Reader) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
}

type Tier string

const (
	TierHot  Tier = "hot"
	TierWarm Tier = "warm"
	TierCold Tier = "cold"
	TierGPU  Tier = "gpu"
)

type StorageTier = Tier

type TierAccessStats struct {
	AccessCount     int64
	LastAccess      time.Time
	TotalReadBytes  int64
	TotalWriteBytes int64
}

type TierManager struct {
	mu sync.RWMutex

	tiers map[Tier]*TierStorage

	config TieredStorageConfig
	logger *zerolog.Logger

	accessStats map[string]*TierAccessStats
	statsMu     sync.RWMutex
}

type TierStorage struct {
	tier        Tier
	directory   string
	mmapEnabled bool
	evictFn     func(keys []string) error

	mu   sync.RWMutex
	keys map[string]bool
}

type TieredStorageConfig struct {
	HotDirectory    string
	WarmDirectory   string
	ColdDirectory   string
	CacheSizeMB     int64
	AccessThreshold int
	AgeThreshold    time.Duration
	EvictBatchSize  int
}

func NewTierManager(config TieredStorageConfig, logger *zerolog.Logger) (*TierManager, error) {
	tm := &TierManager{
		tiers:       make(map[Tier]*TierStorage),
		config:      config,
		logger:      logger,
		accessStats: make(map[string]*TierAccessStats),
	}

	if config.HotDirectory != "" {
		if err := os.MkdirAll(config.HotDirectory, 0755); err != nil {
			return nil, err
		}
		tm.tiers[TierHot] = &TierStorage{
			tier:        TierHot,
			directory:   config.HotDirectory,
			mmapEnabled: true,
			keys:        make(map[string]bool),
		}
	}

	if config.WarmDirectory != "" {
		if err := os.MkdirAll(config.WarmDirectory, 0755); err != nil {
			return nil, err
		}
		tm.tiers[TierWarm] = &TierStorage{
			tier:      TierWarm,
			directory: config.WarmDirectory,
			keys:      make(map[string]bool),
		}
	}

	if config.ColdDirectory != "" {
		if err := os.MkdirAll(config.ColdDirectory, 0755); err != nil {
			return nil, err
		}
		tm.tiers[TierCold] = &TierStorage{
			tier:      TierCold,
			directory: config.ColdDirectory,
			keys:      make(map[string]bool),
		}
	}

	return tm, nil
}

func (tm *TierManager) GetTier(key string) Tier {
	tm.statsMu.RLock()
	stats, ok := tm.accessStats[key]
	tm.statsMu.RUnlock()

	if !ok {
		return TierHot
	}

	accessThreshold := int64(tm.config.AccessThreshold)
	if time.Since(stats.LastAccess) < tm.config.AgeThreshold && stats.AccessCount >= accessThreshold {
		return TierHot
	}

	if stats.AccessCount >= accessThreshold/2 {
		return TierWarm
	}

	return TierCold
}

func (tm *TierManager) RecordAccess(key string, bytesRead int64) {
	tm.statsMu.Lock()
	defer tm.statsMu.Unlock()

	stats, ok := tm.accessStats[key]
	if !ok {
		stats = &TierAccessStats{}
		tm.accessStats[key] = stats
	}

	stats.AccessCount++
	stats.LastAccess = time.Now()
	stats.TotalReadBytes += bytesRead
}

func (tm *TierManager) RecordWrite(key string, bytesWritten int64) {
	tm.statsMu.Lock()
	defer tm.statsMu.Unlock()

	stats, ok := tm.accessStats[key]
	if !ok {
		stats = &TierAccessStats{}
		tm.accessStats[key] = stats
	}

	stats.TotalWriteBytes += bytesWritten
}

func (tm *TierManager) MigrateToTier(key string, targetTier Tier) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	currentTier := tm.GetTier(key)

	if currentTier == targetTier {
		return nil
	}

	tm.logger.Info().
		Str("key", key).
		Str("from", string(currentTier)).
		Str("to", string(targetTier)).
		Msg("Migrating vector between tiers")

	fromStorage := tm.tiers[currentTier]
	toStorage := tm.tiers[targetTier]

	if fromStorage != nil {
		fromStorage.mu.Lock()
		delete(fromStorage.keys, key)
		fromStorage.mu.Unlock()
	}

	if toStorage != nil {
		toStorage.mu.Lock()
		toStorage.keys[key] = true
		toStorage.mu.Unlock()
	}

	return nil
}

func (tm *TierManager) GetStorage(tier Tier) *TierStorage {
	return tm.tiers[tier]
}

func (tm *TierManager) RunTierMigration(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tm.migrateColdToWarm()
			tm.migrateWarmToHot()
		}
	}
}

func (tm *TierManager) migrateColdToWarm() {
	tm.statsMu.RLock()
	var coldKeys []string
	for key, stats := range tm.accessStats {
		if tm.GetTier(key) == TierCold && stats.AccessCount > 0 {
			coldKeys = append(coldKeys, key)
		}
	}
	tm.statsMu.RUnlock()

	for _, key := range coldKeys {
		if tm.GetTier(key) == TierCold {
			_ = tm.MigrateToTier(key, TierWarm) // nosec G104
		}
	}
}

func (tm *TierManager) migrateWarmToHot() {
	tm.statsMu.RLock()
	var warmKeys []string
	accessThreshold := int64(tm.config.AccessThreshold)
	for key, stats := range tm.accessStats {
		if tm.GetTier(key) == TierWarm && stats.AccessCount >= accessThreshold {
			warmKeys = append(warmKeys, key)
		}
	}
	tm.statsMu.RUnlock()

	for _, key := range warmKeys {
		if tm.GetTier(key) == TierWarm {
			_ = tm.MigrateToTier(key, TierHot) // nosec G104
		}
	}
}

type TieredVectorStore struct {
	tierManager *TierManager
	gpuEnabled  bool
	gpuMemLimit int64
}

func NewTieredVectorStore(tierManager *TierManager, gpuEnabled bool, gpuMemLimit int64) *TieredVectorStore {
	return &TieredVectorStore{
		tierManager: tierManager,
		gpuEnabled:  gpuEnabled,
		gpuMemLimit: gpuMemLimit,
	}
}

func (tv *TieredVectorStore) ShouldUseGPU(vectorSize int) bool {
	if !tv.gpuEnabled {
		return false
	}

	estimatedGPUmem := int64(vectorSize) * 4
	return estimatedGPUmem < tv.gpuMemLimit
}

func (tv *TieredVectorStore) GetTierForVector(id string, vectorSize int) Tier {
	stats := tv.tierManager.accessStats[id]
	if stats == nil {
		return TierHot
	}

	if tv.ShouldUseGPU(vectorSize) && stats.AccessCount > 100 {
		return TierGPU
	}

	return tv.tierManager.GetTier(id)
}
