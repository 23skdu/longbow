package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
)

type RateLimitConfig struct {
	RequestsPerSecond float64
	Burst             int
	Enabled           bool
}

type namespaceRateLimiter struct {
	mu           sync.RWMutex
	requests     []int64
	windowStart  time.Time
	config       RateLimitConfig
	blocked      bool
	blockExpires time.Time
}

func newNamespaceRateLimiter(config RateLimitConfig) *namespaceRateLimiter {
	return &namespaceRateLimiter{
		config:      config,
		windowStart: time.Now(),
		requests:    make([]int64, 0),
	}
}

func (r *namespaceRateLimiter) Allow() bool {
	if !r.config.Enabled {
		return true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.blocked {
		if time.Now().After(r.blockExpires) {
			r.blocked = false
			r.requests = r.requests[:0]
			r.windowStart = time.Now()
		} else {
			metrics.RecordNamespaceRateLimitHit("unknown")
			return false
		}
	}

	now := time.Now()
	windowDuration := time.Second

	if now.Sub(r.windowStart) >= windowDuration {
		r.requests = r.requests[:0]
		r.windowStart = now
	}

	currentRate := float64(len(r.requests)) / now.Sub(r.windowStart).Seconds()
	if currentRate > r.config.RequestsPerSecond {
		r.blocked = true
		r.blockExpires = now.Add(10 * time.Second)
		metrics.RecordNamespaceRateLimitHit("unknown")
		return false
	}

	r.requests = append(r.requests, now.UnixNano())
	return true
}

func (r *namespaceRateLimiter) SetConfig(config RateLimitConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.config = config
}

func (r *namespaceRateLimiter) GetConfig() RateLimitConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}

type RateLimiterManager struct {
	mu         sync.RWMutex
	limiters   map[string]*namespaceRateLimiter
	defaultCfg RateLimitConfig
}

func NewRateLimiterManager(defaultCfg RateLimitConfig) *RateLimiterManager {
	return &RateLimiterManager{
		defaultCfg: defaultCfg,
		limiters:   make(map[string]*namespaceRateLimiter),
	}
}

func (m *RateLimiterManager) GetLimiter(namespace string) *namespaceRateLimiter {
	m.mu.RLock()
	limiter, ok := m.limiters[namespace]
	m.mu.RUnlock()

	if ok {
		return limiter
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if limiter, ok = m.limiters[namespace]; ok {
		return limiter
	}

	limiter = newNamespaceRateLimiter(m.defaultCfg)
	m.limiters[namespace] = limiter
	return limiter
}

func (m *RateLimiterManager) SetLimit(namespace string, config RateLimitConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limiter, ok := m.limiters[namespace]; ok {
		limiter.SetConfig(config)
	} else {
		m.limiters[namespace] = newNamespaceRateLimiter(config)
	}
}

func (m *RateLimiterManager) RemoveLimit(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.limiters, namespace)
}

func (m *RateLimiterManager) Allow(namespace string) bool {
	limiter := m.GetLimiter(namespace)
	allowed := limiter.Allow()

	ns := namespace
	if !allowed {
		metrics.RecordNamespaceRateLimitHit(ns)
	}
	return allowed
}

var (
	ErrNamespaceRateLimited = errors.New("namespace rate limit exceeded")
)

func (vs *VectorStore) CheckNamespaceRateLimit(namespace string) error {
	if vs.rateLimiterManager == nil {
		return nil
	}

	if !vs.rateLimiterManager.Allow(namespace) {
		return ErrNamespaceRateLimited
	}
	return nil
}

func (vs *VectorStore) SetNamespaceRateLimit(namespace string, config RateLimitConfig) error {
	if vs.rateLimiterManager == nil {
		vs.rateLimiterManager = NewRateLimiterManager(RateLimitConfig{Enabled: false})
	}
	vs.rateLimiterManager.SetLimit(namespace, config)
	return nil
}

func (vs *VectorStore) GetNamespaceRateLimit(namespace string) (RateLimitConfig, error) {
	if vs.rateLimiterManager == nil {
		return RateLimitConfig{}, nil
	}
	return vs.rateLimiterManager.GetLimiter(namespace).GetConfig(), nil
}

type NamespaceMigrationConfig struct {
	SourceNamespace string
	TargetNamespace string
	TargetNode      string
	Datasets        []string
	CopyMode        bool
}

type NamespaceMigrationResult struct {
	Success          bool
	MigratedDatasets int
	FailedDatasets   []string
	Duration         time.Duration
}

func (vs *VectorStore) MigrateNamespace(config NamespaceMigrationConfig) (*NamespaceMigrationResult, error) {
	startTime := time.Now()
	result := &NamespaceMigrationResult{
		FailedDatasets: make([]string, 0),
	}

	sourceNS := vs.GetNamespace(config.SourceNamespace)
	if sourceNS == nil {
		return result, errors.New("source namespace not found")
	}

	targetNS := vs.GetNamespace(config.TargetNamespace)
	if targetNS == nil {
		if err := vs.CreateNamespace(config.TargetNamespace); err != nil {
			return result, fmt.Errorf("failed to create target namespace: %w", err)
		}
		targetNS = vs.GetNamespace(config.TargetNamespace)
	}

	datasets := sourceNS.ListDatasets()
	if len(config.Datasets) > 0 {
		var filtered []string
		for _, ds := range datasets {
			for _, wanted := range config.Datasets {
				if ds == wanted {
					filtered = append(filtered, ds)
				}
			}
		}
		datasets = filtered
	}

	for _, dataset := range datasets {
		newName := config.TargetNamespace + "/" + dataset[len(config.SourceNamespace)+1:]

		if config.CopyMode {
			if vs.engine != nil && vs.engine.GetSnapshotBackend() != nil {
				if err := vs.CloneDataset(context.Background(), dataset, newName, vs.engine.GetSnapshotBackend()); err != nil {
					result.FailedDatasets = append(result.FailedDatasets, dataset)
					continue
				}
			} else {
				result.FailedDatasets = append(result.FailedDatasets, dataset)
				continue
			}
		} else {
			if vs.engine != nil && vs.engine.GetSnapshotBackend() != nil {
				_, err := vs.ExportDataset(dataset, vs.engine.GetSnapshotBackend())
				if err != nil {
					result.FailedDatasets = append(result.FailedDatasets, dataset)
					continue
				}

				_, err = vs.ImportDataset(context.Background(), newName, vs.engine.GetSnapshotBackend(), nil)
				if err != nil {
					result.FailedDatasets = append(result.FailedDatasets, dataset)
					continue
				}
			} else {
				result.FailedDatasets = append(result.FailedDatasets, dataset)
				continue
			}
		}

		result.MigratedDatasets++
	}

	result.Success = len(result.FailedDatasets) == 0
	result.Duration = time.Since(startTime)

	return result, nil
}

func (vs *VectorStore) ExportDataset(name string, backend storage.SnapshotBackend) (int64, error) {
	datasetIO := NewDatasetIO(vs)
	return datasetIO.ExportToParquet(vs.ctx, name, backend)
}

func (vs *VectorStore) ImportDataset(ctx context.Context, name string, backend storage.SnapshotBackend, schema *arrow.Schema) (int64, error) {
	datasetIO := NewDatasetIO(vs)
	return datasetIO.ImportFromParquet(ctx, name, backend, schema)
}

func (vs *VectorStore) CloneDataset(ctx context.Context, source, target string, backend storage.SnapshotBackend) error {
	datasetIO := NewDatasetIO(vs)
	_, err := datasetIO.ExportToParquet(ctx, source, backend)
	if err != nil {
		return err
	}
	_, err = datasetIO.ImportFromParquet(ctx, target, backend, nil)
	return err
}
