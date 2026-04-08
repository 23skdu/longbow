package store

import (
	"context"
	"sync"
	"time"
)

type TTLPolicy struct {
	mu              sync.RWMutex
	defaultTTL      time.Duration
	enabled         bool
	cleanupInterval time.Duration
	temporalIndex   *TemporalIndex
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

type TTLPolicyConfig struct {
	Enabled         bool
	DefaultTTL      time.Duration
	CleanupInterval time.Duration
}

func DefaultTTLPolicyConfig() TTLPolicyConfig {
	return TTLPolicyConfig{
		Enabled:         false,
		DefaultTTL:      30 * 24 * time.Hour,
		CleanupInterval: time.Hour,
	}
}

func NewTTLPolicy(temporalIndex *TemporalIndex, cfg TTLPolicyConfig) *TTLPolicy {
	return &TTLPolicy{
		defaultTTL:      cfg.DefaultTTL,
		enabled:         cfg.Enabled,
		cleanupInterval: cfg.CleanupInterval,
		temporalIndex:   temporalIndex,
		stopChan:        make(chan struct{}),
	}
}

func (tp *TTLPolicy) Start(ctx context.Context) {
	if !tp.enabled {
		return
	}

	tp.wg.Add(1)
	go func() {
		defer tp.wg.Done()
		ticker := time.NewTicker(tp.cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tp.stopChan:
				return
			case <-ticker.C:
				tp.cleanup()
			}
		}
	}()
}

func (tp *TTLPolicy) Stop() {
	if !tp.enabled {
		return
	}
	close(tp.stopChan)
	tp.wg.Wait()
}

func (tp *TTLPolicy) cleanup() {
	if tp.temporalIndex == nil {
		return
	}

	ctx := context.Background()
	now := time.Now().UnixNano()
	cutoff := now - tp.defaultTTL.Nanoseconds()

	deleted, err := tp.temporalIndex.DeleteByTime(ctx, cutoff)
	if err != nil {
		return
	}

	if deleted > 0 {
		_ = deleted
	}
}

func (tp *TTLPolicy) SetEnabled(enabled bool) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.enabled = enabled
}

func (tp *TTLPolicy) IsEnabled() bool {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return tp.enabled
}

func (tp *TTLPolicy) SetDefaultTTL(ttl time.Duration) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.defaultTTL = ttl
}

func (tp *TTLPolicy) GetDefaultTTL() time.Duration {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return tp.defaultTTL
}
