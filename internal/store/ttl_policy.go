package store

import (
	"context"
	"sync"
	"time"
)

// TTLPolicy manages the time-to-live policy for vectors, automatically deleting expired data.
type TTLPolicy struct {
	mu              sync.RWMutex
	defaultTTL      time.Duration
	enabled         bool
	cleanupInterval time.Duration
	temporalIndex   *TemporalIndex
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

// TTLPolicyConfig defines the configuration for the TTL policy.
type TTLPolicyConfig struct {
	Enabled         bool
	DefaultTTL      time.Duration
	CleanupInterval time.Duration
}

// DefaultTTLPolicyConfig returns a TTLPolicyConfig with production defaults.
func DefaultTTLPolicyConfig() TTLPolicyConfig {
	return TTLPolicyConfig{
		Enabled:         false,
		DefaultTTL:      30 * 24 * time.Hour,
		CleanupInterval: time.Hour,
	}
}

// NewTTLPolicy creates a new TTLPolicy with the given temporal index and configuration.
func NewTTLPolicy(temporalIndex *TemporalIndex, cfg TTLPolicyConfig) *TTLPolicy {
	return &TTLPolicy{
		defaultTTL:      cfg.DefaultTTL,
		enabled:         cfg.Enabled,
		cleanupInterval: cfg.CleanupInterval,
		temporalIndex:   temporalIndex,
		stopChan:        make(chan struct{}),
	}
}

// Start begins the background cleanup process for the TTL policy.
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

// Stop terminates the background cleanup process for the TTL policy.
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

// SetEnabled enables or disables the TTL policy.
func (tp *TTLPolicy) SetEnabled(enabled bool) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.enabled = enabled
}

// IsEnabled returns whether the TTL policy is currently enabled.
func (tp *TTLPolicy) IsEnabled() bool {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return tp.enabled
}

// SetDefaultTTL updates the default TTL duration.
func (tp *TTLPolicy) SetDefaultTTL(ttl time.Duration) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.defaultTTL = ttl
}

// GetDefaultTTL returns the current default TTL duration.
func (tp *TTLPolicy) GetDefaultTTL() time.Duration {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return tp.defaultTTL
}
