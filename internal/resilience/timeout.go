package resilience

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type TimeoutConfig struct {
	DefaultTimeout     time.Duration
	NetworkTimeout     time.Duration
	StorageTimeout     time.Duration
	SearchTimeout      time.Duration
	ReplicationTimeout time.Duration
	GracePeriod        time.Duration
}

func DefaultTimeoutConfig() *TimeoutConfig {
	return &TimeoutConfig{
		DefaultTimeout:     30 * time.Second,
		NetworkTimeout:     10 * time.Second,
		StorageTimeout:     60 * time.Second,
		SearchTimeout:      5 * time.Second,
		ReplicationTimeout: 120 * time.Second,
		GracePeriod:        1 * time.Second,
	}
}

type TimeoutManager struct {
	config *TimeoutConfig
	mu     sync.RWMutex
}

func NewTimeoutManager(config *TimeoutConfig) *TimeoutManager {
	if config == nil {
		config = DefaultTimeoutConfig()
	}
	return &TimeoutManager{
		config: config,
	}
}

func (tm *TimeoutManager) GetTimeout(operationType string) time.Duration {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	switch operationType {
	case "network":
		return tm.config.NetworkTimeout
	case "storage":
		return tm.config.StorageTimeout
	case "search":
		return tm.config.SearchTimeout
	case "replication":
		return tm.config.ReplicationTimeout
	default:
		return tm.config.DefaultTimeout
	}
}

func (tm *TimeoutManager) WithTimeout(ctx context.Context, operationType string, fn func(context.Context) error) error {
	timeout := tm.GetTimeout(operationType)
	return tm.WithCustomTimeout(ctx, timeout, fn)
}

func (tm *TimeoutManager) WithCustomTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	errChan := make(chan error, 1)

	go func() {
		errChan <- fn(timeoutCtx)
	}()

	select {
	case err := <-errChan:
		return err
	case <-timeoutCtx.Done():
		return fmt.Errorf("operation timed out after %v", timeout)
	}
}

func (tm *TimeoutManager) WithTimeoutGraceful(ctx context.Context, operationType string, fn func(context.Context) error) error {
	timeout := tm.GetTimeout(operationType) + tm.config.GracePeriod
	return tm.WithCustomTimeout(ctx, timeout, fn)
}

func (tm *TimeoutManager) UpdateConfig(config *TimeoutConfig) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.config = config
}

type AdaptiveTimeout struct {
	baseTimeout  time.Duration
	minTimeout   time.Duration
	maxTimeout   time.Duration
	adjustment   float64
	measurements []time.Duration
	mu           sync.RWMutex
}

func NewAdaptiveTimeout(base, minTimeout, maxTimeout time.Duration) *AdaptiveTimeout {
	return &AdaptiveTimeout{
		baseTimeout:  base,
		minTimeout:   minTimeout,
		maxTimeout:   maxTimeout,
		adjustment:   0.1,
		measurements: make([]time.Duration, 0, 100),
	}
}

func (at *AdaptiveTimeout) GetTimeout() time.Duration {
	at.mu.RLock()
	defer at.mu.RUnlock()

	if len(at.measurements) == 0 {
		return at.baseTimeout
	}

	p95 := at.percentile(0.95)

	newTimeout := time.Duration(float64(p95) * (1 + at.adjustment))

	if newTimeout < at.minTimeout {
		newTimeout = at.minTimeout
	} else if newTimeout > at.maxTimeout {
		newTimeout = at.maxTimeout
	}

	return newTimeout
}

func (at *AdaptiveTimeout) RecordDuration(d time.Duration) {
	at.mu.Lock()
	defer at.mu.Unlock()

	at.measurements = append(at.measurements, d)
	if len(at.measurements) > 100 {
		at.measurements = at.measurements[1:]
	}
}

func (at *AdaptiveTimeout) percentile(p float64) time.Duration {
	if len(at.measurements) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(at.measurements))
	copy(sorted, at.measurements)

	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	index := int(float64(len(sorted)) * p)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

type TimeoutGroup struct {
	managers map[string]*TimeoutManager
	adaptive map[string]*AdaptiveTimeout
	mu       sync.RWMutex
}

func NewTimeoutGroup() *TimeoutGroup {
	return &TimeoutGroup{
		managers: make(map[string]*TimeoutManager),
		adaptive: make(map[string]*AdaptiveTimeout),
	}
}

func (tg *TimeoutGroup) GetManager(name string) *TimeoutManager {
	tg.mu.RLock()
	manager, exists := tg.managers[name]
	tg.mu.RUnlock()

	if exists {
		return manager
	}

	tg.mu.Lock()
	defer tg.mu.Unlock()

	if manager, exists := tg.managers[name]; exists {
		return manager
	}

	manager = NewTimeoutManager(DefaultTimeoutConfig())
	tg.managers[name] = manager

	return manager
}

func (tg *TimeoutGroup) GetAdaptiveTimeout(name string, base, minTimeout, maxTimeout time.Duration) *AdaptiveTimeout {
	tg.mu.RLock()
	timeout, exists := tg.adaptive[name]
	tg.mu.RUnlock()

	if exists {
		return timeout
	}

	tg.mu.Lock()
	defer tg.mu.Unlock()

	if timeout, exists := tg.adaptive[name]; exists {
		return timeout
	}

	timeout = NewAdaptiveTimeout(base, minTimeout, maxTimeout)
	tg.adaptive[name] = timeout

	return timeout
}

func (tg *TimeoutGroup) WithTimeout(ctx context.Context, managerName, operationType string, fn func(context.Context) error) error {
	manager := tg.GetManager(managerName)
	return manager.WithTimeout(ctx, operationType, fn)
}

func (tg *TimeoutGroup) WithAdaptiveTimeout(ctx context.Context, timeoutName string, fn func(context.Context) error) error {
	timeout := tg.GetAdaptiveTimeout(timeoutName, 30*time.Second, 5*time.Second, 120*time.Second)

	timeoutDuration := timeout.GetTimeout()
	start := time.Now()

	err := func() error {
		timeoutCtx, cancel := context.WithTimeout(ctx, timeoutDuration)
		defer cancel()

		errChan := make(chan error, 1)

		go func() {
			errChan <- fn(timeoutCtx)
		}()

		select {
		case err := <-errChan:
			return err
		case <-timeoutCtx.Done():
			return fmt.Errorf("adaptive operation timed out after %v", timeoutDuration)
		}
	}()

	duration := time.Since(start)
	timeout.RecordDuration(duration)

	return err
}

func (tg *TimeoutGroup) RemoveManager(name string) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	delete(tg.managers, name)
}

func (tg *TimeoutGroup) RemoveAdaptiveTimeout(name string) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	delete(tg.adaptive, name)
}
