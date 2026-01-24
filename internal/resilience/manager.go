package resilience

import (
	"context"
	"sync"
	"time"
)

type ResilienceConfig struct {
	Retry     *RetryConfig
	Timeout   *TimeoutConfig
	Circuit   CircuitBreakerSettings
	Bulkheads map[string]int
}

func DefaultResilienceConfig() *ResilienceConfig {
	return &ResilienceConfig{
		Retry:   NewRetryConfig(),
		Timeout: DefaultTimeoutConfig(),
		Circuit: CircuitBreakerSettings{
			Name:        "longbow",
			MaxRequests: 10,
			Interval:    60 * time.Second,
			Timeout:     30 * time.Second,
			ReadyToTrip: DefaultReadyToTrip,
		},
		Bulkheads: map[string]int{
			"search":      100,
			"storage":     50,
			"replication": 10,
			"network":     200,
		},
	}
}

type ResilienceManager struct {
	config        *ResilienceConfig
	circuitGroup  *CircuitBreakerGroup
	timeoutGroup  *TimeoutGroup
	bulkheadGroup *BulkheadGroup
	degradation   *GracefulDegradation
	cache         *FallbackCache
	mu            sync.RWMutex
}

func NewResilienceManager(config *ResilienceConfig) *ResilienceManager {
	if config == nil {
		config = DefaultResilienceConfig()
	}

	return &ResilienceManager{
		config:        config,
		circuitGroup:  NewCircuitBreakerGroup(config.Circuit),
		timeoutGroup:  NewTimeoutGroup(),
		bulkheadGroup: NewBulkheadGroup(),
		degradation:   NewGracefulDegradation(),
		cache:         NewFallbackCache(5 * time.Minute),
	}
}

func (rm *ResilienceManager) ExecuteWithResilience(ctx context.Context, operationType, circuitName string, fn func() (interface{}, error)) (interface{}, error) {
	bulkhead := rm.bulkheadGroup.GetBulkhead(operationType, rm.config.Bulkheads[operationType])

	return bulkhead.Execute(func() (interface{}, error) {
		return rm.circuitGroup.Execute(ctx, circuitName, func() (interface{}, error) {
			timeout := rm.timeoutGroup.GetManager(operationType)

			var result interface{}
			err := timeout.WithTimeout(ctx, operationType, func(timeoutCtx context.Context) error {
				var opErr error
				result, opErr = fn()
				return opErr
			})

			return result, err
		})
	})
}

func (rm *ResilienceManager) ExecuteWithRetry(ctx context.Context, operationType, circuitName string, fn func() (interface{}, error)) (interface{}, error) {
	policy := rm.config.Retry
	var retryPolicy *RetryPolicy

	switch operationType {
	case "network":
		retryPolicy = policy.Network
	case "storage":
		retryPolicy = policy.Storage
	case "search":
		retryPolicy = policy.Search
	case "replication":
		retryPolicy = policy.Replication
	default:
		retryPolicy = DefaultRetryPolicy()
	}

	return Retry(ctx, retryPolicy, func() (interface{}, error) {
		return rm.ExecuteWithResilience(ctx, operationType, circuitName, fn)
	})
}

func (rm *ResilienceManager) ExecuteWithDegradation(ctx context.Context, operationType, circuitName, fallbackStrategy string, primary func() (interface{}, error)) (interface{}, error) {
	return rm.degradation.Execute(ctx, primary, fallbackStrategy)
}

func (rm *ResilienceManager) GetCircuitBreaker(name string) *CircuitBreaker {
	return rm.circuitGroup.GetBreaker(name)
}

func (rm *ResilienceManager) GetTimeoutManager(operationType string) *TimeoutManager {
	return rm.timeoutGroup.GetManager(operationType)
}

func (rm *ResilienceManager) GetBulkhead(operationType string) *Bulkhead {
	concurrency, exists := rm.config.Bulkheads[operationType]
	if !exists {
		concurrency = 50
	}
	return rm.bulkheadGroup.GetBulkhead(operationType, concurrency)
}

func (rm *ResilienceManager) AddFallbackStrategy(name string, level DegradationLevel, fallback FallbackFunc, enabled bool) {
	rm.degradation.AddStrategy(name, level, fallback, enabled)
}

func (rm *ResilienceManager) AddHealthCheck(name string, check func() error) {
	rm.degradation.AddHealthCheck(name, check)
}

func (rm *ResilienceManager) StartHealthMonitoring(ctx context.Context, interval time.Duration) {
	go rm.degradation.StartHealthMonitoring(ctx, interval)
}

func (rm *ResilienceManager) GetFallbackCache() *FallbackCache {
	return rm.cache
}

func (rm *ResilienceManager) GetMetrics() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	metrics := make(map[string]interface{})

	circuitMetrics := rm.circuitGroup.GetAllMetrics()
	metrics["circuit_breakers"] = circuitMetrics

	degradationStats := rm.degradation.GetStats()
	metrics["degradation"] = degradationStats

	metrics["cache_size"] = rm.cache.Size()

	bulkheadStats := make(map[string]interface{})
	for name, concurrency := range rm.config.Bulkheads {
		bulkhead := rm.bulkheadGroup.GetBulkhead(name, concurrency)
		bulkheadStats[name] = map[string]interface{}{
			"available_slots": bulkhead.AvailableSlots(),
			"max_concurrency": concurrency,
		}
	}
	metrics["bulkheads"] = bulkheadStats

	return metrics
}

func (rm *ResilienceManager) ResetAll() {
	rm.circuitGroup.ResetAll()
	rm.cache.Clear()
	rm.degradation.SetLevel(DegradationNone)
}

type ResilienceInterceptor struct {
	manager *ResilienceManager
}

func NewResilienceInterceptor(manager *ResilienceManager) *ResilienceInterceptor {
	return &ResilienceInterceptor{
		manager: manager,
	}
}

func (ri *ResilienceInterceptor) InterceptNetworkCall(ctx context.Context, circuitName string, fn func() (interface{}, error)) (interface{}, error) {
	return ri.manager.ExecuteWithRetry(ctx, "network", circuitName, fn)
}

func (ri *ResilienceInterceptor) InterceptStorageCall(ctx context.Context, circuitName string, fn func() (interface{}, error)) (interface{}, error) {
	return ri.manager.ExecuteWithRetry(ctx, "storage", circuitName, fn)
}

func (ri *ResilienceInterceptor) InterceptSearchCall(ctx context.Context, circuitName string, fn func() (interface{}, error)) (interface{}, error) {
	return ri.manager.ExecuteWithRetry(ctx, "search", circuitName, fn)
}

func (ri *ResilienceInterceptor) InterceptReplicationCall(ctx context.Context, circuitName string, fn func() (interface{}, error)) (interface{}, error) {
	return ri.manager.ExecuteWithRetry(ctx, "replication", circuitName, fn)
}

func (ri *ResilienceInterceptor) InterceptCallWithFallback(ctx context.Context, operationType, circuitName, fallbackStrategy string, primary func() (interface{}, error)) (interface{}, error) {
	result, err := ri.manager.ExecuteWithRetry(ctx, operationType, circuitName, primary)
	if err != nil && fallbackStrategy != "" {
		return ri.manager.ExecuteWithDegradation(ctx, operationType, circuitName, fallbackStrategy, primary)
	}
	return result, err
}

func (ri *ResilienceInterceptor) GetManager() *ResilienceManager {
	return ri.manager
}

var (
	GlobalResilienceManager     *ResilienceManager
	GlobalResilienceInterceptor *ResilienceInterceptor
	once                        sync.Once
)

func InitializeGlobalResilience(config *ResilienceConfig) {
	once.Do(func() {
		GlobalResilienceManager = NewResilienceManager(config)
		GlobalResilienceInterceptor = NewResilienceInterceptor(GlobalResilienceManager)
	})
}

func GetGlobalResilience() *ResilienceManager {
	if GlobalResilienceManager == nil {
		InitializeGlobalResilience(nil)
	}
	return GlobalResilienceManager
}

func GetGlobalResilienceInterceptor() *ResilienceInterceptor {
	if GlobalResilienceInterceptor == nil {
		InitializeGlobalResilience(nil)
	}
	return GlobalResilienceInterceptor
}
