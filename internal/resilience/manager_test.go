package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDefaultResilienceConfig runs the test default resilience config operation.
func TestDefaultResilienceConfig(t *testing.T) {
	cfg := DefaultResilienceConfig()
	assert.NotNil(t, cfg.Retry)
	assert.NotNil(t, cfg.Timeout)
	assert.NotNil(t, cfg.Circuit)
	assert.NotNil(t, cfg.Bulkheads)
	assert.Equal(t, 100, cfg.Bulkheads["search"])
	assert.Equal(t, 50, cfg.Bulkheads["storage"])
}

// TestNewResilienceManager runs the test new resilience manager operation.
func TestNewResilienceManager(t *testing.T) {
	rm := NewResilienceManager(nil)
	assert.NotNil(t, rm)
}

// TestNewResilienceManagerWithConfig runs the test new resilience manager with config operation.
func TestNewResilienceManagerWithConfig(t *testing.T) {
	cfg := DefaultResilienceConfig()
	rm := NewResilienceManager(cfg)
	assert.NotNil(t, rm)
}

// TestExecuteWithResilience runs the test execute with resilience operation.
func TestExecuteWithResilience(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := rm.ExecuteWithResilience(context.Background(), "search", "test", func() (any, error) {
		return "ok", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "ok", result)
}

// TestExecuteWithResilienceError runs the test execute with resilience error operation.
func TestExecuteWithResilienceError(t *testing.T) {
	rm := NewResilienceManager(nil)
	_, err := rm.ExecuteWithResilience(context.Background(), "search", "test", func() (any, error) {
		return nil, errors.New("fail")
	})
	assert.Error(t, err)
}

// TestExecuteWithResilienceGeneric runs the test execute with resilience generic operation.
func TestExecuteWithResilienceGeneric(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithResilience(rm, context.Background(), "search", "test", func() (string, error) {
		return "hello", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "hello", result)
}

// TestExecuteWithRetry runs the test execute with retry operation.
func TestExecuteWithRetry(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := rm.ExecuteWithRetry(context.Background(), "network", "svc", func() (any, error) {
		return "retried", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "retried", result)
}

// TestExecuteWithRetryGeneric runs the test execute with retry generic operation.
func TestExecuteWithRetryGeneric(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithRetry(rm, context.Background(), "storage", "svc", func() (string, error) {
		return "data", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "data", result)
}

// TestExecuteWithDegradation runs the test execute with degradation operation.
func TestExecuteWithDegradation(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := rm.ExecuteWithDegradation(context.Background(), "search", "test", "fallback", func() (any, error) {
		return "primary", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "primary", result)
}

// TestGetCircuitBreaker runs the test get circuit breaker operation.
func TestGetCircuitBreaker(t *testing.T) {
	rm := NewResilienceManager(nil)
	cb := rm.GetCircuitBreaker("test")
	assert.NotNil(t, cb)
	assert.Equal(t, "test", cb.Name())
}

// TestGetTimeoutManager runs the test get timeout manager operation.
func TestGetTimeoutManager(t *testing.T) {
	rm := NewResilienceManager(nil)
	tm := rm.GetTimeoutManager("search")
	assert.NotNil(t, tm)
}

// TestGetBulkhead runs the test get bulkhead operation.
func TestGetBulkhead(t *testing.T) {
	rm := NewResilienceManager(nil)
	b := rm.GetBulkhead("search")
	assert.NotNil(t, b)
}

// TestGetBulkheadUnknown runs the test get bulkhead unknown operation.
func TestGetBulkheadUnknown(t *testing.T) {
	rm := NewResilienceManager(nil)
	b := rm.GetBulkhead("unknown")
	assert.NotNil(t, b)
	assert.Equal(t, 50, b.AvailableSlots())
}

// TestAddFallbackStrategy runs the test add fallback strategy operation.
func TestAddFallbackStrategy(t *testing.T) {
	rm := NewResilienceManager(nil)
	rm.AddFallbackStrategy("stale", DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return "cached", nil
	}, true)
	assert.NotPanics(t, func() { rm.AddFallbackStrategy("s", DegradationNone, nil, true) })
}

// TestResilienceManagerAddHealthCheck runs the test resilience manager add health check operation.
func TestResilienceManagerAddHealthCheck(t *testing.T) {
	rm := NewResilienceManager(nil)
	rm.AddHealthCheck("db", func() error { return nil })
	assert.NotPanics(t, func() { rm.AddHealthCheck("h", func() error { return nil }) })
}

// TestGetFallbackCache runs the test get fallback cache operation.
func TestGetFallbackCache(t *testing.T) {
	rm := NewResilienceManager(nil)
	fc := rm.GetFallbackCache()
	assert.NotNil(t, fc)
}

// TestGetMetrics runs the test get metrics operation.
func TestGetMetrics(t *testing.T) {
	rm := NewResilienceManager(nil)
	metrics := rm.GetMetrics()
	assert.Contains(t, metrics, "circuit_breakers")
	assert.Contains(t, metrics, "degradation")
	assert.Contains(t, metrics, "cache_size")
	assert.Contains(t, metrics, "bulkheads")
}

// TestResetAll runs the test reset all operation.
func TestResetAll(t *testing.T) {
	rm := NewResilienceManager(nil)
	rm.ResetAll()
	assert.NotPanics(t, rm.ResetAll)
}

// TestNewResilienceInterceptor runs the test new resilience interceptor operation.
func TestNewResilienceInterceptor(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	assert.NotNil(t, ri)
	assert.Same(t, rm, ri.GetManager())
}

// TestInterceptNetworkCall runs the test intercept network call operation.
func TestInterceptNetworkCall(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptNetworkCall(context.Background(), "svc", func() (interface{}, error) {
		return "ok", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "ok", result)
}

// TestInterceptStorageCall runs the test intercept storage call operation.
func TestInterceptStorageCall(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptStorageCall(context.Background(), "svc", func() (interface{}, error) {
		return "stored", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "stored", result)
}

// TestInterceptSearchCall runs the test intercept search call operation.
func TestInterceptSearchCall(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptSearchCall(context.Background(), "svc", func() (interface{}, error) {
		return "found", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "found", result)
}

// TestInterceptReplicationCall runs the test intercept replication call operation.
func TestInterceptReplicationCall(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptReplicationCall(context.Background(), "svc", func() (interface{}, error) {
		return "replicated", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "replicated", result)
}

// TestInterceptCallWithFallback runs the test intercept call with fallback operation.
func TestInterceptCallWithFallback(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptCallWithFallback(context.Background(), "search", "svc", "fallback", func() (interface{}, error) {
		return "primary", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "primary", result)
}

// TestInterceptCallWithFallbackErrorPath runs the test intercept call with fallback error path operation.
func TestInterceptCallWithFallbackErrorPath(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptCallWithFallback(context.Background(), "search", "svc", "fallback", func() (interface{}, error) {
		return nil, errors.New("primary failed")
	})
	assert.Error(t, err)
	assert.Nil(t, result)
}

// TestExecuteWithRetryNetwork runs the test execute with retry network operation.
func TestExecuteWithRetryNetwork(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithRetry(rm, context.Background(), "network", "svc", func() (string, error) {
		return "net-result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "net-result", result)
}

// TestExecuteWithRetryReplication runs the test execute with retry replication operation.
func TestExecuteWithRetryReplication(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithRetry(rm, context.Background(), "replication", "svc", func() (string, error) {
		return "repl-result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "repl-result", result)
}

// TestExecuteWithRetryDefaultOpType runs the test execute with retry default op type operation.
func TestExecuteWithRetryDefaultOpType(t *testing.T) {
	cfg := DefaultResilienceConfig()
	cfg.Bulkheads["default_op"] = 50
	rm := NewResilienceManager(cfg)
	result, err := ExecuteWithRetry(rm, context.Background(), "default_op", "svc", func() (string, error) {
		return "default-result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "default-result", result)
}

// TestExecuteWithResilienceSearch runs the test execute with resilience search operation.
func TestExecuteWithResilienceSearch(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithResilience(rm, context.Background(), "storage", "svc", func() (string, error) {
		return "stored", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "stored", result)
}

// TestGetGlobalResilienceAutoInit runs the test get global resilience auto init operation.
func TestGetGlobalResilienceAutoInit(t *testing.T) {
	oldMgr := GlobalResilienceManager
	oldInt := GlobalResilienceInterceptor
	// oldOnce := once
	GlobalResilienceManager = nil
	GlobalResilienceInterceptor = nil
	once = *new(sync.Once)

	mgr := GetGlobalResilience()
	assert.NotNil(t, mgr)

	ri := GetGlobalResilienceInterceptor()
	assert.NotNil(t, ri)

	GlobalResilienceManager = oldMgr
	GlobalResilienceInterceptor = oldInt
	once = *new(sync.Once)
}

// TestGlobalResilience runs the test global resilience operation.
func TestGlobalResilience(t *testing.T) {
	InitializeGlobalResilience(nil)
	assert.NotNil(t, GlobalResilienceManager)
	assert.NotNil(t, GlobalResilienceInterceptor)

	mgr := GetGlobalResilience()
	assert.Same(t, GlobalResilienceManager, mgr)

	ri := GetGlobalResilienceInterceptor()
	assert.Same(t, GlobalResilienceInterceptor, ri)
}
