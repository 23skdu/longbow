package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultResilienceConfig(t *testing.T) {
	cfg := DefaultResilienceConfig()
	assert.NotNil(t, cfg.Retry)
	assert.NotNil(t, cfg.Timeout)
	assert.NotNil(t, cfg.Circuit)
	assert.NotNil(t, cfg.Bulkheads)
	assert.Equal(t, 100, cfg.Bulkheads["search"])
	assert.Equal(t, 50, cfg.Bulkheads["storage"])
}

func TestNewResilienceManager(t *testing.T) {
	rm := NewResilienceManager(nil)
	assert.NotNil(t, rm)
}

func TestNewResilienceManagerWithConfig(t *testing.T) {
	cfg := DefaultResilienceConfig()
	rm := NewResilienceManager(cfg)
	assert.NotNil(t, rm)
}

func TestExecuteWithResilience(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := rm.ExecuteWithResilience(context.Background(), "search", "test", func() (any, error) {
		return "ok", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "ok", result)
}

func TestExecuteWithResilienceError(t *testing.T) {
	rm := NewResilienceManager(nil)
	_, err := rm.ExecuteWithResilience(context.Background(), "search", "test", func() (any, error) {
		return nil, errors.New("fail")
	})
	assert.Error(t, err)
}

func TestExecuteWithResilienceGeneric(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithResilience(rm, context.Background(), "search", "test", func() (string, error) {
		return "hello", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func TestExecuteWithRetry(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := rm.ExecuteWithRetry(context.Background(), "network", "svc", func() (any, error) {
		return "retried", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "retried", result)
}

func TestExecuteWithRetryGeneric(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithRetry(rm, context.Background(), "storage", "svc", func() (string, error) {
		return "data", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "data", result)
}

func TestExecuteWithDegradation(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := rm.ExecuteWithDegradation(context.Background(), "search", "test", "fallback", func() (any, error) {
		return "primary", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "primary", result)
}

func TestGetCircuitBreaker(t *testing.T) {
	rm := NewResilienceManager(nil)
	cb := rm.GetCircuitBreaker("test")
	assert.NotNil(t, cb)
	assert.Equal(t, "test", cb.Name())
}

func TestGetTimeoutManager(t *testing.T) {
	rm := NewResilienceManager(nil)
	tm := rm.GetTimeoutManager("search")
	assert.NotNil(t, tm)
}

func TestGetBulkhead(t *testing.T) {
	rm := NewResilienceManager(nil)
	b := rm.GetBulkhead("search")
	assert.NotNil(t, b)
}

func TestGetBulkheadUnknown(t *testing.T) {
	rm := NewResilienceManager(nil)
	b := rm.GetBulkhead("unknown")
	assert.NotNil(t, b)
	assert.Equal(t, 50, b.AvailableSlots())
}

func TestAddFallbackStrategy(t *testing.T) {
	rm := NewResilienceManager(nil)
	rm.AddFallbackStrategy("stale", DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return "cached", nil
	}, true)
	assert.NotPanics(t, func() { rm.AddFallbackStrategy("s", DegradationNone, nil, true) })
}

func TestResilienceManagerAddHealthCheck(t *testing.T) {
	rm := NewResilienceManager(nil)
	rm.AddHealthCheck("db", func() error { return nil })
	assert.NotPanics(t, func() { rm.AddHealthCheck("h", func() error { return nil }) })
}

func TestGetFallbackCache(t *testing.T) {
	rm := NewResilienceManager(nil)
	fc := rm.GetFallbackCache()
	assert.NotNil(t, fc)
}

func TestGetMetrics(t *testing.T) {
	rm := NewResilienceManager(nil)
	metrics := rm.GetMetrics()
	assert.Contains(t, metrics, "circuit_breakers")
	assert.Contains(t, metrics, "degradation")
	assert.Contains(t, metrics, "cache_size")
	assert.Contains(t, metrics, "bulkheads")
}

func TestResetAll(t *testing.T) {
	rm := NewResilienceManager(nil)
	rm.ResetAll()
	assert.NotPanics(t, rm.ResetAll)
}

func TestNewResilienceInterceptor(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	assert.NotNil(t, ri)
	assert.Same(t, rm, ri.GetManager())
}

func TestInterceptNetworkCall(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptNetworkCall(context.Background(), "svc", func() (interface{}, error) {
		return "ok", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "ok", result)
}

func TestInterceptStorageCall(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptStorageCall(context.Background(), "svc", func() (interface{}, error) {
		return "stored", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "stored", result)
}

func TestInterceptSearchCall(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptSearchCall(context.Background(), "svc", func() (interface{}, error) {
		return "found", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "found", result)
}

func TestInterceptReplicationCall(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptReplicationCall(context.Background(), "svc", func() (interface{}, error) {
		return "replicated", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "replicated", result)
}

func TestInterceptCallWithFallback(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptCallWithFallback(context.Background(), "search", "svc", "fallback", func() (interface{}, error) {
		return "primary", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "primary", result)
}

func TestInterceptCallWithFallbackErrorPath(t *testing.T) {
	rm := NewResilienceManager(nil)
	ri := NewResilienceInterceptor(rm)
	result, err := ri.InterceptCallWithFallback(context.Background(), "search", "svc", "fallback", func() (interface{}, error) {
		return nil, errors.New("primary failed")
	})
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestExecuteWithRetryNetwork(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithRetry(rm, context.Background(), "network", "svc", func() (string, error) {
		return "net-result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "net-result", result)
}

func TestExecuteWithRetryReplication(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithRetry(rm, context.Background(), "replication", "svc", func() (string, error) {
		return "repl-result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "repl-result", result)
}

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

func TestExecuteWithResilienceSearch(t *testing.T) {
	rm := NewResilienceManager(nil)
	result, err := ExecuteWithResilience(rm, context.Background(), "storage", "svc", func() (string, error) {
		return "stored", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "stored", result)
}

func TestGetGlobalResilienceAutoInit(t *testing.T) {
	oldMgr := GlobalResilienceManager
	oldInt := GlobalResilienceInterceptor
	oldOnce := once
	GlobalResilienceManager = nil
	GlobalResilienceInterceptor = nil
	once = sync.Once{}

	mgr := GetGlobalResilience()
	assert.NotNil(t, mgr)

	ri := GetGlobalResilienceInterceptor()
	assert.NotNil(t, ri)

	GlobalResilienceManager = oldMgr
	GlobalResilienceInterceptor = oldInt
	once = oldOnce
}

func TestGlobalResilience(t *testing.T) {
	InitializeGlobalResilience(nil)
	assert.NotNil(t, GlobalResilienceManager)
	assert.NotNil(t, GlobalResilienceInterceptor)

	mgr := GetGlobalResilience()
	assert.Same(t, GlobalResilienceManager, mgr)

	ri := GetGlobalResilienceInterceptor()
	assert.Same(t, GlobalResilienceInterceptor, ri)
}


