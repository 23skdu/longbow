package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNewGracefulDegradation runs the test new graceful degradation operation.
func TestNewGracefulDegradation(t *testing.T) {
	gd := NewGracefulDegradation()
	assert.NotNil(t, gd)
	assert.Equal(t, DegradationNone, gd.GetCurrentLevel())
}

// TestAddStrategy runs the test add strategy operation.
func TestAddStrategy(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddStrategy("cache", DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return "cached", nil
	}, true)

	stats := gd.GetStats()
	strategies := stats["strategies"].(map[string]interface{})
	assert.Contains(t, strategies, "cache")
}

// TestAddHealthCheck runs the test add health check operation.
func TestAddHealthCheck(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddHealthCheck("db", func() error { return nil })
	stats := gd.GetStats()
	assert.Equal(t, 1, stats["health_checks"])
}

// TestExecutePrimaryOnly runs the test execute primary only operation.
func TestExecutePrimaryOnly(t *testing.T) {
	gd := NewGracefulDegradation()
	result, err := gd.Execute(context.Background(), func() (any, error) {
		return "primary", nil
	}, "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, "primary", result)
}

// TestExecuteFallbackDisabled runs the test execute fallback disabled operation.
func TestExecuteFallbackDisabled(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddStrategy("fallback", DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return "fallback-data", nil
	}, false)

	result, err := gd.Execute(context.Background(), func() (any, error) {
		return "primary", nil
	}, "fallback")
	assert.NoError(t, err)
	assert.Equal(t, "primary", result)
}

// TestExecuteFallbackUsed runs the test execute fallback used operation.
func TestExecuteFallbackUsed(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddStrategy("fallback", DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return "fallback-data", nil
	}, true)
	gd.SetLevel(DegradationSevere)

	result, err := gd.Execute(context.Background(), func() (any, error) {
		return "primary", nil
	}, "fallback")
	assert.NoError(t, err)
	assert.Equal(t, "fallback-data", result)
}

// TestExecuteFallbackLevelNotReached runs the test execute fallback level not reached operation.
func TestExecuteFallbackLevelNotReached(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddStrategy("fallback", DegradationSevere, func(ctx context.Context) (interface{}, error) {
		return "fallback-data", nil
	}, true)
	gd.SetLevel(DegradationMinimal)

	result, err := gd.Execute(context.Background(), func() (any, error) {
		return "primary", nil
	}, "fallback")
	assert.NoError(t, err)
	assert.Equal(t, "primary", result)
}

// TestExecuteFallbackError runs the test execute fallback error operation.
func TestExecuteFallbackError(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddStrategy("fallback", DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return nil, errors.New("fallback also failed")
	}, true)
	gd.SetLevel(DegradationSevere)

	result, err := gd.Execute(context.Background(), func() (any, error) {
		return "primary", nil
	}, "fallback")
	assert.NoError(t, err)
	assert.Equal(t, "primary", result)
}

// TestExecuteWithGracefulDegradation runs the test execute with graceful degradation operation.
func TestExecuteWithGracefulDegradation(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddStrategy("stale", DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return "stale-data", nil
	}, true)
	gd.SetLevel(DegradationModerate)

	result, err := ExecuteWithGracefulDegradation(gd, context.Background(), func() (string, error) {
		return "fresh-data", nil
	}, "stale")
	assert.NoError(t, err)
	assert.Equal(t, "stale-data", result)
}

// TestExecuteWithGracefulDegradationNilResult runs the test execute with graceful degradation nil result operation.
func TestExecuteWithGracefulDegradationNilResult(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddStrategy("nil-fallback", DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return nil, nil
	}, true)
	gd.SetLevel(DegradationModerate)

	result, err := ExecuteWithGracefulDegradation(gd, context.Background(), func() (string, error) {
		return "fresh", nil
	}, "nil-fallback")
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

// TestExecuteWithGracefulDegradationTypeAssertion runs the test execute with graceful degradation type assertion operation.
func TestExecuteWithGracefulDegradationTypeAssertion(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddStrategy("cache", DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return "cached-value", nil
	}, true)
	gd.SetLevel(DegradationModerate)

	result, err := ExecuteWithGracefulDegradation(gd, context.Background(), func() (string, error) {
		return "live", nil
	}, "cache")
	assert.NoError(t, err)
	assert.Equal(t, "cached-value", result)
}

// TestSetLevel runs the test set level operation.
func TestSetLevel(t *testing.T) {
	gd := NewGracefulDegradation()
	assert.Equal(t, DegradationNone, gd.GetCurrentLevel())

	gd.SetLevel(DegradationModerate)
	assert.Equal(t, DegradationModerate, gd.GetCurrentLevel())

	gd.SetLevel(DegradationNone)
	assert.Equal(t, DegradationNone, gd.GetCurrentLevel())
}

// TestDegradationLevelValues runs the test degradation level values operation.
func TestDegradationLevelValues(t *testing.T) {
	assert.Equal(t, DegradationLevel(0), DegradationNone)
	assert.Equal(t, DegradationLevel(1), DegradationMinimal)
	assert.Equal(t, DegradationLevel(2), DegradationModerate)
	assert.Equal(t, DegradationLevel(3), DegradationSevere)
	assert.Equal(t, DegradationLevel(4), DegradationCritical)
}

// TestAssessHealthNoChecks runs the test assess health no checks operation.
func TestAssessHealthNoChecks(t *testing.T) {
	gd := NewGracefulDegradation()
	assert.Equal(t, DegradationNone, gd.AssessHealth())
}

// TestAssessHealthAllPass runs the test assess health all pass operation.
func TestAssessHealthAllPass(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddHealthCheck("a", func() error { return nil })
	gd.AddHealthCheck("b", func() error { return nil })
	assert.Equal(t, DegradationNone, gd.AssessHealth())
}

// TestAssessHealthPartialFail runs the test assess health partial fail operation.
func TestAssessHealthPartialFail(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddHealthCheck("a", func() error { return nil })
	gd.AddHealthCheck("b", func() error { return errors.New("down") })
	level := gd.AssessHealth()
	assert.Equal(t, DegradationSevere, level)
}

// TestAssessHealthAllFail runs the test assess health all fail operation.
func TestAssessHealthAllFail(t *testing.T) {
	gd := NewGracefulDegradation()
	gd.AddHealthCheck("a", func() error { return errors.New("down") })
	gd.AddHealthCheck("b", func() error { return errors.New("down") })
	level := gd.AssessHealth()
	assert.Equal(t, DegradationCritical, level)
}

// TestGetStats runs the test get stats operation.
func TestGetStats(t *testing.T) {
	gd := NewGracefulDegradation()
	stats := gd.GetStats()
	assert.Contains(t, stats, "current_level")
	assert.Contains(t, stats, "strategies")
	assert.Contains(t, stats, "health_checks")
	assert.Contains(t, stats, "degraded_since")
	assert.Contains(t, stats, "last_level_change")
}

// TestNewFallbackCache runs the test new fallback cache operation.
func TestNewFallbackCache(t *testing.T) {
	fc := NewFallbackCache(time.Minute)
	assert.NotNil(t, fc)
	assert.Equal(t, 0, fc.Size())
}

// TestFallbackCacheGetSet runs the test fallback cache get set operation.
func TestFallbackCacheGetSet(t *testing.T) {
	fc := NewFallbackCache(time.Minute)

	val, found, err := fc.Get("key")
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, val)

	fc.Set("key", "value", true, nil)
	val, found, err = fc.Get("key")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value", val)
}

// TestFallbackCacheExpiry runs the test fallback cache expiry operation.
func TestFallbackCacheExpiry(t *testing.T) {
	fc := NewFallbackCache(10 * time.Millisecond)
	fc.Set("key", "val", false, nil)
	time.Sleep(20 * time.Millisecond)
	val, found, err := fc.Get("key")
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, val)
}

// TestFallbackCacheClear runs the test fallback cache clear operation.
func TestFallbackCacheClear(t *testing.T) {
	fc := NewFallbackCache(time.Minute)
	fc.Set("k", "v", false, nil)
	fc.Clear()
	assert.Equal(t, 0, fc.Size())
}

// TestFallbackCacheSize runs the test fallback cache size operation.
func TestFallbackCacheSize(t *testing.T) {
	fc := NewFallbackCache(time.Minute)
	fc.Set("a", 1, false, nil)
	fc.Set("b", 2, false, nil)
	assert.Equal(t, 2, fc.Size())
}

// TestNewBulkhead runs the test new bulkhead operation.
func TestNewBulkhead(t *testing.T) {
	b := NewBulkhead("test", 5)
	assert.NotNil(t, b)
	assert.Equal(t, "test", b.Name())
	assert.Equal(t, 5, b.AvailableSlots())
}

// TestBulkheadExecute runs the test bulkhead execute operation.
func TestBulkheadExecute(t *testing.T) {
	b := NewBulkhead("test", 1)
	result, err := b.Execute(func() (interface{}, error) {
		return "result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "result", result)
}

// TestBulkheadFull runs the test bulkhead full operation.
func TestBulkheadFull(t *testing.T) {
	b := NewBulkhead("test", 1)
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	go func() {
		_, _ = b.Execute(func() (interface{}, error) {
			close(blocked)
			<-unblock
			return nil, nil
		})
	}()
	<-blocked

	_, err := b.Execute(func() (interface{}, error) {
		return "should-fail", nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bulkhead")
	close(unblock)
}

// TestBulkheadExecuteWithContextCancelled runs the test bulkhead execute with context cancelled operation.
func TestBulkheadExecuteWithContextCancelled(t *testing.T) {
	b := NewBulkhead("ctx-cancel", 1)
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	go func() {
		_, _ = b.Execute(func() (interface{}, error) {
			close(blocked)
			<-unblock
			return nil, nil
		})
	}()
	<-blocked

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := b.ExecuteWithContext(ctx, func(ctx context.Context) (interface{}, error) {
		return nil, nil
	})
	if err != nil {
		assert.Contains(t, err.Error(), "context cancelled")
	}
	close(unblock)
}

// TestExecuteWithBulkhead runs the test execute with bulkhead operation.
func TestExecuteWithBulkhead(t *testing.T) {
	b := NewBulkhead("generic", 1)
	result, err := ExecuteWithBulkhead(b, func() (string, error) {
		return "data", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "data", result)
}

// TestBulkheadExecuteWithContext runs the test bulkhead execute with context operation.
func TestBulkheadExecuteWithContext(t *testing.T) {
	b := NewBulkhead("ctx-test", 1)
	result, err := b.ExecuteWithContext(context.Background(), func(ctx context.Context) (interface{}, error) {
		return "ok", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "ok", result)
}

// TestExecuteWithBulkheadAndContext runs the test execute with bulkhead and context operation.
func TestExecuteWithBulkheadAndContext(t *testing.T) {
	b := NewBulkhead("ctx-gen", 1)
	result, err := ExecuteWithBulkheadAndContext(b, context.Background(), func(ctx context.Context) (string, error) {
		return "val", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "val", result)
}

// TestBulkheadGroup runs the test bulkhead group operation.
func TestBulkheadGroup(t *testing.T) {
	bg := NewBulkheadGroup()
	b := bg.GetBulkhead("svc", 5)
	assert.Equal(t, "svc", b.Name())

	same := bg.GetBulkhead("svc", 10)
	assert.Same(t, b, same)
}

// TestBulkheadGroupExecute runs the test bulkhead group execute operation.
func TestBulkheadGroupExecute(t *testing.T) {
	bg := NewBulkheadGroup()
	result, err := bg.Execute("svc", 5, func() (any, error) {
		return "result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "result", result)
}

// TestExecuteWithBulkheadGroup runs the test execute with bulkhead group operation.
func TestExecuteWithBulkheadGroup(t *testing.T) {
	bg := NewBulkheadGroup()
	result, err := ExecuteWithBulkheadGroup(bg, "svc", 5, func() (string, error) {
		return "data", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "data", result)
}

// TestBulkheadGroupRemoveBulkhead runs the test bulkhead group remove bulkhead operation.
func TestBulkheadGroupRemoveBulkhead(t *testing.T) {
	bg := NewBulkheadGroup()
	bg.GetBulkhead("temp", 1)
	assert.NotPanics(t, func() { bg.RemoveBulkhead("temp") })
}

// TestBulkheadAvailableSlots runs the test bulkhead available slots operation.
func TestBulkheadAvailableSlots(t *testing.T) {
	b := NewBulkhead("slots", 3)
	assert.Equal(t, 3, b.AvailableSlots())
}

// TestExecuteWithBulkheadFull runs the test execute with bulkhead full operation.
func TestExecuteWithBulkheadFull(t *testing.T) {
	b := NewBulkhead("generic-full", 1)
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	go func() {
		_, _ = b.Execute(func() (interface{}, error) {
			close(blocked)
			<-unblock
			return nil, nil
		})
	}()
	<-blocked

	_, err := ExecuteWithBulkhead(b, func() (string, error) {
		return "should-not-reach", nil
	})
	assert.Error(t, err)
	close(unblock)
}

// TestExecuteWithBulkheadAndContextFull runs the test execute with bulkhead and context full operation.
func TestExecuteWithBulkheadAndContextFull(t *testing.T) {
	b := NewBulkhead("ctx-gen-full", 1)
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	go func() {
		_, _ = b.Execute(func() (interface{}, error) {
			close(blocked)
			<-unblock
			return nil, nil
		})
	}()
	<-blocked

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := ExecuteWithBulkheadAndContext(b, ctx, func(ctx context.Context) (string, error) {
		return "should-not-reach", nil
	})
	assert.Error(t, err)
	close(unblock)
}
