package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{Name: "test"})
	assert.Equal(t, "test", cb.Name())
	assert.Equal(t, StateClosed, cb.State())
	assert.NotNil(t, cb)
}

func TestCircuitBreakerDefaults(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{Name: "defaults"})
	assert.Equal(t, uint32(1), cb.maxRequests)
	assert.Equal(t, 60*time.Second, cb.interval)
	assert.Equal(t, 60*time.Second, cb.timeout)
	assert.NotNil(t, cb.readyToTrip)
}

func TestCircuitBreakerDefaultReadyToTrip(t *testing.T) {
	assert.True(t, DefaultReadyToTrip(CircuitBreakerMetrics{ConsecutiveFailures: 6}))
	assert.False(t, DefaultReadyToTrip(CircuitBreakerMetrics{ConsecutiveFailures: 5}))
	assert.True(t, DefaultReadyToTrip(CircuitBreakerMetrics{Failures: 11, Requests: 15}))
	assert.False(t, DefaultReadyToTrip(CircuitBreakerMetrics{Failures: 6, Requests: 10}))
}

func TestCircuitBreakerExecuteSuccess(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{Name: "success"})
	result, err := cb.Execute(context.Background(), func() (any, error) {
		return "ok", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "ok", result)
	assert.Equal(t, StateClosed, cb.State())
}

func TestCircuitBreakerExecuteFailure(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{
		Name:        "failure",
		MaxRequests: 1,
		ReadyToTrip: func(counts CircuitBreakerMetrics) bool {
			return counts.Failures >= 2
		},
	})

	_, err := cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})
	assert.Error(t, err)
	assert.Equal(t, StateClosed, cb.State())

	_, err = cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})
	assert.Error(t, err)
	assert.Equal(t, StateOpen, cb.State())
}

func TestCircuitBreakerOpenState(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{
		Name:        "open-test",
		MaxRequests: 1,
		Timeout:     50 * time.Millisecond,
		ReadyToTrip: func(counts CircuitBreakerMetrics) bool {
			return counts.Failures >= 2
		},
	})

	cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})
	cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})
	assert.Equal(t, StateOpen, cb.State())

	_, err := cb.Execute(context.Background(), func() (any, error) {
		return "should-not-reach", nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "circuit breaker")
}

func TestCircuitBreakerHalfOpenToClosed(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{
		Name:        "half-open",
		MaxRequests: 1,
		Timeout:     50 * time.Millisecond,
		ReadyToTrip: func(counts CircuitBreakerMetrics) bool {
			return counts.Failures >= 1
		},
	})

	cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})
	assert.Equal(t, StateOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	result, err := cb.Execute(context.Background(), func() (any, error) {
		return "recovered", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "recovered", result)
	assert.Equal(t, StateClosed, cb.State())
}

func TestCircuitBreakerHalfOpenToOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{
		Name:        "half-open-fail",
		MaxRequests: 1,
		Timeout:     50 * time.Millisecond,
		ReadyToTrip: func(counts CircuitBreakerMetrics) bool {
			return counts.Failures >= 2
		},
	})

	cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})
	cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})
	assert.Equal(t, StateOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("still fail")
	})
	assert.Equal(t, StateOpen, cb.State())
}

func TestCircuitBreakerReset(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{
		Name:        "reset-test",
		MaxRequests: 1,
		ReadyToTrip: func(counts CircuitBreakerMetrics) bool {
			return counts.Failures >= 1
		},
	})

	cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})
	assert.Equal(t, StateOpen, cb.State())

	cb.Reset()
	assert.Equal(t, StateClosed, cb.State())
	assert.Equal(t, uint64(0), cb.Metrics().Failures)
}

func TestCircuitBreakerMetrics(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{Name: "metrics-test"})

	cb.Execute(context.Background(), func() (any, error) {
		return "ok", nil
	})
	cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})

	metrics := cb.Metrics()
	assert.Equal(t, uint64(2), metrics.Requests)
	assert.Equal(t, uint64(1), metrics.Successes)
	assert.Equal(t, uint64(1), metrics.Failures)
}

func TestExecuteWithCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{Name: "generic"})
	result, err := ExecuteWithCircuitBreaker(cb, context.Background(), func() (string, error) {
		return "hello", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func TestExecuteWithCircuitBreakerErrorPath(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerSettings{
		Name:        "open-for-generic",
		MaxRequests: 1,
		Timeout:     50 * time.Millisecond,
		ReadyToTrip: func(counts CircuitBreakerMetrics) bool {
			return counts.Failures >= 1
		},
	})

	ExecuteWithCircuitBreaker(cb, context.Background(), func() (string, error) {
		return "", errors.New("fail")
	})

	_, err := ExecuteWithCircuitBreaker(cb, context.Background(), func() (string, error) {
		return "should-fail", nil
	})
	assert.Error(t, err)
}

func TestCircuitBreakerOnStateChange(t *testing.T) {
	var mu sync.Mutex
	var changed []string
	cb := NewCircuitBreaker(CircuitBreakerSettings{
		Name:        "state-change",
		MaxRequests: 1,
		Timeout:     50 * time.Millisecond,
		ReadyToTrip: func(counts CircuitBreakerMetrics) bool {
			return counts.Failures >= 1
		},
		OnStateChange: func(name string, from, to CircuitState) {
			mu.Lock()
			changed = append(changed, "changed")
			mu.Unlock()
		},
	})

	cb.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})
	mu.Lock()
	assert.GreaterOrEqual(t, len(changed), 1)
	mu.Unlock()
}

func TestCircuitBreakerGroup(t *testing.T) {
	cbg := NewCircuitBreakerGroup(CircuitBreakerSettings{Name: "group"})
	assert.NotNil(t, cbg)

	breaker := cbg.GetBreaker("svc1")
	assert.Equal(t, "svc1", breaker.Name())

	same := cbg.GetBreaker("svc1")
	assert.Same(t, breaker, same)

	breaker2 := cbg.GetBreaker("svc2")
	assert.NotSame(t, breaker, breaker2)
}

func TestCircuitBreakerGroupExecute(t *testing.T) {
	cbg := NewCircuitBreakerGroup(CircuitBreakerSettings{Name: "group-exec"})
	result, err := cbg.Execute(context.Background(), "test", func() (any, error) {
		return "result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "result", result)
}

func TestExecuteWithCircuitBreakerGroup(t *testing.T) {
	cbg := NewCircuitBreakerGroup(CircuitBreakerSettings{Name: "group-gen"})
	result, err := ExecuteWithCircuitBreakerGroup(cbg, context.Background(), "svc", func() (string, error) {
		return "data", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "data", result)
}

func TestCircuitBreakerGroupGetAllMetrics(t *testing.T) {
	cbg := NewCircuitBreakerGroup(CircuitBreakerSettings{Name: "group-metrics"})
	cbg.GetBreaker("a")
	cbg.GetBreaker("b")

	metrics := cbg.GetAllMetrics()
	assert.Len(t, metrics, 2)
	assert.Contains(t, metrics, "a")
	assert.Contains(t, metrics, "b")
}

func TestCircuitBreakerGroupResetAll(t *testing.T) {
	cbg := NewCircuitBreakerGroup(CircuitBreakerSettings{Name: "group-reset"})
	breaker := cbg.GetBreaker("x")

	breaker.Execute(context.Background(), func() (any, error) {
		return nil, errors.New("fail")
	})

	cbg.ResetAll()
	assert.Equal(t, uint64(0), breaker.Metrics().Failures)
}

func TestCircuitBreakerGroupRemoveBreaker(t *testing.T) {
	cbg := NewCircuitBreakerGroup(CircuitBreakerSettings{Name: "group-remove"})
	cbg.GetBreaker("temp")
	cbg.RemoveBreaker("temp")
	metrics := cbg.GetAllMetrics()
	assert.NotContains(t, metrics, "temp")
}

func TestCircuitStateValues(t *testing.T) {
	assert.Equal(t, CircuitState(0), StateClosed)
	assert.Equal(t, CircuitState(1), StateHalfOpen)
	assert.Equal(t, CircuitState(2), StateOpen)
}
