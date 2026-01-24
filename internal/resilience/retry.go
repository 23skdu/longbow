package resilience

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

type RetryPolicy struct {
	MaxAttempts   int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	Multiplier    float64
	Jitter        bool
	RetryableFunc func(error) bool
	OnRetry       func(attempt int, err error)
}

func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts:   3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      30 * time.Second,
		Multiplier:    2.0,
		Jitter:        true,
		RetryableFunc: DefaultRetryableFunc,
		OnRetry:       nil,
	}
}

func DefaultRetryableFunc(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	retryableErrors := []string{
		"connection refused",
		"connection reset",
		"timeout",
		"deadline exceeded",
		"temporary failure",
		"service unavailable",
		"resource temporarily unavailable",
		"network is unreachable",
		"no such host",
		"connection timed out",
		"read: connection reset",
		"write: connection reset",
	}

	for _, retryableErr := range retryableErrors {
		if contains(errStr, retryableErr) {
			return true
		}
	}

	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func Retry[T any](ctx context.Context, policy *RetryPolicy, fn func() (T, error)) (T, error) {
	var result T
	var lastErr error

	if policy == nil {
		policy = DefaultRetryPolicy()
	}

	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		if attempt > 0 {
			delay := calculateDelay(policy, attempt)

			if policy.Jitter {
				delay = time.Duration(float64(delay) * (0.8 + 0.4*rand.Float64()))
			}

			select {
			case <-ctx.Done():
				return result, fmt.Errorf("retry cancelled: %w", ctx.Err())
			case <-time.After(delay):
			}
		}

		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err

		if policy.RetryableFunc != nil && !policy.RetryableFunc(err) {
			break
		}

		if policy.OnRetry != nil {
			policy.OnRetry(attempt+1, err)
		}
	}

	return result, lastErr
}

func calculateDelay(policy *RetryPolicy, attempt int) time.Duration {
	if attempt <= 0 {
		return policy.InitialDelay
	}

	delay := float64(policy.InitialDelay) * math.Pow(policy.Multiplier, float64(attempt-1))

	if delay > float64(policy.MaxDelay) {
		delay = float64(policy.MaxDelay)
	}

	return time.Duration(delay)
}

type RetryConfig struct {
	Network     *RetryPolicy
	Storage     *RetryPolicy
	Search      *RetryPolicy
	Replication *RetryPolicy
}

func NewRetryConfig() *RetryConfig {
	return &RetryConfig{
		Network: &RetryPolicy{
			MaxAttempts:   5,
			InitialDelay:  50 * time.Millisecond,
			MaxDelay:      10 * time.Second,
			Multiplier:    2.0,
			Jitter:        true,
			RetryableFunc: DefaultRetryableFunc,
		},
		Storage: &RetryPolicy{
			MaxAttempts:   3,
			InitialDelay:  200 * time.Millisecond,
			MaxDelay:      5 * time.Second,
			Multiplier:    1.5,
			Jitter:        true,
			RetryableFunc: DefaultRetryableFunc,
		},
		Search: &RetryPolicy{
			MaxAttempts:   2,
			InitialDelay:  10 * time.Millisecond,
			MaxDelay:      1 * time.Second,
			Multiplier:    2.0,
			Jitter:        false,
			RetryableFunc: DefaultRetryableFunc,
		},
		Replication: &RetryPolicy{
			MaxAttempts:   10,
			InitialDelay:  500 * time.Millisecond,
			MaxDelay:      30 * time.Second,
			Multiplier:    1.5,
			Jitter:        true,
			RetryableFunc: DefaultRetryableFunc,
		},
	}
}

type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string {
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

type AdaptiveRetryPolicy struct {
	*RetryPolicy
	mu               sync.RWMutex
	successCount     int64
	failureCount     int64
	lastAdjustment   time.Time
	adjustmentPeriod time.Duration
}

func NewAdaptiveRetryPolicy(base *RetryPolicy) *AdaptiveRetryPolicy {
	if base == nil {
		base = DefaultRetryPolicy()
	}

	return &AdaptiveRetryPolicy{
		RetryPolicy:      base,
		adjustmentPeriod: 5 * time.Minute,
		lastAdjustment:   time.Now(),
	}
}

func (a *AdaptiveRetryPolicy) RecordSuccess() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.successCount++
	a.tryAdjust()
}

func (a *AdaptiveRetryPolicy) RecordFailure() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failureCount++
	a.tryAdjust()
}

func (a *AdaptiveRetryPolicy) tryAdjust() {
	now := time.Now()
	if now.Sub(a.lastAdjustment) < a.adjustmentPeriod {
		return
	}

	total := a.successCount + a.failureCount
	if total < 10 {
		return
	}

	failureRate := float64(a.failureCount) / float64(total)

	if failureRate > 0.3 {
		a.MaxAttempts = minInt(a.MaxAttempts+1, 10)
		newDelay := a.InitialDelay * 2
		maxDelay := a.MaxDelay / 4
		if newDelay < maxDelay {
			a.InitialDelay = newDelay
		} else {
			a.InitialDelay = maxDelay
		}
	} else if failureRate < 0.05 {
		a.MaxAttempts = maxInt(a.MaxAttempts-1, 2)
		newDelay := a.InitialDelay / 2
		minDelay := 10 * time.Millisecond
		if newDelay > minDelay {
			a.InitialDelay = newDelay
		} else {
			a.InitialDelay = minDelay
		}
	}

	a.successCount = 0
	a.failureCount = 0
	a.lastAdjustment = now
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
