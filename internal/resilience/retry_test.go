package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	assert.Equal(t, 3, p.MaxAttempts)
	assert.Equal(t, 100*time.Millisecond, p.InitialDelay)
	assert.Equal(t, 30*time.Second, p.MaxDelay)
	assert.Equal(t, 2.0, p.Multiplier)
	assert.True(t, p.Jitter)
	assert.NotNil(t, p.RetryableFunc)
}

func TestDefaultRetryableFunc(t *testing.T) {
	assert.True(t, DefaultRetryableFunc(errors.New("connection refused")))
	assert.True(t, DefaultRetryableFunc(errors.New("timeout occurred")))
	assert.True(t, DefaultRetryableFunc(errors.New("service unavailable")))
	assert.True(t, DefaultRetryableFunc(errors.New("deadline exceeded")))
	assert.False(t, DefaultRetryableFunc(errors.New("permission denied")))
	assert.False(t, DefaultRetryableFunc(nil))
}

func TestContains(t *testing.T) {
	assert.True(t, contains("hello world", "world"))
	assert.True(t, contains("hello", "hello"))
	assert.True(t, contains("hello world", "hello"))
	assert.False(t, contains("hello", "xyz"))
	assert.False(t, contains("ab", "abc"))
}

func TestFindSubstring(t *testing.T) {
	assert.True(t, findSubstring("hello world", "lo wo"))
	assert.True(t, findSubstring("abcde", "bcd"))
	assert.False(t, findSubstring("abc", "xyz"))
	assert.False(t, findSubstring("a", "ab"))
}

func TestRetrySuccess(t *testing.T) {
	ctx := context.Background()
	policy := &RetryPolicy{
		MaxAttempts:   3,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		Multiplier:    1.5,
		Jitter:        false,
		RetryableFunc: DefaultRetryableFunc,
	}

	result, err := Retry(ctx, policy, func() (string, error) {
		return "success", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "success", result)
}

func TestRetrySuccessAfterFailure(t *testing.T) {
	ctx := context.Background()
	var attempts int
	policy := &RetryPolicy{
		MaxAttempts:   3,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		Multiplier:    1.5,
		Jitter:        false,
		RetryableFunc: DefaultRetryableFunc,
	}

	result, err := Retry(ctx, policy, func() (string, error) {
		attempts++
		if attempts < 3 {
			return "", errors.New("connection refused")
		}
		return "ok", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "ok", result)
	assert.Equal(t, 3, attempts)
}

func TestRetryAllFail(t *testing.T) {
	ctx := context.Background()
	policy := &RetryPolicy{
		MaxAttempts:   3,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		Multiplier:    1.5,
		Jitter:        false,
		RetryableFunc: DefaultRetryableFunc,
	}

	_, err := Retry(ctx, policy, func() (string, error) {
		return "", errors.New("connection refused")
	})
	assert.Error(t, err)
}

func TestRetryNonRetryable(t *testing.T) {
	ctx := context.Background()
	policy := &RetryPolicy{
		MaxAttempts:   3,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		Multiplier:    1.5,
		Jitter:        false,
		RetryableFunc: DefaultRetryableFunc,
	}

	_, err := Retry(ctx, policy, func() (string, error) {
		return "", errors.New("permission denied")
	})
	assert.Error(t, err)
}

func TestRetryNilPolicy(t *testing.T) {
	ctx := context.Background()
	result, err := Retry(ctx, nil, func() (string, error) {
		return "defaults", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "defaults", result)
}

func TestRetryContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	policy := &RetryPolicy{
		MaxAttempts:   3,
		InitialDelay:  10 * time.Millisecond,
		MaxDelay:      100 * time.Millisecond,
		Multiplier:    1.0,
		Jitter:        false,
		RetryableFunc: DefaultRetryableFunc,
	}

	_, err := Retry(ctx, policy, func() (string, error) {
		return "", errors.New("connection refused")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cancelled")
}

func TestCalculateDelay(t *testing.T) {
	policy := &RetryPolicy{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		Multiplier:   2.0,
	}

	assert.Equal(t, 100*time.Millisecond, calculateDelay(policy, 0))
	assert.Equal(t, 100*time.Millisecond, calculateDelay(policy, 1))
	assert.Equal(t, 200*time.Millisecond, calculateDelay(policy, 2))
	assert.Equal(t, 400*time.Millisecond, calculateDelay(policy, 3))
}

func TestCalculateDelayMax(t *testing.T) {
	policy := &RetryPolicy{
		InitialDelay: 1 * time.Second,
		MaxDelay:     3 * time.Second,
		Multiplier:   4.0,
	}

	delay := calculateDelay(policy, 3)
	assert.Equal(t, 3*time.Second, delay)
}

func TestNewRetryConfig(t *testing.T) {
	cfg := NewRetryConfig()
	assert.NotNil(t, cfg.Network)
	assert.NotNil(t, cfg.Storage)
	assert.NotNil(t, cfg.Search)
	assert.NotNil(t, cfg.Replication)
	assert.Equal(t, 5, cfg.Network.MaxAttempts)
	assert.Equal(t, 3, cfg.Storage.MaxAttempts)
	assert.Equal(t, 2, cfg.Search.MaxAttempts)
	assert.Equal(t, 10, cfg.Replication.MaxAttempts)
}

func TestRetryableError(t *testing.T) {
	inner := errors.New("inner")
	err := &RetryableError{Err: inner}
	assert.Equal(t, "inner", err.Error())
	assert.Equal(t, inner, err.Unwrap())
}

func TestNonRetryableError(t *testing.T) {
	inner := errors.New("inner")
	err := &NonRetryableError{Err: inner}
	assert.Equal(t, "inner", err.Error())
	assert.Equal(t, inner, err.Unwrap())
}

func TestNewAdaptiveRetryPolicy(t *testing.T) {
	base := DefaultRetryPolicy()
	adaptive := NewAdaptiveRetryPolicy(base)
	assert.Equal(t, base.MaxAttempts, adaptive.MaxAttempts)
	assert.Equal(t, base.InitialDelay, adaptive.InitialDelay)
}

func TestNewAdaptiveRetryPolicyNil(t *testing.T) {
	adaptive := NewAdaptiveRetryPolicy(nil)
	assert.NotNil(t, adaptive)
	assert.Equal(t, 3, adaptive.MaxAttempts)
}

func TestAdaptiveRetryPolicyRecordSuccess(t *testing.T) {
	adaptive := NewAdaptiveRetryPolicy(nil)
	adaptive.RecordSuccess()
	assert.NotPanics(t, adaptive.RecordSuccess)
}

func TestAdaptiveRetryPolicyRecordFailure(t *testing.T) {
	adaptive := NewAdaptiveRetryPolicy(nil)
	adaptive.RecordFailure()
	assert.NotPanics(t, adaptive.RecordFailure)
}

func TestMinInt(t *testing.T) {
	assert.Equal(t, 1, minInt(1, 2))
	assert.Equal(t, 1, minInt(2, 1))
	assert.Equal(t, 0, minInt(0, 5))
}

func TestMaxInt(t *testing.T) {
	assert.Equal(t, 2, maxInt(1, 2))
	assert.Equal(t, 2, maxInt(2, 1))
	assert.Equal(t, 5, maxInt(5, 0))
}

func TestRetryOnRetryCallback(t *testing.T) {
	ctx := context.Background()
	var onRetryCalled bool
	policy := &RetryPolicy{
		MaxAttempts:   2,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		Multiplier:    1.0,
		Jitter:        false,
		RetryableFunc: DefaultRetryableFunc,
		OnRetry: func(attempt int, err error) {
			onRetryCalled = true
		},
	}

	_, err := Retry(ctx, policy, func() (string, error) {
		return "", errors.New("connection refused")
	})
	assert.Error(t, err)
	assert.True(t, onRetryCalled)
}
