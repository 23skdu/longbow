package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDefaultRetryPolicy runs the test default retry policy operation.
func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	assert.Equal(t, 3, p.MaxAttempts)
	assert.Equal(t, 100*time.Millisecond, p.InitialDelay)
	assert.Equal(t, 30*time.Second, p.MaxDelay)
	assert.Equal(t, 2.0, p.Multiplier)
	assert.True(t, p.Jitter)
	assert.NotNil(t, p.RetryableFunc)
}

// TestDefaultRetryableFunc runs the test default retryable func operation.
func TestDefaultRetryableFunc(t *testing.T) {
	assert.True(t, DefaultRetryableFunc(errors.New("connection refused")))
	assert.True(t, DefaultRetryableFunc(errors.New("timeout occurred")))
	assert.True(t, DefaultRetryableFunc(errors.New("service unavailable")))
	assert.True(t, DefaultRetryableFunc(errors.New("deadline exceeded")))
	assert.False(t, DefaultRetryableFunc(errors.New("permission denied")))
	assert.False(t, DefaultRetryableFunc(nil))
}

// TestContains runs the test contains operation.
func TestContains(t *testing.T) {
	assert.True(t, contains("hello world", "world"))
	assert.True(t, contains("hello", "hello"))
	assert.True(t, contains("hello world", "hello"))
	assert.False(t, contains("hello", "xyz"))
	assert.False(t, contains("ab", "abc"))
}

// TestFindSubstring runs the test find substring operation.
func TestFindSubstring(t *testing.T) {
	assert.True(t, findSubstring("hello world", "lo wo"))
	assert.True(t, findSubstring("abcde", "bcd"))
	assert.False(t, findSubstring("abc", "xyz"))
	assert.False(t, findSubstring("a", "ab"))
}

// TestRetrySuccess runs the test retry success operation.
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

// TestRetrySuccessAfterFailure runs the test retry success after failure operation.
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

// TestRetryAllFail runs the test retry all fail operation.
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

// TestRetryNonRetryable runs the test retry non retryable operation.
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

// TestRetryNilPolicy runs the test retry nil policy operation.
func TestRetryNilPolicy(t *testing.T) {
	ctx := context.Background()
	result, err := Retry(ctx, nil, func() (string, error) {
		return "defaults", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "defaults", result)
}

// TestRetryContextCancelled runs the test retry context cancelled operation.
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

// TestCalculateDelay runs the test calculate delay operation.
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

// TestCalculateDelayMax runs the test calculate delay max operation.
func TestCalculateDelayMax(t *testing.T) {
	policy := &RetryPolicy{
		InitialDelay: 1 * time.Second,
		MaxDelay:     3 * time.Second,
		Multiplier:   4.0,
	}

	delay := calculateDelay(policy, 3)
	assert.Equal(t, 3*time.Second, delay)
}

// TestNewRetryConfig runs the test new retry config operation.
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

// TestRetryableError runs the test retryable error operation.
func TestRetryableError(t *testing.T) {
	inner := errors.New("inner")
	err := &RetryableError{Err: inner}
	assert.Equal(t, "inner", err.Error())
	assert.Equal(t, inner, err.Unwrap())
}

// TestNonRetryableError runs the test non retryable error operation.
func TestNonRetryableError(t *testing.T) {
	inner := errors.New("inner")
	err := &NonRetryableError{Err: inner}
	assert.Equal(t, "inner", err.Error())
	assert.Equal(t, inner, err.Unwrap())
}

// TestNewAdaptiveRetryPolicy runs the test new adaptive retry policy operation.
func TestNewAdaptiveRetryPolicy(t *testing.T) {
	base := DefaultRetryPolicy()
	adaptive := NewAdaptiveRetryPolicy(base)
	assert.Equal(t, base.MaxAttempts, adaptive.MaxAttempts)
	assert.Equal(t, base.InitialDelay, adaptive.InitialDelay)
}

// TestNewAdaptiveRetryPolicyNil runs the test new adaptive retry policy nil operation.
func TestNewAdaptiveRetryPolicyNil(t *testing.T) {
	adaptive := NewAdaptiveRetryPolicy(nil)
	assert.NotNil(t, adaptive)
	assert.Equal(t, 3, adaptive.MaxAttempts)
}

// TestAdaptiveRetryPolicyRecordSuccess runs the test adaptive retry policy record success operation.
func TestAdaptiveRetryPolicyRecordSuccess(t *testing.T) {
	adaptive := NewAdaptiveRetryPolicy(nil)
	adaptive.RecordSuccess()
	assert.NotPanics(t, adaptive.RecordSuccess)
}

// TestAdaptiveRetryPolicyRecordFailure runs the test adaptive retry policy record failure operation.
func TestAdaptiveRetryPolicyRecordFailure(t *testing.T) {
	adaptive := NewAdaptiveRetryPolicy(nil)
	adaptive.RecordFailure()
	assert.NotPanics(t, adaptive.RecordFailure)
}

// TestMinInt runs the test min int operation.
func TestMinInt(t *testing.T) {
	assert.Equal(t, 1, minInt(1, 2))
	assert.Equal(t, 1, minInt(2, 1))
	assert.Equal(t, 0, minInt(0, 5))
}

// TestMaxInt runs the test max int operation.
func TestMaxInt(t *testing.T) {
	assert.Equal(t, 2, maxInt(1, 2))
	assert.Equal(t, 2, maxInt(2, 1))
	assert.Equal(t, 5, maxInt(5, 0))
}

// TestRetryOnRetryCallback runs the test retry on retry callback operation.
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
