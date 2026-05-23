package retry

import (
	"context"
	"math/rand"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryPolicy defines the interface for backoff strategies.
type RetryPolicy interface {
	// Backoff returns the duration to wait before the next attempt.
	// attempt is 0-indexed.
	Backoff(attempt int) time.Duration
	// MaxRetries returns the maximum number of retry attempts.
	MaxRetries() int
	// Timeout returns the timeout for a single attempt.
	Timeout() time.Duration
	// IsRetryable returns true if the error should be retried.
	IsRetryable(err error) bool
}

// ExponentialBackoff implements a standard exponential backoff with jitter.
type ExponentialBackoff struct {
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	Retries        int
	AttemptTimeout time.Duration
	// Jitter factor (0.0 to 1.0)
	Jitter float64
}

func (b *ExponentialBackoff) Backoff(attempt int) time.Duration {
	if attempt < 0 {
		return 0
	}

	// delay = BaseDelay * 2^attempt
	delay := b.BaseDelay * (1 << uint(attempt))
	if delay > b.MaxDelay {
		delay = b.MaxDelay
	}

	if b.Jitter > 0 {
		jitter := time.Duration(rand.Float64() * b.Jitter * float64(delay)) // #nosec G404
		if rand.Intn(2) == 0 {                                              // #nosec G404
			delay -= jitter
		} else {
			delay += jitter
		}
	}

	return delay
}

func (b *ExponentialBackoff) MaxRetries() int {
	return b.Retries
}

func (b *ExponentialBackoff) Timeout() time.Duration {
	return b.AttemptTimeout
}

func (b *ExponentialBackoff) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	s, ok := status.FromError(err)
	if !ok {
		// Non-gRPC error, treat as retryable by default (e.g. errors.New)
		return true
	}

	switch s.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal:
		return true
	default:
		return false
	}
}

// Do executes a function with retries according to the policy.
func Do(ctx context.Context, policy RetryPolicy, fn func(context.Context) error) error {
	var lastErr error

	for attempt := 0; attempt <= policy.MaxRetries(); attempt++ {
		// Check global context
		if err := ctx.Err(); err != nil {
			return err
		}

		// Prepare attempt context
		var attemptCtx context.Context
		var cancel context.CancelFunc
		if policy.Timeout() > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, policy.Timeout())
		} else {
			attemptCtx = ctx
		}

		lastErr = fn(attemptCtx)
		if cancel != nil {
			cancel()
		}

		if lastErr == nil {
			return nil
		}

		// Always check if the parent context was cancelled, regardless of what fn returned.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if !policy.IsRetryable(lastErr) {
			return lastErr
		}

		if attempt < policy.MaxRetries() {
			// Wait before next attempt
			wait := policy.Backoff(attempt)
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
			timer.Stop()
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	return lastErr
}
