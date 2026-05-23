package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExponentialBackoff_Backoff(t *testing.T) {
	b := &ExponentialBackoff{
		BaseDelay: 10 * time.Millisecond,
		MaxDelay:  100 * time.Millisecond,
	}

	// Test exponential growth
	if d := b.Backoff(0); d != 10*time.Millisecond {
		t.Errorf("expected 10ms, got %v", d)
	}
	if d := b.Backoff(1); d != 20*time.Millisecond {
		t.Errorf("expected 20ms, got %v", d)
	}
	if d := b.Backoff(2); d != 40*time.Millisecond {
		t.Errorf("expected 40ms, got %v", d)
	}

	// Test max delay
	if d := b.Backoff(10); d != 100*time.Millisecond {
		t.Errorf("expected 100ms, got %v", d)
	}
}

func TestExponentialBackoff_Jitter(t *testing.T) {
	b := &ExponentialBackoff{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  1 * time.Second,
		Jitter:    0.2,
	}

	// With 20% jitter, 100ms should be between 80ms and 120ms
	for i := 0; i < 100; i++ {
		d := b.Backoff(0)
		if d < 80*time.Millisecond || d > 120*time.Millisecond {
			t.Errorf("jittered delay %v out of range [80ms, 120ms]", d)
		}
	}
}

func TestDo_Success(t *testing.T) {
	policy := &ExponentialBackoff{
		BaseDelay: 1 * time.Millisecond,
		Retries:   3,
	}

	attempts := 0
	err := Do(context.Background(), policy, func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return status.Error(codes.Unavailable, "try again")
		}
		return nil
	})

	if err != nil {
		t.Errorf("expected success, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestDo_MaxRetriesReached(t *testing.T) {
	policy := &ExponentialBackoff{
		BaseDelay: 1 * time.Millisecond,
		Retries:   2,
	}

	attempts := 0
	err := Do(context.Background(), policy, func(ctx context.Context) error {
		attempts++
		return status.Error(codes.Unavailable, "keep failing")
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.Unavailable {
		t.Errorf("expected Unavailable, got %v", err)
	}
	if attempts != 3 { // 1 initial + 2 retries
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestDo_NonRetryableError(t *testing.T) {
	policy := &ExponentialBackoff{
		BaseDelay: 1 * time.Millisecond,
		Retries:   3,
	}

	attempts := 0
	err := Do(context.Background(), policy, func(ctx context.Context) error {
		attempts++
		return status.Error(codes.InvalidArgument, "don't retry me")
	})

	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}

func TestDo_ContextCancelled(t *testing.T) {
	policy := &ExponentialBackoff{
		BaseDelay: 10 * time.Second,
		Retries:   5,
	}

	ctx, cancel := context.WithCancel(context.Background())
	err := Do(ctx, policy, func(ctx context.Context) error {
		cancel()
		return status.Error(codes.Unavailable, "cancelled")
	})

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestDo_AttemptTimeout(t *testing.T) {
	policy := &ExponentialBackoff{
		AttemptTimeout: 50 * time.Millisecond,
		Retries:        1,
	}

	err := Do(context.Background(), policy, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	})

	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestExponentialBackoff_Interface(t *testing.T) {
	policy := &ExponentialBackoff{
		Retries:        5,
		AttemptTimeout: 10 * time.Second,
	}
	if policy.MaxRetries() != 5 {
		t.Errorf("expected 5, got %d", policy.MaxRetries())
	}
	if policy.Timeout() != 10*time.Second {
		t.Errorf("expected 10s, got %v", policy.Timeout())
	}
	if b := policy.Backoff(-1); b != 0 {
		t.Errorf("expected 0 for negative attempt, got %v", b)
	}
}

func TestDo_InitialContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Do(ctx, &ExponentialBackoff{}, func(ctx context.Context) error {
		return nil
	})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestDo_ContextCancelledDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	policy := &ExponentialBackoff{
		BaseDelay: 10 * time.Second, // Long wait
		Retries:   1,
	}

	started := make(chan struct{})
	go func() {
		<-started
		cancel()
		time.Sleep(20 * time.Millisecond) // Give time for context propagation
	}()

	err := Do(ctx, policy, func(ctx context.Context) error {
		select {
		case started <- struct{}{}:
		default:
		}
		time.Sleep(50 * time.Millisecond) // Wait for cancel to propagate
		return status.Error(codes.Unavailable, "retry me")
	})

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestDo_RetryExhausted_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	policy := &ExponentialBackoff{
		Retries: 0,
	}

	err := Do(ctx, policy, func(ctx context.Context) error {
		cancel()
		time.Sleep(20 * time.Millisecond)
		return errors.New("last error")
	})

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestIsRetryable(t *testing.T) {
	policy := &ExponentialBackoff{}

	cases := []struct {
		err      error
		expected bool
	}{
		{nil, false},
		{errors.New("generic"), true},
		{status.Error(codes.Unavailable, "u"), true},
		{status.Error(codes.DeadlineExceeded, "d"), true},
		{status.Error(codes.Internal, "i"), true},
		{status.Error(codes.InvalidArgument, "v"), false},
		{status.Error(codes.NotFound, "n"), false},
		{context.DeadlineExceeded, true},
		{context.Canceled, true},
	}

	for _, c := range cases {
		if res := policy.IsRetryable(c.err); res != c.expected {
			t.Errorf("IsRetryable(%v) = %v; expected %v", c.err, res, c.expected)
		}
	}
}
