package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func FuzzRetryDo(f *testing.F) {
	f.Add(1, 10, 50, 100)
	f.Fuzz(func(t *testing.T, retries, baseDelayMs, maxDelayMs, timeoutMs int) {
		if retries < 0 || retries > 10 || baseDelayMs <= 0 || maxDelayMs <= 0 || timeoutMs <= 0 {
			return
		}

		policy := &ExponentialBackoff{
			BaseDelay:      time.Duration(baseDelayMs) * time.Millisecond,
			MaxDelay:       time.Duration(maxDelayMs) * time.Millisecond,
			Retries:        retries,
			AttemptTimeout: time.Duration(timeoutMs) * time.Millisecond,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		attempts := 0
		err := Do(ctx, policy, func(ctx context.Context) error {
			attempts++
			if attempts <= retries {
				return errors.New("temp error")
			}
			return nil
		})

		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			if attempts <= retries {
				t.Errorf("Unexpected error: %v (attempts: %d)", err, attempts)
			}
		}
	})
}
