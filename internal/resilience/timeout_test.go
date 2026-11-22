package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDefaultTimeoutConfig runs the test default timeout config operation.
func TestDefaultTimeoutConfig(t *testing.T) {
	cfg := DefaultTimeoutConfig()
	assert.Equal(t, 30*time.Second, cfg.DefaultTimeout)
	assert.Equal(t, 10*time.Second, cfg.NetworkTimeout)
	assert.Equal(t, 60*time.Second, cfg.StorageTimeout)
	assert.Equal(t, 5*time.Second, cfg.SearchTimeout)
	assert.Equal(t, 120*time.Second, cfg.ReplicationTimeout)
	assert.Equal(t, 1*time.Second, cfg.GracePeriod)
}

// TestNewTimeoutManager runs the test new timeout manager operation.
func TestNewTimeoutManager(t *testing.T) {
	tm := NewTimeoutManager(nil)
	assert.NotNil(t, tm)
	assert.NotNil(t, tm.config)
}

// TestGetTimeout runs the test get timeout operation.
func TestGetTimeout(t *testing.T) {
	tm := NewTimeoutManager(DefaultTimeoutConfig())
	assert.Equal(t, 10*time.Second, tm.GetTimeout("network"))
	assert.Equal(t, 60*time.Second, tm.GetTimeout("storage"))
	assert.Equal(t, 5*time.Second, tm.GetTimeout("search"))
	assert.Equal(t, 120*time.Second, tm.GetTimeout("replication"))
	assert.Equal(t, 30*time.Second, tm.GetTimeout("unknown"))
}

// TestWithTimeout runs the test with timeout operation.
func TestWithTimeout(t *testing.T) {
	tm := NewTimeoutManager(DefaultTimeoutConfig())
	err := tm.WithTimeout(context.Background(), "network", func(ctx context.Context) error {
		return nil
	})
	assert.NoError(t, err)
}

// TestWithTimeoutError runs the test with timeout error operation.
func TestWithTimeoutError(t *testing.T) {
	tm := NewTimeoutManager(DefaultTimeoutConfig())
	err := tm.WithTimeout(context.Background(), "search", func(ctx context.Context) error {
		return errors.New("op failed")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "op failed")
}

// TestWithTimeoutExpired runs the test with timeout expired operation.
func TestWithTimeoutExpired(t *testing.T) {
	tm := NewTimeoutManager(&TimeoutConfig{
		SearchTimeout: 1 * time.Millisecond,
	})

	err := tm.WithTimeout(context.Background(), "search", func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

// TestWithCustomTimeout runs the test with custom timeout operation.
func TestWithCustomTimeout(t *testing.T) {
	tm := NewTimeoutManager(nil)
	err := tm.WithCustomTimeout(context.Background(), time.Minute, func(ctx context.Context) error {
		return errors.New("custom err")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "custom err")
}

// TestWithTimeoutGraceful runs the test with timeout graceful operation.
func TestWithTimeoutGraceful(t *testing.T) {
	tm := NewTimeoutManager(&TimeoutConfig{
		SearchTimeout: 20 * time.Millisecond,
		GracePeriod:   10 * time.Millisecond,
	})

	err := tm.WithTimeoutGraceful(context.Background(), "search", func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	if err != nil {
		assert.Contains(t, err.Error(), "timed out")
	}
}

// TestUpdateConfig runs the test update config operation.
func TestUpdateConfig(t *testing.T) {
	tm := NewTimeoutManager(nil)
	newCfg := &TimeoutConfig{DefaultTimeout: 5 * time.Second}
	tm.UpdateConfig(newCfg)
	assert.Equal(t, 5*time.Second, tm.GetTimeout("unknown"))
}

// TestNewAdaptiveTimeout runs the test new adaptive timeout operation.
func TestNewAdaptiveTimeout(t *testing.T) {
	at := NewAdaptiveTimeout(10*time.Second, 1*time.Second, 30*time.Second)
	assert.NotNil(t, at)
	assert.Equal(t, 10*time.Second, at.GetTimeout())
}

// TestAdaptiveTimeoutGetTimeoutNoMeasurements runs the test adaptive timeout get timeout no measurements operation.
func TestAdaptiveTimeoutGetTimeoutNoMeasurements(t *testing.T) {
	at := NewAdaptiveTimeout(5*time.Second, 1*time.Second, 30*time.Second)
	assert.Equal(t, 5*time.Second, at.GetTimeout())
}

// TestAdaptiveTimeoutRecordAndGet runs the test adaptive timeout record and get operation.
func TestAdaptiveTimeoutRecordAndGet(t *testing.T) {
	at := NewAdaptiveTimeout(10*time.Second, 1*time.Second, 30*time.Second)
	at.RecordDuration(1 * time.Second)
	at.RecordDuration(2 * time.Second)
	at.RecordDuration(3 * time.Second)

	timeout := at.GetTimeout()
	assert.Greater(t, timeout, 1*time.Second)
	assert.LessOrEqual(t, timeout, 30*time.Second)
}

// TestAdaptiveTimeoutMinClamp runs the test adaptive timeout min clamp operation.
func TestAdaptiveTimeoutMinClamp(t *testing.T) {
	at := NewAdaptiveTimeout(10*time.Second, 5*time.Second, 30*time.Second)
	at.RecordDuration(1 * time.Nanosecond)
	timeout := at.GetTimeout()
	assert.GreaterOrEqual(t, timeout, 5*time.Second)
}

// TestAdaptiveTimeoutMaxClamp runs the test adaptive timeout max clamp operation.
func TestAdaptiveTimeoutMaxClamp(t *testing.T) {
	at := NewAdaptiveTimeout(1*time.Second, 1*time.Second, 2*time.Second)
	at.RecordDuration(10 * time.Second)
	timeout := at.GetTimeout()
	assert.LessOrEqual(t, timeout, 2*time.Second)
}

// TestAdaptiveTimeoutMaxMeasurements runs the test adaptive timeout max measurements operation.
func TestAdaptiveTimeoutMaxMeasurements(t *testing.T) {
	at := NewAdaptiveTimeout(1*time.Second, 1*time.Second, 30*time.Second)
	for i := 0; i < 200; i++ {
		at.RecordDuration(time.Duration(i) * time.Millisecond)
	}
	assert.LessOrEqual(t, len(at.measurements), 100)
}

// TestPercentile runs the test percentile operation.
func TestPercentile(t *testing.T) {
	at := NewAdaptiveTimeout(1*time.Second, 1*time.Second, 30*time.Second)
	at.RecordDuration(10 * time.Millisecond)
	at.RecordDuration(20 * time.Millisecond)
	at.RecordDuration(30 * time.Millisecond)

	p := at.percentile(0.5)
	assert.Equal(t, 20*time.Millisecond, p)

	p = at.percentile(1.0)
	assert.Equal(t, 30*time.Millisecond, p)
}

// TestPercentileEmpty runs the test percentile empty operation.
func TestPercentileEmpty(t *testing.T) {
	at := NewAdaptiveTimeout(1*time.Second, 1*time.Second, 30*time.Second)
	assert.Equal(t, time.Duration(0), at.percentile(0.5))
}

// TestNewTimeoutGroup runs the test new timeout group operation.
func TestNewTimeoutGroup(t *testing.T) {
	tg := NewTimeoutGroup()
	assert.NotNil(t, tg)
}

// TestTimeoutGroupGetManager runs the test timeout group get manager operation.
func TestTimeoutGroupGetManager(t *testing.T) {
	tg := NewTimeoutGroup()
	mgr := tg.GetManager("test")
	assert.NotNil(t, mgr)

	same := tg.GetManager("test")
	assert.Same(t, mgr, same)
}

// TestTimeoutGroupGetAdaptiveTimeout runs the test timeout group get adaptive timeout operation.
func TestTimeoutGroupGetAdaptiveTimeout(t *testing.T) {
	tg := NewTimeoutGroup()
	at := tg.GetAdaptiveTimeout("test", 5*time.Second, 1*time.Second, 30*time.Second)
	assert.NotNil(t, at)

	same := tg.GetAdaptiveTimeout("test", 5*time.Second, 1*time.Second, 30*time.Second)
	assert.Same(t, at, same)
}

// TestTimeoutGroupWithTimeout runs the test timeout group with timeout operation.
func TestTimeoutGroupWithTimeout(t *testing.T) {
	tg := NewTimeoutGroup()
	err := tg.WithTimeout(context.Background(), "mgr", "search", func(ctx context.Context) error {
		return nil
	})
	assert.NoError(t, err)
}

// TestTimeoutGroupWithAdaptiveTimeout runs the test timeout group with adaptive timeout operation.
func TestTimeoutGroupWithAdaptiveTimeout(t *testing.T) {
	tg := NewTimeoutGroup()
	err := tg.WithAdaptiveTimeout(context.Background(), "adapt", func(ctx context.Context) error {
		return errors.New("op err")
	})
	assert.Error(t, err)
}

// TestTimeoutGroupRemoveManager runs the test timeout group remove manager operation.
func TestTimeoutGroupRemoveManager(t *testing.T) {
	tg := NewTimeoutGroup()
	tg.GetManager("temp")
	assert.NotPanics(t, func() { tg.RemoveManager("temp") })
}

// TestTimeoutGroupRemoveAdaptiveTimeout runs the test timeout group remove adaptive timeout operation.
func TestTimeoutGroupRemoveAdaptiveTimeout(t *testing.T) {
	tg := NewTimeoutGroup()
	tg.GetAdaptiveTimeout("temp", 1*time.Second, 100*time.Millisecond, 10*time.Second)
	assert.NotPanics(t, func() { tg.RemoveAdaptiveTimeout("temp") })
}
