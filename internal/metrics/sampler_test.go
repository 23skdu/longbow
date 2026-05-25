package metrics

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestAdaptiveSampler_ShouldSample_AlwaysSample(t *testing.T) {
	sampler := &AdaptiveSampler{
		AlwaysSample: true,
	}

	for i := 0; i < 100; i++ {
		should, mult := sampler.ShouldSample()
		assert.True(t, should)
		assert.Equal(t, 1.0, mult)

		shouldPres, multPres := sampler.ShouldSampleUnderPressure(100_000)
		assert.True(t, shouldPres)
		assert.Equal(t, 1.0, multPres)
	}

	assert.Equal(t, int64(0), sampler.skippedCount.Load())
}

func TestAdaptiveSampler_ShouldSample_NormalLoad(t *testing.T) {
	sampler := &AdaptiveSampler{}
	assert.False(t, sampler.AlwaysSample)

	now := time.Now().UnixNano()

	// First sample should win immediately (lastSampleNs is 0)
	should, mult := sampler.ShouldSample()
	assert.True(t, should)
	assert.Equal(t, 1.0, mult)
	assert.Greater(t, sampler.lastSampleNs.Load(), int64(0))

	// Subsequent samples immediately after should be skipped
	for i := 0; i < 5; i++ {
		should, mult := sampler.ShouldSample()
		assert.False(t, should)
		assert.Equal(t, 0.0, mult)
	}
	assert.Equal(t, int64(5), sampler.skippedCount.Load())

	// Force-advance time by manipulating the atomic state to simulate 2ms elapsed
	sampler.lastSampleNs.Store(now - 2_000_000)

	// Now we should sample again and claim all 5 skipped events (total events = 5 skipped + 1 current = 6)
	should2, mult2 := sampler.ShouldSample()
	assert.True(t, should2)
	assert.Equal(t, 6.0, mult2)
	assert.Equal(t, int64(0), sampler.skippedCount.Load())
}

func TestAdaptiveSampler_ShouldSampleUnderPressure(t *testing.T) {
	sampler := &AdaptiveSampler{}
	now := time.Now().UnixNano()

	t.Run("Low load: nodeCount < 50k (1ms interval)", func(t *testing.T) {
		// First sample should win immediately
		should, mult := sampler.ShouldSampleUnderPressure(10_000)
		assert.True(t, should)
		assert.Equal(t, 1.0, mult)

		// Immediate subsequent should skip
		should2, mult2 := sampler.ShouldSampleUnderPressure(10_000)
		assert.False(t, should2)
		assert.Equal(t, 0.0, mult2)

		// Simulate 2ms elapsed (interval is 1ms)
		sampler.lastSampleNs.Store(now - 2_000_000)
		should3, mult3 := sampler.ShouldSampleUnderPressure(10_000)
		assert.True(t, should3)
		assert.Equal(t, 2.0, mult3) // 1 skipped + 1 current = 2
	})

	t.Run("Medium load: 50k <= nodeCount < 200k (10ms interval)", func(t *testing.T) {
		sampler.skippedCount.Store(0)
		// Set last sample time to now
		sampler.lastSampleNs.Store(now)

		// Call immediate -> should skip because interval is 10ms
		should, mult := sampler.ShouldSampleUnderPressure(100_000)
		assert.False(t, should)
		assert.Equal(t, 0.0, mult)

		// Simulate 5ms elapsed (should still skip because < 10ms)
		sampler.lastSampleNs.Store(now - 5_000_000)
		should2, mult2 := sampler.ShouldSampleUnderPressure(100_000)
		assert.False(t, should2)
		assert.Equal(t, 0.0, mult2)

		// Simulate 15ms elapsed (should sample)
		sampler.lastSampleNs.Store(now - 15_000_000)
		should3, mult3 := sampler.ShouldSampleUnderPressure(100_000)
		assert.True(t, should3)
		assert.Equal(t, 3.0, mult3) // 2 skipped + 1 current = 3
	})

	t.Run("High load: nodeCount >= 200k (100ms interval)", func(t *testing.T) {
		sampler.skippedCount.Store(0)
		sampler.lastSampleNs.Store(now)

		// Call immediate -> should skip because interval is 100ms
		should, mult := sampler.ShouldSampleUnderPressure(300_000)
		assert.False(t, should)
		assert.Equal(t, 0.0, mult)

		// Simulate 50ms elapsed (should still skip because < 100ms)
		sampler.lastSampleNs.Store(now - 50_000_000)
		should2, mult2 := sampler.ShouldSampleUnderPressure(300_000)
		assert.False(t, should2)
		assert.Equal(t, 0.0, mult2)

		// Simulate 150ms elapsed (should sample)
		sampler.lastSampleNs.Store(now - 150_000_000)
		should3, mult3 := sampler.ShouldSampleUnderPressure(300_000)
		assert.True(t, should3)
		assert.Equal(t, 3.0, mult3) // 2 skipped + 1 current = 3
	})
}

func TestAdaptiveSampler_Concurrency(t *testing.T) {
	sampler := &AdaptiveSampler{}
	numWorkers := 8
	callsPerWorker := 50000
	totalExpectedCalls := numWorkers * callsPerWorker

	var totalSampledCount int64
	var activeSamplesCount int64

	eg, _ := errgroup.WithContext(context.Background())

	// Spin a goroutine that constantly pushes time forward in the background to simulate elapsed time under load
	stopTimeSim := make(chan struct{})
	var wgTimeSim sync.WaitGroup
	wgTimeSim.Add(1)
	go func() {
		defer wgTimeSim.Done()
		ticker := time.NewTicker(100 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTimeSim:
				return
			case <-ticker.C:
				// Push lastSampleNs back in time to trigger sampling
				sampler.lastSampleNs.Store(time.Now().UnixNano() - 2_000_000)
			}
		}
	}()

	for i := 0; i < numWorkers; i++ {
		eg.Go(func() error {
			for j := 0; j < callsPerWorker; j++ {
				should, mult := sampler.ShouldSample()
				if should {
					atomic.AddInt64(&totalSampledCount, int64(mult))
					atomic.AddInt64(&activeSamplesCount, 1)
				}
			}
			return nil
		})
	}

	err := eg.Wait()
	assert.NoError(t, err)

	close(stopTimeSim)
	wgTimeSim.Wait()

	// Sum the last remaining skipped counts
	finalSkipped := sampler.skippedCount.Load()
	totalActualReported := totalSampledCount + finalSkipped

	t.Logf("Total expected calls: %d", totalExpectedCalls)
	t.Logf("Total active samples: %d", activeSamplesCount)
	t.Logf("Total sampled/multiplied counts reported: %d", totalSampledCount)
	t.Logf("Final skipped counts remaining: %d", finalSkipped)
	t.Logf("Total actual reported: %d", totalActualReported)

	// Invariant validation: Under concurrent load, the sum of all sampled multipliers
	// plus any final remaining skipped count must mathematically equal the total number of calls!
	assert.Equal(t, int64(totalExpectedCalls), totalActualReported,
		"Adaptive scaling invariant breached: sum of multipliers + final skipped count must equal total calls")
}
