package metrics

import (
	"sync/atomic"
	"time"
)

// AdaptiveSampler provides a high-performance, load-adaptive sampling mechanism.
// It samples at a high rate under low load, and dynamically reduces the sample
// rate under high concurrent load to prevent metric collection from becoming a bottleneck.
// By accumulating skipped counts, it allows callers to accurately scale counters.
type AdaptiveSampler struct {
	lastSampleNs atomic.Int64
	skippedCount atomic.Int64
}

// GlobalHotpathSampler is the default sampler for extreme hotpaths.
var GlobalHotpathSampler = &AdaptiveSampler{}

// ShouldSample limits sampling to at most 1000 times per second (1ms interval).
// It returns a boolean indicating whether to sample, and a float64 multiplier
// indicating how many events this sample represents (1 + skipped counts).
func (s *AdaptiveSampler) ShouldSample() (bool, float64) {
	now := time.Now().UnixNano()
	last := s.lastSampleNs.Load()
	
	// 1 millisecond interval = 1,000,000 nanoseconds
	if now-last > 1_000_000 {
		if s.lastSampleNs.CompareAndSwap(last, now) {
			// We won the race to sample. Claim all accumulated skipped counts.
			// Swap resets skippedCount to 0 and returns the previous value.
			skipped := s.skippedCount.Swap(0)
			return true, float64(skipped + 1)
		}
	}
	
	// We didn't sample, just increment the skip count
	s.skippedCount.Add(1)
	return false, 0
}
