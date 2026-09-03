package memory

import (
	"runtime"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestGCTuner_EmergencyRateLimiter(t *testing.T) {
	limit := int64(100 * 1024 * 1024)
	logger := zerolog.Nop()
	tuner := NewGCTuner(limit, 100, 10, &logger)
	tuner.EnableGPUTuning = false
	tuner.GetPhysicalStats = func() (int64, int64) { return 0, 0 }

	// Cooldown set to 100ms for test speed
	tuner.SetEmergencyGCCooldown(100 * time.Millisecond)

	now := time.Now()
	// First emergency check should be admitted
	assert.True(t, tuner.shouldRunEmergencyGC(now), "Initial emergency GC should run")

	// Immediate consecutive checks should be blocked by cooldown
	for i := 0; i < 10; i++ {
		assert.False(t, tuner.shouldRunEmergencyGC(now.Add(time.Duration(i)*time.Millisecond)),
			"Rapid consecutive check should be blocked by cooldown")
	}

	// Check after cooldown duration should succeed
	assert.True(t, tuner.shouldRunEmergencyGC(now.Add(101*time.Millisecond)),
		"Check after cooldown window should be admitted")

	// Next immediate check should be blocked again
	assert.False(t, tuner.shouldRunEmergencyGC(now.Add(102*time.Millisecond)),
		"Check immediately after second run should be blocked")
}

func TestGCTuner_RapidTune_RateLimited(t *testing.T) {
	limit := int64(100 * 1024 * 1024)
	logger := zerolog.Nop()
	tuner := NewGCTuner(limit, 100, 10, &logger)
	tuner.EnableGPUTuning = false
	tuner.IsAggressive = true
	tuner.GetPhysicalStats = func() (int64, int64) { return 0, 0 }

	// Cooldown set to 10 seconds default
	assert.Equal(t, 10*time.Second, tuner.emergencyGCCooldown)

	// Simulate 100 rapid calls under 98% memory pressure
	heapUsage := uint64(98 * 1024 * 1024)
	stats := &runtime.MemStats{HeapAlloc: heapUsage}

	for i := 0; i < 100; i++ {
		tuner.tune(stats, true)
	}

	// Should not have panicked or deadlocked, target GOGC clamped to low
	assert.Equal(t, 10, tuner.currentGOGC)
	assert.False(t, tuner.lastEmergencyGC.IsZero())
}

func FuzzGCTuner_EmergencyRateLimiter(f *testing.F) {
	f.Add(int64(100), int64(10), int64(5))
	f.Add(int64(1000), int64(500), int64(1000))
	f.Add(int64(10), int64(1), int64(0))

	f.Fuzz(func(t *testing.T, cooldownMs, step1Ms, step2Ms int64) {
		if cooldownMs < 1 {
			cooldownMs = 1
		}
		if cooldownMs > 10000 {
			cooldownMs = 10000
		}
		if step1Ms < 0 {
			step1Ms = 0
		}
		if step2Ms < 0 {
			step2Ms = 0
		}

		logger := zerolog.Nop()
		tuner := NewGCTuner(100*1024*1024, 100, 10, &logger)
		cooldown := time.Duration(cooldownMs) * time.Millisecond
		tuner.SetEmergencyGCCooldown(cooldown)

		base := time.Now()
		r1 := tuner.shouldRunEmergencyGC(base)
		assert.True(t, r1, "Initial call must always succeed")

		t1 := base.Add(time.Duration(step1Ms) * time.Millisecond)
		r2 := tuner.shouldRunEmergencyGC(t1)
		if time.Duration(step1Ms)*time.Millisecond < cooldown {
			assert.False(t, r2, "Step 1 within cooldown must be false")
		} else {
			assert.True(t, r2, "Step 1 after cooldown must be true")
		}
	})
}
