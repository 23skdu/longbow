package index

import (
	"sync"
	"time"
)

// PIDTuner implements a simple PID controller to tune efSearch.
type PIDTuner struct {
	targetRecall float64
	kp, ki, kd   float64

	mu         sync.Mutex
	lastError  float64
	integral   float64
	lastUpdate time.Time

	currentEf float64
}

// NewPIDTuner initializes a new PID controller for dynamic parameter tuning.
func NewPIDTuner(targetRecall float64, initialEf int) *PIDTuner {
	return &PIDTuner{
		targetRecall: targetRecall,
		kp:           0.5,
		ki:           0.1,
		kd:           0.05,
		currentEf:    float64(initialEf),
		lastUpdate:   time.Now(),
	}
}

// Update adjusts the currentEf based on the observed recall proxy.
// Since we don't have true recall, we use 'relative stability' or
// 'candidate set coverage' as a proxy.
func (t *PIDTuner) Update(observedRecall float64) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	dt := now.Sub(t.lastUpdate).Seconds()
	if dt <= 0 {
		dt = 0.001
	}

	error := t.targetRecall - observedRecall
	t.integral += error * dt
	derivative := (error - t.lastError) / dt

	output := t.kp*error + t.ki*t.integral + t.kd*derivative

	t.currentEf += output

	// Constraints
	if t.currentEf < 10 {
		t.currentEf = 10
	}
	if t.currentEf > 2000 {
		t.currentEf = 2000
	}

	t.lastError = error
	t.lastUpdate = now

	return int(t.currentEf)
}

// GetCurrentEf returns the current tuned efSearch value.
func (t *PIDTuner) GetCurrentEf() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return int(t.currentEf)
}
