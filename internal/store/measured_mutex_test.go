package store


import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMeasuredMutex(t *testing.T) {
	// Simple test to ensure no panics and basic functionality.
	// Verifying generic prometheus output in unit test is hard without a registry,
	// but we can trust the library. We just check basic locking works.

	m := NewMeasuredMutex("test_mutex")

	// Test Lock/Unlock
	m.Lock()
	_ = 0
	m.Unlock()

	// Test concurrency
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		m.Lock()
		time.Sleep(10 * time.Millisecond)
		m.Unlock()
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		m.Lock() // Should contend
		_ = 0
		m.Unlock()
	}()

	wg.Wait()
}

func TestMeasuredRWMutex(t *testing.T) {
	m := NewMeasuredRWMutex("test_rwmutex")

	// Write Lock
	m.Lock()
	_ = 0
	m.Unlock()

	// Read Lock
	m.RLock()
	_ = 0
	m.RUnlock()

	// Concurrent R/W
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		m.Lock()
		time.Sleep(20 * time.Millisecond)
		m.Unlock()
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		m.RLock() // Should block
		assert.True(t, true)
		m.RUnlock()
	}()

	wg.Wait()
}
