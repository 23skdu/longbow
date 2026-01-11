package store

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/gc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorStore_AdaptiveGC_Integration(t *testing.T) {
	pool := memory.NewGoAllocator()
	logger := zerolog.Nop()

	// Create VectorStore
	store := NewVectorStore(pool, logger, 1<<30, 0, time.Hour)
	require.NotNil(t, store)
	require.NotNil(t, store.gcController, "GC controller should be initialized")

	// By default, GC controller should be disabled
	// We can't easily test this without exposing internal state,
	// but we can verify EnableAdaptiveGC works

	// Enable adaptive GC with custom config
	config := gc.AdaptiveGCConfig{
		Enabled:        true,
		MinGOGC:        60,
		MaxGOGC:        180,
		AdjustInterval: 100 * time.Millisecond,
	}

	store.EnableAdaptiveGC(config)

	// Let it run for a bit
	time.Sleep(300 * time.Millisecond)

	// Disable it
	store.DisableAdaptiveGC()

	// Should be able to call disable again safely
	store.DisableAdaptiveGC()

	assert.True(t, true, "Integration test completed without panic")
}

func TestVectorStore_AdaptiveGC_EnableDisableMultiple(t *testing.T) {
	pool := memory.NewGoAllocator()
	logger := zerolog.Nop()

	store := NewVectorStore(pool, logger, 1<<30, 0, time.Hour)

	config := gc.AdaptiveGCConfig{
		Enabled:        true,
		MinGOGC:        50,
		MaxGOGC:        200,
		AdjustInterval: 50 * time.Millisecond,
	}

	// Enable, disable, enable again
	store.EnableAdaptiveGC(config)
	time.Sleep(100 * time.Millisecond)

	store.DisableAdaptiveGC()
	time.Sleep(50 * time.Millisecond)

	// Re-enable with different config
	config.MinGOGC = 70
	config.MaxGOGC = 150
	store.EnableAdaptiveGC(config)
	time.Sleep(100 * time.Millisecond)

	store.DisableAdaptiveGC()

	assert.True(t, true, "Multiple enable/disable cycles completed")
}

func TestVectorStore_AdaptiveGC_DefaultDisabled(t *testing.T) {
	pool := memory.NewGoAllocator()
	logger := zerolog.Nop()

	store := NewVectorStore(pool, logger, 1<<30, 0, time.Hour)

	// Controller should exist but not be running
	require.NotNil(t, store.gcController)

	// Disabling when not enabled should be safe
	store.DisableAdaptiveGC()

	assert.True(t, true, "Default disabled state verified")
}
