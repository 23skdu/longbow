package store

import (
	"os"
	"testing"
)

func TestLowMemConfig(t *testing.T) {
	// 1. Test without LONGBOW_LOW_MEM
	os.Unsetenv("LONGBOW_LOW_MEM")
	config := DefaultArrowHNSWConfig()
	if config.InitialCapacity != 50000 {
		t.Errorf("Expected InitialCapacity 50000, got %d", config.InitialCapacity)
	}
	if config.M != 32 {
		t.Errorf("Expected M 32, got %d", config.M)
	}

	// 2. Test with LONGBOW_LOW_MEM="1"
	os.Setenv("LONGBOW_LOW_MEM", "1")
	config = DefaultArrowHNSWConfig()
	if config.InitialCapacity != 5000 {
		t.Errorf("Expected InitialCapacity 5000 with LOW_MEM=1, got %d", config.InitialCapacity)
	}
	if config.M != 16 {
		t.Errorf("Expected M 16 with LOW_MEM=1, got %d", config.M)
	}

	// 3. Test with LONGBOW_LOW_MEM="true"
	os.Setenv("LONGBOW_LOW_MEM", "true")
	config = DefaultArrowHNSWConfig()
	if config.InitialCapacity != 5000 {
		t.Errorf("Expected InitialCapacity 5000 with LOW_MEM=true, got %d", config.InitialCapacity)
	}

	// Cleanup
	os.Unsetenv("LONGBOW_LOW_MEM")
}
