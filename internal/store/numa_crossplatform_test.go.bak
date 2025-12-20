package store

import (
	"runtime"
	"testing"
)

// TestCrossPlatformCompilation verifies that platform-specific
// functions exist and have correct signatures for cross-compilation.
// This test documents the interface contract for stub implementations.
func TestCrossPlatformCompilation(t *testing.T) {
	// GetCurrentCPU must exist on all platforms
	cpu := GetCurrentCPU()
	t.Logf("GetCurrentCPU returned: %d (GOOS=%s)", cpu, runtime.GOOS)

	// On non-Linux, should return -1 (stub behavior)
	// On Linux, returns actual CPU or -1 if syscall fails
	if cpu < -1 || cpu >= runtime.NumCPU()*2 {
		t.Errorf("GetCurrentCPU returned invalid value: %d", cpu)
	}
}

func TestBindGoroutineToNode_CrossPlatform(t *testing.T) {
	cfg := NUMAConfig{Enabled: false}
	alloc, err := NewNUMAAllocator(cfg)
	if err != nil {
		t.Skipf("Could not create allocator: %v", err)
	}

	// BindGoroutineToNode must exist on all platforms
	// When NUMA disabled, should succeed silently
	err = alloc.BindGoroutineToNode(0)
	if err != nil {
		t.Logf("BindGoroutineToNode returned error (expected on non-NUMA systems): %v", err)
	}
}

func TestGetCurrentNode_CrossPlatform(t *testing.T) {
	cfg := NUMAConfig{Enabled: false}
	alloc, _ := NewNUMAAllocator(cfg)

	// When disabled, should return -1
	node := alloc.GetCurrentNode()
	if node != -1 {
		t.Errorf("GetCurrentNode with disabled NUMA should return -1, got %d", node)
	}
}
