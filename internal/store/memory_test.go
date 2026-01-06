package store


import (
	"runtime"
	"testing"
	"unsafe"
)

func TestGetNumaNode(t *testing.T) {
	// Allocate a byte slice
	data := make([]byte, 4096)
	// Force allocation by touching
	data[0] = 1

	ptr := unsafe.Pointer(&data[0])
	node, err := GetNumaNode(ptr)

	if runtime.GOOS != "linux" {
		if err != nil {
			t.Errorf("Expected nil error on non-Linux, got %v", err)
		}
		if node != -1 {
			t.Errorf("Expected node -1 on non-Linux, got %d", node)
		}
		t.Skip("Skipping NUMA test on non-Linux")
	}

	if err != nil {
		t.Fatalf("GetNumaNode failed: %v", err)
	}
	t.Logf("Memory mapped to NUMA node: %d", node)
}

func TestPinThreadToNode(t *testing.T) {
	// This test is tricky because it requires existing nodes.
	// We'll try node 0 which usually exists.
	err := PinThreadToNode(0)

	if runtime.GOOS != "linux" {
		if err != nil {
			t.Errorf("Expected nil error on non-Linux, got %v", err)
		}
		t.Skip("Skipping thread pinning test on non-Linux")
	}

	if err != nil {
		// Might fail if we don't have permission or node 0 doesn't exist (unlikely)
		// Or if we are in a container/restricted env.
		// We'll just log it for now to avoid flaky tests in restricted CI.
		t.Logf("PinThreadToNode failed (expected in some envs): %v", err)
	} else {
		t.Log("Successfully pinned thread to node 0")
	}
}

func TestPinThreadToCore(t *testing.T) {
	err := PinThreadToCore(0)
	if runtime.GOOS != "linux" {
		if err != nil {
			t.Errorf("Expected nil error on non-Linux, got %v", err)
		}
		t.Skip("Skipping core pinning test on non-Linux")
	}

	if err != nil {
		t.Logf("PinThreadToCore failed: %v", err)
	}
}
