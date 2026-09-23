package tpu

import (
	"context"
	"testing"
)

func TestCPUFallbackEngineSearch(t *testing.T) {
	engine := &CPUFallbackEngine{}
	result, err := engine.Search(context.Background(), [][]float32{{1, 2, 3}}, 5)
	if err != nil {
		t.Errorf("CPUFallbackEngine.Search returned error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestCPUFallbackEngineClose(t *testing.T) {
	engine := &CPUFallbackEngine{}
	if err := engine.Close(); err != nil {
		t.Errorf("CPUFallbackEngine.Close returned error: %v", err)
	}
}

func TestSearchEngineInterface(t *testing.T) {
	var _ SearchEngine = &CPUFallbackEngine{}
	var _ SearchEngine = &TPUEngine{}
}

func TestNewHybridEngineFallback(t *testing.T) {
	// NewHybridEngine tries to load libtpu.so which won't exist in test
	engine := NewHybridEngine("/nonexistent/kernel.hlo")
	if engine == nil {
		t.Fatal("NewHybridEngine returned nil")
	}
	// Should have fallen back to CPU
	if _, ok := engine.(*CPUFallbackEngine); !ok {
		t.Errorf("expected *CPUFallbackEngine, got %T", engine)
	}
}

func TestLoadKernelEmptyFilename(t *testing.T) {
	c := &Client{}
	_, err := c.LoadKernel("")
	if err == nil {
		t.Fatal("expected error for empty filename")
	}
	if err.Error() != "kernel filename cannot be empty" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestLoadKernelNonexistentFile(t *testing.T) {
	c := &Client{}
	_, err := c.LoadKernel("/nonexistent/kernel.hlo")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestCompileHLORequiresDevice(t *testing.T) {
	c := &Client{}
	_, err := c.CompileHLO([]byte("test payload"))
	if err == nil {
		t.Fatal("expected error without TPU device")
	}
}

func TestClientClose(t *testing.T) {
	c := &Client{}
	if err := c.Close(); err != nil {
		t.Errorf("Client.Close returned error: %v", err)
	}
}

func TestErrDeviceNotAvailable(t *testing.T) {
	if ErrDeviceNotAvailable.Error() != "TPU device library (libtpu.so) not available or failed to load" {
		t.Errorf("unexpected error message: %s", ErrDeviceNotAvailable.Error())
	}
}
