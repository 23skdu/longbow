//go:build gpu

package gpu

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestMultiGPUStrategy_String(t *testing.T) {
	tests := []struct {
		strategy MultiGPUStrategy
		expected string
	}{
		{StrategyRoundRobin, "round_robin"},
		{StrategyLoadBalance, "load_balance"},
		{StrategyAffinity, "affinity"},
		{StrategyMemoryAware, "memory_aware"},
		{MultiGPUStrategy(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.strategy.String(); got != tt.expected {
			t.Errorf("MultiGPUStrategy(%d).String() = %q, want %q", tt.strategy, got, tt.expected)
		}
	}
}

func TestDefaultMultiGPUConfig(t *testing.T) {
	config := DefaultMultiGPUConfig()

	if len(config.DeviceIDs) != 1 || config.DeviceIDs[0] != 0 {
		t.Errorf("Expected default DeviceIDs [0], got %v", config.DeviceIDs)
	}
	if config.Strategy != StrategyRoundRobin {
		t.Errorf("Expected default Strategy RoundRobin, got %v", config.Strategy)
	}
}

func TestDetectAvailableDevices(t *testing.T) {
	devices, err := DetectAvailableDevices()

	if err != nil {
		t.Skipf("No GPU devices available: %v", err)
		return
	}

	if len(devices) == 0 {
		t.Error("Expected at least one device")
	}

	for i, d := range devices {
		if d != i {
			t.Errorf("Expected device %d at index %d, got %d", i, i, d)
		}
	}
}

func TestNewMultiGPUManager_NoDevices(t *testing.T) {
	logger := zerolog.Nop()
	config := MultiGPUConfig{
		DeviceIDs: []int{},
		Strategy:  StrategyRoundRobin,
	}

	_, err := NewMultiGPUManager(config, logger)
	if err == nil {
		t.Error("Expected error for empty DeviceIDs")
	}
}

func TestNewMultiGPUManager_InvalidDevice(t *testing.T) {
	logger := zerolog.Nop()
	config := MultiGPUConfig{
		DeviceIDs: []int{999},
		Strategy:  StrategyRoundRobin,
	}

	_, err := NewMultiGPUManager(config, logger)
	if err == nil {
		t.Error("Expected error for invalid device ID")
	}
}

func TestNewMultiGPUManager_ValidDevice(t *testing.T) {
	logger := zerolog.Nop()

	devices, err := DetectAvailableDevices()
	if err != nil {
		t.Skipf("No GPU devices available: %v", err)
		return
	}

	config := MultiGPUConfig{
		DeviceIDs: devices[:1],
		Strategy:  StrategyRoundRobin,
	}

	mgr, err := NewMultiGPUManager(config, logger)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	if mgr.DeviceCount() != 1 {
		t.Errorf("Expected 1 device, got %d", mgr.DeviceCount())
	}
}

func TestMultiGPUManager_SelectDevice_RoundRobin(t *testing.T) {
	logger := zerolog.Nop()

	devices, err := DetectAvailableDevices()
	if err != nil || len(devices) < 1 {
		t.Skipf("Not enough GPU devices: %v", err)
	}

	config := MultiGPUConfig{
		DeviceIDs: devices[:1],
		Strategy:  StrategyRoundRobin,
	}

	mgr, err := NewMultiGPUManager(config, logger)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	device := mgr.SelectDevice()
	if device == nil {
		t.Error("Expected non-nil device")
	}

	if device.ID != devices[0] {
		t.Errorf("Expected device %d, got %d", devices[0], device.ID)
	}
}

func TestMultiGPUManager_GetStats(t *testing.T) {
	logger := zerolog.Nop()

	devices, err := DetectAvailableDevices()
	if err != nil || len(devices) < 1 {
		t.Skipf("No GPU devices available: %v", err)
	}

	config := MultiGPUConfig{
		DeviceIDs: devices[:1],
		Strategy:  StrategyRoundRobin,
	}

	mgr, err := NewMultiGPUManager(config, logger)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	stats := mgr.GetStats()

	if stats.DeviceCount != 1 {
		t.Errorf("Expected 1 device in stats, got %d", stats.DeviceCount)
	}

	if len(stats.Devices) != 1 {
		t.Errorf("Expected 1 device stats, got %d", len(stats.Devices))
	}

	if stats.Strategy != "round_robin" {
		t.Errorf("Expected round_robin strategy, got %s", stats.Strategy)
	}
}

func TestMultiGPUManager_Close(t *testing.T) {
	logger := zerolog.Nop()

	devices, err := DetectAvailableDevices()
	if err != nil || len(devices) < 1 {
		t.Skipf("No GPU devices available: %v", err)
	}

	config := MultiGPUConfig{
		DeviceIDs: devices[:1],
		Strategy:  StrategyRoundRobin,
	}

	mgr, err := NewMultiGPUManager(config, logger)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	err = mgr.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if mgr.DeviceCount() != 0 {
		t.Errorf("Expected 0 devices after close, got %d", mgr.DeviceCount())
	}
}
