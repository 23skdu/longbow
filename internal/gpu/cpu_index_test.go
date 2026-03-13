package gpu

import (
	"testing"
)

func TestCPUIndex_AddAndSearch(t *testing.T) {
	cfg := DefaultGPUConfig()
	cfg.Dimension = 3

	idx, err := NewCPUIndex(cfg)
	if err != nil {
		t.Fatalf("Failed to create CPU index: %v", err)
	}
	defer idx.Close()

	// Add some test vectors
	ids := []int64{1, 2, 3}
	vectors := []float32{
		1.0, 2.0, 3.0, // ID 1
		4.0, 5.0, 6.0, // ID 2
		7.0, 8.0, 9.0, // ID 3
	}

	err = idx.Add(ids, vectors)
	if err != nil {
		t.Fatalf("Failed to add vectors: %v", err)
	}

	// Search for nearest neighbors
	query := []float32{1.0, 2.0, 3.0}
	resultIDs, distances, err := idx.Search(query, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(resultIDs) != 2 {
		t.Errorf("Expected 2 results, got %d", len(resultIDs))
	}

	if len(distances) != 2 {
		t.Errorf("Expected 2 distances, got %d", len(distances))
	}

	// First result should be ID 1 (exact match)
	if resultIDs[0] != 1 {
		t.Errorf("Expected first result to be ID 1, got %d", resultIDs[0])
	}

	if distances[0] != 0 {
		t.Errorf("Expected distance 0 for exact match, got %f", distances[0])
	}
}

func TestCPUIndex_Empty(t *testing.T) {
	cfg := DefaultGPUConfig()
	cfg.Dimension = 3

	idx, err := NewCPUIndex(cfg)
	if err != nil {
		t.Fatalf("Failed to create CPU index: %v", err)
	}
	defer idx.Close()

	// Search on empty index
	query := []float32{1.0, 2.0, 3.0}
	resultIDs, distances, err := idx.Search(query, 5)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(resultIDs) != 0 {
		t.Errorf("Expected 0 results for empty index, got %d", len(resultIDs))
	}

	if len(distances) != 0 {
		t.Errorf("Expected 0 distances for empty index, got %d", len(distances))
	}
}

func TestCPUIndex_Backend(t *testing.T) {
	cfg := DefaultGPUConfig()

	idx, err := NewCPUIndex(cfg)
	if err != nil {
		t.Fatalf("Failed to create CPU index: %v", err)
	}
	defer idx.Close()

	backend := idx.Backend()
	if backend != BackendCPU {
		t.Errorf("Expected backend to be BackendCPU, got %v", backend)
	}
}

func TestCPUIndex_DeviceInfo(t *testing.T) {
	cfg := DefaultGPUConfig()

	idx, err := NewCPUIndex(cfg)
	if err != nil {
		t.Fatalf("Failed to create CPU index: %v", err)
	}
	defer idx.Close()

	info, err := idx.GetDeviceInfo()
	if err != nil {
		t.Fatalf("GetDeviceInfo failed: %v", err)
	}

	if info.Backend != BackendCPU {
		t.Errorf("Expected backend to be BackendCPU, got %v", info.Backend)
	}

	if info.Name != "CPU" {
		t.Errorf("Expected device name to be 'CPU', got %s", info.Name)
	}
}
