package gpu

import (
	"testing"
	"unsafe"
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

func TestCPUIndex_SearchFloat16(t *testing.T) {
	cfg := DefaultGPUConfig()
	cfg.Dimension = 3

	idx, err := NewCPUIndex(cfg)
	if err != nil {
		t.Fatalf("Failed to create CPU index: %v", err)
	}
	defer idx.Close()

	// Add test vectors as float32
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

	// Convert query to float16 (uint16)
	queryF32 := []float32{1.0, 2.0, 3.0}
	queryF16 := make([]uint16, len(queryF32))
	for i, v := range queryF32 {
		queryF16[i] = float32ToFloat16(v)
	}

	resultIDs, distances, err := idx.SearchFloat16(queryF16, 2)
	if err != nil {
		t.Fatalf("SearchFloat16 failed: %v", err)
	}

	if len(resultIDs) != 2 {
		t.Errorf("Expected 2 results, got %d", len(resultIDs))
	}

	// First result should be ID 1 (exact match)
	if resultIDs[0] != 1 {
		t.Errorf("Expected first result to be ID 1, got %d", resultIDs[0])
	}

	_ = distances // Acknowledge distances returned
}

func TestCPUIndex_SearchComplex64(t *testing.T) {
	cfg := DefaultGPUConfig()
	cfg.Dimension = 4 // complex64 = 2 x float32

	idx, err := NewCPUIndex(cfg)
	if err != nil {
		t.Fatalf("Failed to create CPU index: %v", err)
	}
	defer idx.Close()

	// Add test vectors as float32 (complex64 stored as 2 x float32)
	ids := []int64{1, 2}
	vectors := []float32{
		1.0, 2.0, 3.0, 4.0, // ID 1: complex64(1+2i, 3+4i)
		5.0, 6.0, 7.0, 8.0, // ID 2: complex64(5+6i, 7+8i)
	}

	err = idx.Add(ids, vectors)
	if err != nil {
		t.Fatalf("Failed to add vectors: %v", err)
	}

	// Convert query to float16 (uint16) - complex64 stored as uint16 pairs
	queryF32 := []float32{1.0, 2.0, 3.0, 4.0}
	queryF16 := make([]uint16, len(queryF32))
	for i, v := range queryF32 {
		queryF16[i] = float32ToFloat16(v)
	}

	resultIDs, distances, err := idx.SearchComplex64(queryF16, 2)
	if err != nil {
		t.Fatalf("SearchComplex64 failed: %v", err)
	}

	if len(resultIDs) != 2 {
		t.Errorf("Expected 2 results, got %d", len(resultIDs))
	}

	// First result should be ID 1 (exact match)
	if resultIDs[0] != 1 {
		t.Errorf("Expected first result to be ID 1, got %d", resultIDs[0])
	}

	_ = distances // Acknowledge distances returned
}

func TestCPUIndex_SearchComplex128(t *testing.T) {
	cfg := DefaultGPUConfig()
	cfg.Dimension = 4 // complex128 = 2 x float64, stored as 2 x float32

	idx, err := NewCPUIndex(cfg)
	if err != nil {
		t.Fatalf("Failed to create CPU index: %v", err)
	}
	defer idx.Close()

	// Add test vectors (complex128 stored as float32 pairs)
	ids := []int64{1, 2}
	vectors := []float32{
		1.0, 2.0, 3.0, 4.0, // ID 1: complex128(1+2i, 3+4i)
		5.0, 6.0, 7.0, 8.0, // ID 2: complex128(5+6i, 7+8i)
	}

	err = idx.Add(ids, vectors)
	if err != nil {
		t.Fatalf("Failed to add vectors: %v", err)
	}

	// Query with float32 (complex128 stored as float32 pairs)
	query := []float32{1.0, 2.0, 3.0, 4.0}

	resultIDs, distances, err := idx.SearchComplex128(query, 2)
	if err != nil {
		t.Fatalf("SearchComplex128 failed: %v", err)
	}

	if len(resultIDs) != 2 {
		t.Errorf("Expected 2 results, got %d", len(resultIDs))
	}

	// First result should be ID 1 (exact match)
	if resultIDs[0] != 1 {
		t.Errorf("Expected first result to be ID 1, got %d", resultIDs[0])
	}

	_ = distances // Acknowledge distances returned
}

// float32ToFloat16 converts float32 to uint16 float16 representation
func float32ToFloat16(v float32) uint16 {
	// Simplified conversion - for testing purposes
	bits := *(*uint32)(unsafe.Pointer(&v))
	sign := (bits >> 31) & 0x1
	exp := (bits >> 23) & 0xFF
	mant := bits & 0x7FFFFF

	if exp == 0 {
		return uint16(sign << 15)
	}
	if exp == 0xFF {
		return uint16((sign << 15) | (0x1F << 10) | (mant >> 13))
	}

	newExp := (exp - 127 + 15) & 0x1F
	newMant := mant >> 13
	return uint16((sign << 15) | (newExp << 10) | newMant)
}
