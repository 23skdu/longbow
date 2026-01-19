package store

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestDiskVectorStore_AppendRead(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "vectors.bin")
	dims := 4

	store, err := NewDiskVectorStore(path, dims)
	if err != nil {
		t.Fatalf("NewDiskVectorStore failed: %v", err)
	}
	defer store.Close()

	// Append vectors
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}

	id1, err := store.Append(vec1)
	if err != nil {
		t.Fatalf("Append(0) failed: %v", err)
	}
	if id1 != 0 {
		t.Errorf("Expected ID 0, got %d", id1)
	}

	id2, err := store.Append(vec2)
	if err != nil {
		t.Fatalf("Append(1) failed: %v", err)
	}
	if id2 != 1 {
		t.Errorf("Expected ID 1, got %d", id2)
	}

	// Verify Count
	if store.Count() != 2 {
		t.Errorf("Expected count 2, got %d", store.Count())
	}

	// Read back
	got1, err := store.Get(0)
	if err != nil {
		t.Fatalf("Get(0) failed: %v", err)
	}
	if !reflect.DeepEqual(got1, vec1) {
		t.Errorf("Vector 0 mismatch: got %v, want %v", got1, vec1)
	}

	got2, err := store.Get(1)
	if err != nil {
		t.Fatalf("Get(1) failed: %v", err)
	}
	if !reflect.DeepEqual(got2, vec2) {
		t.Errorf("Vector 1 mismatch: got %v, want %v", got2, vec2)
	}

	// Error cases
	if _, err := store.Get(2); err == nil {
		t.Errorf("Expected error for non-existent ID 2")
	}
}

func TestDiskVectorStore_Reopen(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "vectors_persist.bin")
	dims := 128

	// 1. Create and Write
	store, err := NewDiskVectorStore(path, dims)
	if err != nil {
		t.Fatalf("NewDiskVectorStore failed: %v", err)
	}

	numVecs := 100
	for i := 0; i < numVecs; i++ {
		vec := make([]float32, dims)
		vec[0] = float32(i) // Mark vector
		if _, err := store.Append(vec); err != nil {
			t.Fatalf("Append failed at %d: %v", i, err)
		}
	}
	store.Close()

	// 2. Reopen
	store2, err := NewDiskVectorStore(path, dims)
	if err != nil {
		t.Fatalf("NewDiskVectorStore reopen failed: %v", err)
	}
	defer store2.Close()

	if store2.Count() != uint32(numVecs) {
		t.Errorf("Reopened count mismatch: got %d, want %d", store2.Count(), numVecs)
	}

	// Verify random read
	id := 50
	vec, err := store2.Get(uint32(id))
	if err != nil {
		t.Fatalf("Get(%d) failed: %v", id, err)
	}
	if vec[0] != float32(id) {
		t.Errorf("Vector mismatch at %d: got %v", id, vec[0])
	}
}
