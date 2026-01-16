package store

import (
	"path/filepath"
	"testing"
)

func TestDiskVectorStore_Lifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "vectors.bin")
	dim := 128

	// 1. Create and Open
	store, err := NewDiskVectorStore(path, dim)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// 2. Append Vectors
	count := 100
	vecs := make([][]float32, count)
	for i := 0; i < count; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = float32(i) + float32(j)*0.1
		}
		vecs[i] = vec
		id, err := store.Append(vec)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		if int(id) != i {
			t.Errorf("Expected ID %d, got %d", i, id)
		}
	}

	// 3. Verify Size
	if store.Size() != count {
		t.Errorf("Expected size %d, got %d", count, store.Size())
	}

	// 4. Read Verification (Hot)
	for i := 0; i < count; i++ {
		got, err := store.Get(uint32(i))
		if err != nil {
			t.Errorf("Get(%d) failed: %v", i, err)
			continue
		}
		if len(got) != dim {
			t.Errorf("Get(%d) returned wrong dim: %d", i, len(got))
		}
		// Check a few values
		if got[0] != vecs[i][0] {
			t.Errorf("Vector %d mismatch at 0: want %f, got %f", i, vecs[i][0], got[0])
		}
	}

	// 5. Close
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 6. Re-open (Persistence)
	store2, err := NewDiskVectorStore(path, dim)
	if err != nil {
		t.Fatalf("Failed to re-open store: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if store2.Size() != count {
		t.Errorf("Re-opened size mismatch: want %d, got %d", count, store2.Size())
	}

	// 7. Read Verification (Cold)
	for i := 0; i < count; i++ {
		got, err := store2.Get(uint32(i))
		if err != nil {
			t.Errorf("Get(%d) failed after reopen: %v", i, err)
			continue
		}
		if got[0] != vecs[i][0] {
			t.Errorf("Vector %d mismatch after reopen: want %f, got %f", i, vecs[i][0], got[0])
		}
	}
}

func TestDiskVectorStore_Bounds(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bounds.bin")
	dim := 4

	store, err := NewDiskVectorStore(path, dim)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Append one
	_, _ = store.Append([]float32{1, 2, 3, 4})

	// Get out of bounds
	if _, err := store.Get(1); err == nil {
		t.Error("Expected error for out of bounds Get(1)")
	}

	// Append wrong dim
	if _, err := store.Append([]float32{1, 2, 3}); err == nil {
		t.Error("Expected error for wrong dimension append")
	}
}

func TestDiskVectorStore_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "persistence.bin")
	dim := 2

	// Round 1
	store, _ := NewDiskVectorStore(path, dim)
	_, _ = store.Append([]float32{1.0, 2.0})
	_ = store.Close()

	// Round 2 (Append more)
	store, _ = NewDiskVectorStore(path, dim)
	if store.Size() != 1 {
		t.Fatalf("Expected size 1, got %d", store.Size())
	}
	_, _ = store.Append([]float32{3.0, 4.0})
	_ = store.Close()

	// Round 3 (Verify all)
	store, _ = NewDiskVectorStore(path, dim)
	defer func() { _ = store.Close() }()
	if store.Size() != 2 {
		t.Fatalf("Expected size 2, got %d", store.Size())
	}

	v1, _ := store.Get(0)
	if v1[0] != 1.0 {
		t.Errorf("v1[0] mismatch")
	}
	v2, _ := store.Get(1)
	if v2[0] != 3.0 {
		t.Errorf("v2[0] mismatch")
	}
}
