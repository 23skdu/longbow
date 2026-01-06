package store


import (
	"testing"
)

// Extension tests for Namespace coverage gaps

func TestNamespace_AddDataset(t *testing.T) {
	ns := NewNamespace("test-ns")
	if ns.DatasetCount() != 0 {
		t.Errorf("initial count should be 0, got %d", ns.DatasetCount())
	}

	ns.AddDataset("dataset1")
	if ns.DatasetCount() != 1 {
		t.Errorf("count after add should be 1, got %d", ns.DatasetCount())
	}

	// Add same dataset again (should not duplicate)
	ns.AddDataset("dataset1")
	if ns.DatasetCount() != 1 {
		t.Errorf("count after duplicate add should still be 1, got %d", ns.DatasetCount())
	}

	ns.AddDataset("dataset2")
	if ns.DatasetCount() != 2 {
		t.Errorf("count after second add should be 2, got %d", ns.DatasetCount())
	}
}

func TestNamespace_RemoveDataset(t *testing.T) {
	ns := NewNamespace("test-ns")
	ns.AddDataset("dataset1")
	ns.AddDataset("dataset2")

	ns.RemoveDataset("dataset1")
	if ns.DatasetCount() != 1 {
		t.Errorf("count after remove should be 1, got %d", ns.DatasetCount())
	}

	// Remove non-existent should not panic
	ns.RemoveDataset("nonexistent")
	if ns.DatasetCount() != 1 {
		t.Errorf("count after removing nonexistent should still be 1, got %d", ns.DatasetCount())
	}
}

func TestNamespace_HasDataset(t *testing.T) {
	ns := NewNamespace("test-ns")

	if ns.HasDataset("dataset1") {
		t.Error("should not have dataset1 initially")
	}

	ns.AddDataset("dataset1")
	if !ns.HasDataset("dataset1") {
		t.Error("should have dataset1 after add")
	}

	ns.RemoveDataset("dataset1")
	if ns.HasDataset("dataset1") {
		t.Error("should not have dataset1 after remove")
	}
}

func TestBuildNamespacedPath(t *testing.T) {
	tests := []struct {
		namespace string
		dataset   string
		expected  string
	}{
		{"default", "mydata", "mydata"},
		{"production", "vectors", "production/vectors"},
		{"", "mydata", "/mydata"},
		{"ns", "", "ns/"},
	}

	for _, tc := range tests {
		result := BuildNamespacedPath(tc.namespace, tc.dataset)
		if result != tc.expected {
			t.Errorf("BuildNamespacedPath(%q, %q) = %q, want %q",
				tc.namespace, tc.dataset, result, tc.expected)
		}
	}
}

func TestNamespace_ConcurrentAccess(t *testing.T) {
	ns := NewNamespace("concurrent-ns")
	done := make(chan bool)

	// Concurrent adds
	for i := 0; i < 10; i++ {
		go func(_ int) {
			ns.AddDataset("dataset")
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Should have exactly 1 dataset (all added same name)
	if ns.DatasetCount() != 1 {
		t.Errorf("concurrent adds of same name: got %d, want 1", ns.DatasetCount())
	}
}
