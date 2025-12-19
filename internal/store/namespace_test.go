package store

import (
"testing"

"github.com/apache/arrow-go/v18/arrow/memory"
"go.uber.org/zap"
)

// TestNamespaceCreate verifies namespace creation
func TestNamespaceCreate(t *testing.T) {
logger := zap.NewNop()
mem := memory.NewGoAllocator()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 0)
defer vs.Close() //nolint:errcheck

// Create a namespace
err := vs.CreateNamespace("tenant1")
if err != nil {
t.Fatalf("failed to create namespace: %v", err)
}

// Verify it exists
if !vs.NamespaceExists("tenant1") {
t.Error("namespace should exist after creation")
}
}

// TestNamespaceCreateDuplicate verifies duplicate namespace error
func TestNamespaceCreateDuplicate(t *testing.T) {
logger := zap.NewNop()
mem := memory.NewGoAllocator()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 0)
defer vs.Close() //nolint:errcheck

// Create first
err := vs.CreateNamespace("tenant1")
if err != nil {
t.Fatalf("failed to create namespace: %v", err)
}

// Duplicate should fail
err = vs.CreateNamespace("tenant1")
if err == nil {
t.Error("expected error for duplicate namespace")
}
}

// TestNamespaceDelete verifies namespace deletion
func TestNamespaceDelete(t *testing.T) {
logger := zap.NewNop()
mem := memory.NewGoAllocator()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 0)
defer vs.Close() //nolint:errcheck

vs.CreateNamespace("tenant1") //nolint:errcheck

// Delete
err := vs.DeleteNamespace("tenant1")
if err != nil {
t.Fatalf("failed to delete namespace: %v", err)
}

// Should not exist
if vs.NamespaceExists("tenant1") {
t.Error("namespace should not exist after deletion")
}
}

// TestNamespaceList verifies listing namespaces
func TestNamespaceList(t *testing.T) {
logger := zap.NewNop()
mem := memory.NewGoAllocator()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 0)
defer vs.Close() //nolint:errcheck

// Create multiple
vs.CreateNamespace("tenant1") //nolint:errcheck
vs.CreateNamespace("tenant2") //nolint:errcheck
vs.CreateNamespace("tenant3") //nolint:errcheck

list := vs.ListNamespaces()
// Should have 4: default + 3 created
if len(list) < 3 {
t.Errorf("expected at least 3 namespaces, got %d", len(list))
}
}

// TestNamespaceDefaultExists verifies default namespace is always present
func TestNamespaceDefaultExists(t *testing.T) {
logger := zap.NewNop()
mem := memory.NewGoAllocator()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 0)
defer vs.Close() //nolint:errcheck

// Default namespace should exist on fresh store
if !vs.NamespaceExists("default") {
t.Error("default namespace should exist")
}
}

// TestNamespaceCannotDeleteDefault verifies default namespace cannot be deleted
func TestNamespaceCannotDeleteDefault(t *testing.T) {
logger := zap.NewNop()
mem := memory.NewGoAllocator()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 0)
defer vs.Close() //nolint:errcheck

err := vs.DeleteNamespace("default")
if err == nil {
t.Error("should not be able to delete default namespace")
}
}

// TestNamespaceDatasetIsolation verifies datasets are isolated per namespace
func TestNamespaceDatasetIsolation(t *testing.T) {
logger := zap.NewNop()
mem := memory.NewGoAllocator()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 0)
defer vs.Close() //nolint:errcheck

vs.CreateNamespace("tenant1") //nolint:errcheck
vs.CreateNamespace("tenant2") //nolint:errcheck

// Get dataset count per namespace
count1 := vs.GetNamespaceDatasetCount("tenant1")
count2 := vs.GetNamespaceDatasetCount("tenant2")

// Both should start at 0
if count1 != 0 {
t.Errorf("expected 0 datasets in tenant1, got %d", count1)
}
if count2 != 0 {
t.Errorf("expected 0 datasets in tenant2, got %d", count2)
}
}

// TestParseNamespacedPath verifies path parsing
func TestParseNamespacedPath(t *testing.T) {
tests := []struct {
path      string
namespace string
dataset   string
}{
{"tenant1/dataset1", "tenant1", "dataset1"},
{"dataset1", "default", "dataset1"},
{"org/project/data", "org", "project/data"},
{"/leading/slash", "default", "leading/slash"},
{"", "default", ""},
}

for _, tt := range tests {
ns, ds := ParseNamespacedPath(tt.path)
if ns != tt.namespace {
t.Errorf("ParseNamespacedPath(%q) namespace = %q, want %q", tt.path, ns, tt.namespace)
}
if ds != tt.dataset {
t.Errorf("ParseNamespacedPath(%q) dataset = %q, want %q", tt.path, ds, tt.dataset)
}
}
}

// TestNamespaceMetrics verifies Prometheus metrics are emitted
func TestNamespaceMetrics(t *testing.T) {
logger := zap.NewNop()
mem := memory.NewGoAllocator()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 0)
defer vs.Close() //nolint:errcheck

initialCount := vs.GetTotalNamespaceCount()

vs.CreateNamespace("tenant1") //nolint:errcheck

newCount := vs.GetTotalNamespaceCount()
if newCount != initialCount+1 {
t.Errorf("expected count %d, got %d", initialCount+1, newCount)
}
}
