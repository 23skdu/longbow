package store

import (
"errors"
"strings"
"sync"
"time"

"github.com/23skdu/longbow/internal/metrics"
)

// Namespace represents a tenant isolation unit containing related datasets.
// Namespaces provide multi-tenancy support with dataset isolation.
type Namespace struct {
Name      string
CreatedAt time.Time
Metadata  map[string]string
datasets  map[string]bool // tracks dataset names in this namespace
mu        sync.RWMutex
}

// NewNamespace creates a new namespace with the given name.
func NewNamespace(name string) *Namespace {
return &Namespace{
Name:      name,
CreatedAt: time.Now(),
Metadata:  make(map[string]string),
datasets:  make(map[string]bool),
}
}

// AddDataset registers a dataset in this namespace.
func (n *Namespace) AddDataset(name string) {
n.mu.Lock()
defer n.mu.Unlock()
n.datasets[name] = true
}

// RemoveDataset removes a dataset from this namespace.
func (n *Namespace) RemoveDataset(name string) {
n.mu.Lock()
defer n.mu.Unlock()
delete(n.datasets, name)
}

// DatasetCount returns the number of datasets in this namespace.
func (n *Namespace) DatasetCount() int {
n.mu.RLock()
defer n.mu.RUnlock()
return len(n.datasets)
}

// HasDataset checks if a dataset exists in this namespace.
func (n *Namespace) HasDataset(name string) bool {
n.mu.RLock()
defer n.mu.RUnlock()
return n.datasets[name]
}

// namespaces holds all namespaces in the VectorStore
type namespaceManager struct {
namespaces map[string]*Namespace
mu         sync.RWMutex
}

func newNamespaceManager() *namespaceManager {
nm := &namespaceManager{
namespaces: make(map[string]*Namespace),
}
// Always create default namespace
nm.namespaces["default"] = NewNamespace("default")
return nm
}

// CreateNamespace creates a new namespace with the given name.
// Returns error if namespace already exists.
func (vs *VectorStore) CreateNamespace(name string) error {
if name == "" {
return errors.New("namespace name cannot be empty")
}

vs.nsManager.mu.Lock()
defer vs.nsManager.mu.Unlock()

if _, exists := vs.nsManager.namespaces[name]; exists {
return errors.New("namespace already exists: " + name)
}

vs.nsManager.namespaces[name] = NewNamespace(name)
metrics.NamespacesTotal.Inc()
return nil
}

// NamespaceExists checks if a namespace exists.
func (vs *VectorStore) NamespaceExists(name string) bool {
vs.nsManager.mu.RLock()
defer vs.nsManager.mu.RUnlock()
_, exists := vs.nsManager.namespaces[name]
return exists
}

// DeleteNamespace removes a namespace.
// Returns error if namespace is "default" or doesn't exist.
func (vs *VectorStore) DeleteNamespace(name string) error {
if name == "default" {
return errors.New("cannot delete default namespace")
}

vs.nsManager.mu.Lock()
defer vs.nsManager.mu.Unlock()

if _, exists := vs.nsManager.namespaces[name]; !exists {
return errors.New("namespace not found: " + name)
}

delete(vs.nsManager.namespaces, name)
metrics.NamespacesTotal.Dec()
return nil
}

// ListNamespaces returns all namespace names.
func (vs *VectorStore) ListNamespaces() []string {
vs.nsManager.mu.RLock()
defer vs.nsManager.mu.RUnlock()

names := make([]string, 0, len(vs.nsManager.namespaces))
for name := range vs.nsManager.namespaces {
names = append(names, name)
}
return names
}

// GetNamespaceDatasetCount returns the number of datasets in a namespace.
func (vs *VectorStore) GetNamespaceDatasetCount(name string) int {
vs.nsManager.mu.RLock()
defer vs.nsManager.mu.RUnlock()

ns, exists := vs.nsManager.namespaces[name]
if !exists {
return 0
}
return ns.DatasetCount()
}

// GetTotalNamespaceCount returns the total number of namespaces.
func (vs *VectorStore) GetTotalNamespaceCount() int {
vs.nsManager.mu.RLock()
defer vs.nsManager.mu.RUnlock()
return len(vs.nsManager.namespaces)
}

// GetNamespace returns a namespace by name, or nil if not found.
func (vs *VectorStore) GetNamespace(name string) *Namespace {
vs.nsManager.mu.RLock()
defer vs.nsManager.mu.RUnlock()
return vs.nsManager.namespaces[name]
}

// ParseNamespacedPath parses a path into namespace and dataset components.
// Format: "namespace/dataset" or "dataset" (uses "default" namespace)
// Examples:
//   - "tenant1/mydata" -> ("tenant1", "mydata")
//   - "mydata" -> ("default", "mydata")
//   - "org/project/data" -> ("org", "project/data")
func ParseNamespacedPath(path string) (namespace, dataset string) {
// Paths starting with "/" use default namespace with literal path
if strings.HasPrefix(path, "/") {
return "default", strings.TrimPrefix(path, "/")
}

if path == "" {
return "default", ""
}

// Split on first slash
idx := strings.Index(path, "/")
if idx == -1 {
// No slash - use default namespace
return "default", path
}

// First part is namespace, rest is dataset
return path[:idx], path[idx+1:]
}

// BuildNamespacedPath combines namespace and dataset into a path.
func BuildNamespacedPath(namespace, dataset string) string {
if namespace == "default" {
return dataset
}
return namespace + "/" + dataset
}
