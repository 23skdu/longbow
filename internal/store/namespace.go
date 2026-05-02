package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// Namespace represents a tenant isolation unit containing related datasets.
// Namespaces provide multi-tenancy support with dataset isolation.
type Namespace struct {
	Name                string
	CreatedAt           time.Time
	Metadata            map[string]string
	datasets            map[string]bool // tracks dataset names in this namespace
	mu                  sync.RWMutex
	MaxVectors          int64
	MaxDimensions       int
	MaxStorageBytes     int64
	CurrentVectors      int64
	CurrentStorageBytes int64
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
	if !n.datasets[name] {
		n.datasets[name] = true
		metrics.NamespaceDatasetsTotal.WithLabelValues(n.Name).Inc()
		// vs.logger is not available here, but we can use standard log or just wait for higher level logs
	}
}

// RemoveDataset removes a dataset from this namespace.
func (n *Namespace) RemoveDataset(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.datasets[name] {
		delete(n.datasets, name)
		metrics.NamespaceDatasetsTotal.WithLabelValues(n.Name).Dec()
	}
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

// ListDatasets returns a list of all dataset names registered in this namespace.
func (n *Namespace) ListDatasets() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	datasets := make([]string, 0, len(n.datasets))
	for name := range n.datasets {
		datasets = append(datasets, name)
	}
	return datasets
}

// SetQuota configures resource limits for the namespace, including max vectors, dimensions, and storage.
func (n *Namespace) SetQuota(maxVectors int64, maxDimensions int, maxStorageBytes int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.MaxVectors = maxVectors
	n.MaxDimensions = maxDimensions
	n.MaxStorageBytes = maxStorageBytes

	if maxVectors > 0 {
		metrics.SetNamespaceQuotaLimit(n.Name, "vectors", float64(maxVectors))
	}
	if maxDimensions > 0 {
		metrics.SetNamespaceQuotaLimit(n.Name, "dimensions", float64(maxDimensions))
	}
	if maxStorageBytes > 0 {
		metrics.SetNamespaceQuotaLimit(n.Name, "storage", float64(maxStorageBytes))
	}
}

// CheckQuota verifies if the requested resources fit within the namespace's allocated quota.
func (n *Namespace) CheckQuota(vectors int64, dimensions int, storageBytes int64) error {
	if n.MaxVectors > 0 && n.CurrentVectors+vectors > n.MaxVectors {
		return errors.New("namespace quota exceeded: max vectors")
	}
	if n.MaxDimensions > 0 && dimensions > n.MaxDimensions {
		return errors.New("namespace quota exceeded: max dimensions")
	}
	if n.MaxStorageBytes > 0 && n.CurrentStorageBytes+storageBytes > n.MaxStorageBytes {
		return errors.New("namespace quota exceeded: max storage")
	}
	return nil
}

// AddUsage increments the current resource usage counters for the namespace.
func (n *Namespace) AddUsage(vectors int64, storageBytes int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.CurrentVectors += vectors
	n.CurrentStorageBytes += storageBytes

	metrics.SetNamespaceQuotaUsed(n.Name, "vectors", float64(n.CurrentVectors))
	metrics.SetNamespaceQuotaUsed(n.Name, "storage", float64(n.CurrentStorageBytes))
	metrics.RecordNamespaceVectors(n.Name, n.CurrentVectors)
	metrics.RecordNamespaceStorage(n.Name, n.CurrentStorageBytes)
}

// RemoveUsage decrements the current resource usage counters for the namespace.
func (n *Namespace) RemoveUsage(vectors int64, storageBytes int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.CurrentVectors -= vectors
	n.CurrentStorageBytes -= storageBytes
	if n.CurrentVectors < 0 {
		n.CurrentVectors = 0
	}
	if n.CurrentStorageBytes < 0 {
		n.CurrentStorageBytes = 0
	}
}

// GetUsage returns the current vector count and storage bytes used by the namespace.
func (n *Namespace) GetUsage() (vectors int64, storageBytes int64) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.CurrentVectors, n.CurrentStorageBytes
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

	if vs.nsManager == nil {
		return errors.New("namespace manager not initialized")
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
	if vs.nsManager == nil {
		return false
	}
	vs.nsManager.mu.RLock()
	defer vs.nsManager.mu.RUnlock()
	_, exists := vs.nsManager.namespaces[name]
	return exists
}

// DeleteNamespace removes a namespace and all its associated datasets from the store.
func (vs *VectorStore) DeleteNamespace(name string) error {
	if name == "default" {
		return errors.New("cannot delete default namespace")
	}

	if vs.nsManager == nil {
		return errors.New("namespace manager not initialized")
	}

	vs.nsManager.mu.Lock()
	vs.logger.Info().Str("namespace", name).Msg("DeleteNamespace called")

	ns, exists := vs.nsManager.namespaces[name]
	if !exists {
		vs.nsManager.mu.Unlock()
		return errors.New("namespace not found: " + name)
	}

	ns.mu.Lock()

	if len(ns.datasets) > 0 {
		// Recursively delete all datasets in this namespace
		dsNames := make([]string, 0, len(ns.datasets))
		for dsName := range ns.datasets {
			dsNames = append(dsNames, dsName)
		}
		
		vs.logger.Info().Str("namespace", name).Int("datasets", len(dsNames)).Msg("Deleting namespace recursively")
		
		ns.mu.Unlock()
		vs.nsManager.mu.Unlock()

		for _, dsName := range dsNames {
			vs.logger.Info().Str("dataset", dsName).Msg("Dropping dataset during namespace deletion")
			if err := vs.DropDataset(context.Background(), dsName); err != nil {
				vs.logger.Error().Err(err).Str("dataset", dsName).Msg("Failed to drop dataset during namespace deletion")
			}
		}

		// Re-acquire locks to finish namespace deletion
		vs.nsManager.mu.Lock()
		ns = vs.nsManager.namespaces[name]
		if ns == nil {
			vs.nsManager.mu.Unlock()
			return nil // Already deleted?
		}
		ns.mu.Lock()
	}

	// Double check if it's still there after re-acquiring locks
	if vs.nsManager.namespaces[name] == ns {
		delete(vs.nsManager.namespaces, name)
		metrics.NamespacesTotal.Dec()
	}
	
	ns.mu.Unlock()
	vs.nsManager.mu.Unlock()
	return nil
}

// ListNamespaces returns all namespace names.
func (vs *VectorStore) ListNamespaces() []string {
	if vs.nsManager == nil {
		return nil
	}
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
	if vs.nsManager == nil {
		return 0
	}
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
	if vs.nsManager == nil {
		return 0
	}
	vs.nsManager.mu.RLock()
	defer vs.nsManager.mu.RUnlock()
	return len(vs.nsManager.namespaces)
}

// ListDatasetsInNamespace returns all dataset names belonging to the specified namespace.
func (vs *VectorStore) ListDatasetsInNamespace(name string) []string {
	if vs.nsManager == nil {
		return nil
	}
	vs.nsManager.mu.RLock()
	defer vs.nsManager.mu.RUnlock()

	ns, exists := vs.nsManager.namespaces[name]
	if !exists {
		return nil
	}
	return ns.ListDatasets()
}

// GetNamespace returns a namespace by name, or nil if not found.
func (vs *VectorStore) GetNamespace(name string) *Namespace {
	if vs.nsManager == nil {
		return nil
	}
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
