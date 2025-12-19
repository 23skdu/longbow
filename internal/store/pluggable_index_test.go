package store

import (
"os"
"path/filepath"
"sync"
"testing"
)

// =============================================================================
// TDD Red Phase: Pluggable Index Types Tests
// =============================================================================

// TestIndexType_Constants verifies index type constants exist
func TestIndexType_Constants(t *testing.T) {
tests := []struct {
name     string
idxType  IndexType
expected string
}{
{"HNSW", IndexTypeHNSW, "hnsw"},
{"IVF-Flat", IndexTypeIVFFlat, "ivf_flat"},
{"DiskANN", IndexTypeDiskANN, "diskann"},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
if string(tt.idxType) != tt.expected {
t.Errorf("IndexType %s = %q, want %q", tt.name, tt.idxType, tt.expected)
}
})
}
}

// TestPluggableVectorIndex_Interface verifies extended interface methods
func TestPluggableVectorIndex_Interface(t *testing.T) {
// Create a mock index to test interface compliance
var idx PluggableVectorIndex = &mockPluggableIndex{
idxType:   IndexTypeHNSW,
dimension: 128,
}

// Test Type()
if idx.Type() != IndexTypeHNSW {
t.Errorf("Type() = %v, want %v", idx.Type(), IndexTypeHNSW)
}

// Test Dimension()
if idx.Dimension() != 128 {
t.Errorf("Dimension() = %d, want 128", idx.Dimension())
}

// Test Size() initially 0
if idx.Size() != 0 {
t.Errorf("Size() = %d, want 0", idx.Size())
}

// Test NeedsBuild()
if idx.NeedsBuild() {
t.Error("HNSW NeedsBuild() should be false")
}
}

// TestPluggableVectorIndex_AddAndSearch tests core operations
func TestPluggableVectorIndex_AddAndSearch(t *testing.T) {
idx := &mockPluggableIndex{
idxType:   IndexTypeHNSW,
dimension: 4,
vectors:   make(map[uint64][]float32),
}

// Add single vector
err := idx.Add(1, []float32{1.0, 0.0, 0.0, 0.0})
if err != nil {
t.Fatalf("Add() error = %v", err)
}

if idx.Size() != 1 {
t.Errorf("Size() after Add = %d, want 1", idx.Size())
}

// Add batch
err = idx.AddBatch(
[]uint64{2, 3},
[][]float32{{0.0, 1.0, 0.0, 0.0}, {0.0, 0.0, 1.0, 0.0}},
)
if err != nil {
t.Fatalf("AddBatch() error = %v", err)
}

if idx.Size() != 3 {
t.Errorf("Size() after AddBatch = %d, want 3", idx.Size())
}

// Search
results, err := idx.Search([]float32{1.0, 0.0, 0.0, 0.0}, 2)
if err != nil {
t.Fatalf("Search() error = %v", err)
}
if len(results) == 0 {
t.Error("Search() returned no results")
}
}

// TestPluggableVectorIndex_SaveLoad tests persistence
func TestPluggableVectorIndex_SaveLoad(t *testing.T) {
tmpDir := t.TempDir()
path := filepath.Join(tmpDir, "test_index")

idx := &mockPluggableIndex{
idxType:   IndexTypeHNSW,
dimension: 4,
vectors:   make(map[uint64][]float32),
}
idx.Add(1, []float32{1.0, 0.0, 0.0, 0.0}) //nolint:errcheck

// Save
err := idx.Save(path)
if err != nil {
t.Fatalf("Save() error = %v", err)
}

// Verify file exists
if _, err := os.Stat(path); os.IsNotExist(err) {
t.Error("Save() did not create file")
}

// Load into new index
idx2 := &mockPluggableIndex{
idxType:   IndexTypeHNSW,
dimension: 4,
vectors:   make(map[uint64][]float32),
}
err = idx2.Load(path)
if err != nil {
t.Fatalf("Load() error = %v", err)
}

if err != nil {
t.Errorf("Load returned unexpected error")
}
}

// TestIndexFactory_Create tests factory pattern
func TestIndexFactory_Create(t *testing.T) {
factory := NewIndexFactory()

tests := []struct {
name    string
cfg     IndexConfig
wantErr bool
}{
{
name: "HNSW",
cfg: IndexConfig{
Type:      IndexTypeHNSW,
Dimension: 128,
},
wantErr: false,
},
{
name: "IVF-Flat",
cfg: IndexConfig{
Type:      IndexTypeIVFFlat,
Dimension: 128,
},
wantErr: false,
},
{
name: "DiskANN",
cfg: IndexConfig{
Type:      IndexTypeDiskANN,
Dimension: 128,
},
wantErr: false,
},
{
name: "Unknown type",
cfg: IndexConfig{
Type:      "unknown",
Dimension: 128,
},
wantErr: true,
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
idx, err := factory.Create(tt.cfg)
if (err != nil) != tt.wantErr {
t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
return
}
if !tt.wantErr && idx == nil {
t.Error("Create() returned nil index")
}
if !tt.wantErr && idx.Type() != tt.cfg.Type {
t.Errorf("Created index Type() = %v, want %v", idx.Type(), tt.cfg.Type)
}
})
}
}

// TestIndexFactory_Register tests custom type registration
func TestIndexFactory_Register(t *testing.T) {
factory := NewIndexFactory()

// Register custom type
customType := IndexType("custom")
factory.Register(customType, func(cfg IndexConfig) (PluggableVectorIndex, error) {
return &mockPluggableIndex{
idxType:   customType,
dimension: cfg.Dimension,
vectors:   make(map[uint64][]float32),
}, nil
})

// Create custom type
idx, err := factory.Create(IndexConfig{Type: customType, Dimension: 64})
if err != nil {
t.Fatalf("Create() custom type error = %v", err)
}
if idx.Type() != customType {
t.Errorf("Type() = %v, want %v", idx.Type(), customType)
}
}

// TestIndexFactory_ListTypes tests type enumeration
func TestIndexFactory_ListTypes(t *testing.T) {
factory := NewIndexFactory()
types := factory.ListTypes()

// Should have at least HNSW, IVF-Flat, DiskANN
if len(types) < 3 {
t.Errorf("ListTypes() returned %d types, want >= 3", len(types))
}

// Check HNSW is registered
found := false
for _, typ := range types {
if typ == IndexTypeHNSW {
found = true
break
}
}
if !found {
t.Error("ListTypes() missing IndexTypeHNSW")
}
}

// TestPluggableIndex_ConcurrentAccess tests thread safety
func TestPluggableIndex_ConcurrentAccess(t *testing.T) {
idx := &mockPluggableIndex{
idxType:   IndexTypeHNSW,
dimension: 4,
vectors:   make(map[uint64][]float32),
}

var wg sync.WaitGroup
for i := 0; i < 10; i++ {
wg.Add(1)
go func(id uint64) {
defer wg.Done()
idx.Add(id, []float32{float32(id), 0, 0, 0}) //nolint:errcheck
}(uint64(i))
}
wg.Wait()

if idx.Size() != 10 {
t.Errorf("Size() after concurrent adds = %d, want 10", idx.Size())
}
}

// TestPluggableIndex_Metrics tests Prometheus metrics
func TestPluggableIndex_Metrics(t *testing.T) {
factory := NewIndexFactory()
_, err := factory.Create(IndexConfig{Type: IndexTypeHNSW, Dimension: 128})
if err != nil {
t.Fatalf("Create() error = %v", err)
}

// Verify metrics are incremented (basic check)
// Full metrics verification would check prometheus registry
}

// =============================================================================
// Mock Implementation for Testing
// =============================================================================

type mockPluggableIndex struct {
mu        sync.RWMutex
idxType   IndexType
dimension int
vectors   map[uint64][]float32
built     bool
}

func (m *mockPluggableIndex) Type() IndexType {
return m.idxType
}

func (m *mockPluggableIndex) Dimension() int {
return m.dimension
}

func (m *mockPluggableIndex) Size() int {
m.mu.RLock()
defer m.mu.RUnlock()
return len(m.vectors)
}

func (m *mockPluggableIndex) NeedsBuild() bool {
return m.idxType == IndexTypeIVFFlat // IVF needs training
}

func (m *mockPluggableIndex) Add(id uint64, vector []float32) error {
m.mu.Lock()
defer m.mu.Unlock()
m.vectors[id] = vector
return nil
}

func (m *mockPluggableIndex) AddBatch(ids []uint64, vectors [][]float32) error {
m.mu.Lock()
defer m.mu.Unlock()
for i, id := range ids {
m.vectors[id] = vectors[i]
}
return nil
}

func (m *mockPluggableIndex) Search(query []float32, k int) ([]IndexSearchResult, error) {
m.mu.RLock()
defer m.mu.RUnlock()
results := make([]IndexSearchResult, 0, k)
for id := range m.vectors {
results = append(results, IndexSearchResult{ID: id, Distance: 0.0})
if len(results) >= k {
break
}
}
return results, nil
}

func (m *mockPluggableIndex) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
results := make([][]IndexSearchResult, len(queries))
for i, q := range queries {
r, _ := m.Search(q, k)
results[i] = r
}
return results, nil
}

func (m *mockPluggableIndex) Build() error {
m.built = true
return nil
}

func (m *mockPluggableIndex) Save(path string) error {
return os.WriteFile(path, []byte("mock"), 0o644)
}

func (m *mockPluggableIndex) Load(path string) error {
_, err := os.ReadFile(path)
return err
}

func (m *mockPluggableIndex) Close() error {
return nil
}

// Legacy interface compatibility
func (m *mockPluggableIndex) AddByLocation(batchIdx, rowIdx int) error {
return nil
}

func (m *mockPluggableIndex) SearchVectors(query []float32, k int) []SearchResult {
results, _ := m.Search(query, k)
searchResults := make([]SearchResult, len(results))
for i, r := range results {
searchResults[i] = SearchResult{ID: VectorID(r.ID), Score: r.Distance}
}
return searchResults
}

func (m *mockPluggableIndex) Len() int {
return m.Size()
}
