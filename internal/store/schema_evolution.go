package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
)

// SchemaVersion tracks a specific schema state
type SchemaVersion struct {
	Version   uint64
	Fields    []arrow.Field
	CreatedAt time.Time
}

// ColumnMetadata tracks column lifecycle
type ColumnMetadata struct {
	Name      string
	Type      arrow.DataType
	AddedAt   uint64 // version when added
	DroppedAt uint64 // version when dropped (0 = not dropped)
}

// SchemaEvolutionManager handles dynamic schema changes without dataset locks
type SchemaEvolutionManager struct {
	mu          sync.RWMutex
	currentVer  atomic.Uint64
	versions    map[uint64]*SchemaVersion
	columns     map[string]*ColumnMetadata
	columnOrder []string // maintain column order
	datasetName string   // for metrics
}

// NewSchemaEvolutionManager creates a new manager with initial schema
func NewSchemaEvolutionManager(initialSchema *arrow.Schema, datasetName string) *SchemaEvolutionManager {
	mgr := &SchemaEvolutionManager{
		versions:    make(map[uint64]*SchemaVersion),
		columns:     make(map[string]*ColumnMetadata),
		datasetName: datasetName,
	}

	if initialSchema != nil {
		mgr.currentVer.Store(1)
		var fields []arrow.Field
		for _, field := range initialSchema.Fields() {
			mgr.columns[field.Name] = &ColumnMetadata{
				Name:    field.Name,
				Type:    field.Type,
				AddedAt: 1,
			}
			mgr.columnOrder = append(mgr.columnOrder, field.Name)
			fields = append(fields, field)
		}

		mgr.versions[1] = &SchemaVersion{
			Version:   1,
			Fields:    fields,
			CreatedAt: time.Now(),
		}
	}

	// Update metrics
	metrics.SchemaVersionCurrent.WithLabelValues(datasetName).Set(1)

	return mgr
}

// GetCurrentVersion returns the current schema version
func (m *SchemaEvolutionManager) GetCurrentVersion() uint64 {
	return m.currentVer.Load()
}

// GetCurrentSchema returns the current schema (excluding dropped columns)
func (m *SchemaEvolutionManager) GetCurrentSchema() *arrow.Schema {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var fields []arrow.Field
	for _, name := range m.columnOrder {
		col := m.columns[name]
		if col.DroppedAt == 0 { // not dropped
			fields = append(fields, arrow.Field{
				Name: col.Name,
				Type: col.Type,
			})
		}
	}

	return arrow.NewSchema(fields, nil)
}

// AddColumn adds a new column without requiring dataset lock or rewrite
func (m *SchemaEvolutionManager) AddColumn(name string, dtype arrow.DataType) error {
	start := time.Now()
	defer func() {
		metrics.SchemaEvolutionDuration.Observe(time.Since(start).Seconds())
	}()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if column already exists (and not dropped)
	if existing, ok := m.columns[name]; ok && existing.DroppedAt == 0 {
		return fmt.Errorf("column %q already exists", name)
	}

	// If column was previously dropped, we can re-add it
	// But for simplicity, treat as error (common practice)
	if existing, ok := m.columns[name]; ok && existing.DroppedAt > 0 {
		return fmt.Errorf("column %q was previously dropped; use a different name", name)
	}

	// Increment version
	newVer := m.currentVer.Add(1)

	// Add column metadata
	m.columns[name] = &ColumnMetadata{
		Name:      name,
		Type:      dtype,
		AddedAt:   newVer,
		DroppedAt: 0,
	}
	m.columnOrder = append(m.columnOrder, name)

	// Create new schema version (copy all active fields)
	var fields []arrow.Field
	for _, colName := range m.columnOrder {
		col := m.columns[colName]
		if col.DroppedAt == 0 {
			fields = append(fields, arrow.Field{
				Name: col.Name,
				Type: col.Type,
			})
		}
	}

	m.versions[newVer] = &SchemaVersion{
		Version:   newVer,
		Fields:    fields,
		CreatedAt: time.Now(),
	}

	// Update metrics
	metrics.SchemaVersionCurrent.WithLabelValues(m.datasetName).Set(float64(newVer))
	metrics.SchemaColumnsAddedTotal.Inc()

	return nil
}

// DropColumn marks a column as dropped without rewriting data
func (m *SchemaEvolutionManager) DropColumn(name string) error {
	start := time.Now()
	defer func() {
		metrics.SchemaEvolutionDuration.Observe(time.Since(start).Seconds())
	}()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if column exists
	col, ok := m.columns[name]
	if !ok {
		return fmt.Errorf("column %q does not exist", name)
	}

	// Check if already dropped
	if col.DroppedAt > 0 {
		return fmt.Errorf("column %q is already dropped", name)
	}

	// Increment version
	newVer := m.currentVer.Add(1)

	// Mark column as dropped at this version
	col.DroppedAt = newVer

	// Create new schema version (copy all active fields)
	var fields []arrow.Field
	for _, colName := range m.columnOrder {
		c := m.columns[colName]
		if c.DroppedAt == 0 {
			fields = append(fields, arrow.Field{
				Name: c.Name,
				Type: c.Type,
			})
		}
	}

	m.versions[newVer] = &SchemaVersion{
		Version:   newVer,
		Fields:    fields,
		CreatedAt: time.Now(),
	}

	// Update metrics
	metrics.SchemaVersionCurrent.WithLabelValues(m.datasetName).Set(float64(newVer))
	metrics.SchemaColumnsDroppedTotal.Inc()

	return nil
}

// IsColumnDropped checks if a column has been dropped
func (m *SchemaEvolutionManager) IsColumnDropped(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if col, ok := m.columns[name]; ok {
		return col.DroppedAt > 0
	}
	return false
}

// GetSchemaAtVersion returns the schema at a specific version
func (m *SchemaEvolutionManager) GetSchemaAtVersion(version uint64) *arrow.Schema {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find the schema version <= requested version
	var targetVer *SchemaVersion
	for v := version; v >= 1; v-- {
		if sv, ok := m.versions[v]; ok {
			targetVer = sv
			break
		}
	}

	if targetVer == nil {
		return nil
	}

	return arrow.NewSchema(targetVer.Fields, nil)
}

// IsColumnAvailable checks if a column is available at a specific version
func (m *SchemaEvolutionManager) IsColumnAvailable(name string, version uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	col, ok := m.columns[name]
	if !ok {
		return false
	}

	// Column must have been added at or before this version
	if col.AddedAt > version {
		return false
	}

	// Column must not have been dropped at or before this version
	if col.DroppedAt > 0 && col.DroppedAt <= version {
		return false
	}

	return true
}

// GetColumnCount returns the number of active (non-dropped) columns
func (m *SchemaEvolutionManager) GetColumnCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, col := range m.columns {
		if col.DroppedAt == 0 {
			count++
		}
	}
	return count
}

// GetVersionCount returns the number of schema versions
func (m *SchemaEvolutionManager) GetVersionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.versions)
}
