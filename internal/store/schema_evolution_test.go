package store

import (
"sync"
"testing"

"github.com/apache/arrow-go/v18/arrow"
)

// TDD Red Phase: Tests for SchemaEvolutionManager

func TestNewSchemaEvolutionManager(t *testing.T) {
initialSchema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
},
nil,
)

mgr := NewSchemaEvolutionManager(initialSchema)
if mgr == nil {
t.Fatal("Expected non-nil SchemaEvolutionManager")
}

if mgr.GetCurrentVersion() != 1 {
t.Errorf("Expected initial version 1, got %d", mgr.GetCurrentVersion())
}
}

func TestSchemaEvolutionManager_AddColumn(t *testing.T) {
initialSchema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
},
nil,
)

mgr := NewSchemaEvolutionManager(initialSchema)

// Add a new column
err := mgr.AddColumn("name", arrow.BinaryTypes.String)
if err != nil {
t.Fatalf("AddColumn failed: %v", err)
}

// Version should increment
if mgr.GetCurrentVersion() != 2 {
t.Errorf("Expected version 2 after AddColumn, got %d", mgr.GetCurrentVersion())
}

// New schema should have the column
schema := mgr.GetCurrentSchema()
if schema.NumFields() != 2 {
t.Errorf("Expected 2 fields, got %d", schema.NumFields())
}

// Check column exists
idx, found := schema.FieldsByName("name")
if !found || len(idx) == 0 {
t.Error("Expected 'name' column to exist")
}
}

func TestSchemaEvolutionManager_AddColumn_DuplicateError(t *testing.T) {
initialSchema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
},
nil,
)

mgr := NewSchemaEvolutionManager(initialSchema)

// Try to add existing column - should error
err := mgr.AddColumn("id", arrow.PrimitiveTypes.Int32)
if err == nil {
t.Error("Expected error when adding duplicate column")
}
}

func TestSchemaEvolutionManager_DropColumn(t *testing.T) {
initialSchema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "name", Type: arrow.BinaryTypes.String},
{Name: "value", Type: arrow.PrimitiveTypes.Float64},
},
nil,
)

mgr := NewSchemaEvolutionManager(initialSchema)

// Drop a column
err := mgr.DropColumn("name")
if err != nil {
t.Fatalf("DropColumn failed: %v", err)
}

// Version should increment
if mgr.GetCurrentVersion() != 2 {
t.Errorf("Expected version 2 after DropColumn, got %d", mgr.GetCurrentVersion())
}

// Column should be marked as dropped
if !mgr.IsColumnDropped("name") {
t.Error("Expected 'name' column to be marked as dropped")
}

// GetCurrentSchema should exclude dropped columns
schema := mgr.GetCurrentSchema()
_, found := schema.FieldsByName("name")
if found {
t.Error("Dropped column should not appear in current schema")
}
}

func TestSchemaEvolutionManager_DropColumn_NotFoundError(t *testing.T) {
initialSchema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
},
nil,
)

mgr := NewSchemaEvolutionManager(initialSchema)

// Try to drop non-existent column
err := mgr.DropColumn("nonexistent")
if err == nil {
t.Error("Expected error when dropping non-existent column")
}
}

func TestSchemaEvolutionManager_GetSchemaAtVersion(t *testing.T) {
initialSchema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
},
nil,
)

mgr := NewSchemaEvolutionManager(initialSchema)

// Add column to create version 2
_ = mgr.AddColumn("name", arrow.BinaryTypes.String)

// Get version 1 schema
v1Schema := mgr.GetSchemaAtVersion(1)
if v1Schema == nil {
t.Fatal("Expected non-nil schema for version 1")
}
if v1Schema.NumFields() != 1 {
t.Errorf("Version 1 should have 1 field, got %d", v1Schema.NumFields())
}

// Get version 2 schema
v2Schema := mgr.GetSchemaAtVersion(2)
if v2Schema == nil {
t.Fatal("Expected non-nil schema for version 2")
}
if v2Schema.NumFields() != 2 {
t.Errorf("Version 2 should have 2 fields, got %d", v2Schema.NumFields())
}
}

func TestSchemaEvolutionManager_IsColumnAvailable(t *testing.T) {
initialSchema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
},
nil,
)

mgr := NewSchemaEvolutionManager(initialSchema)

// Add column at version 2
_ = mgr.AddColumn("name", arrow.BinaryTypes.String)

// 'id' available at all versions
if !mgr.IsColumnAvailable("id", 1) {
t.Error("'id' should be available at version 1")
}
if !mgr.IsColumnAvailable("id", 2) {
t.Error("'id' should be available at version 2")
}

// 'name' only available from version 2
if mgr.IsColumnAvailable("name", 1) {
t.Error("'name' should NOT be available at version 1")
}
if !mgr.IsColumnAvailable("name", 2) {
t.Error("'name' should be available at version 2")
}
}

func TestSchemaEvolutionManager_ConcurrentAccess(t *testing.T) {
initialSchema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
},
nil,
)

mgr := NewSchemaEvolutionManager(initialSchema)

var wg sync.WaitGroup
const goroutines = 10

// Concurrent reads
for i := 0; i < goroutines; i++ {
wg.Add(1)
go func() {
defer wg.Done()
_ = mgr.GetCurrentVersion()
_ = mgr.GetCurrentSchema()
}()
}

// Concurrent writes
for i := 0; i < goroutines; i++ {
wg.Add(1)
go func(idx int) {
defer wg.Done()
colName := "col_" + string(rune('a'+idx))
_ = mgr.AddColumn(colName, arrow.PrimitiveTypes.Int32)
}(i)
}

wg.Wait()

// Should complete without race/panic
if mgr.GetCurrentVersion() < 2 {
t.Error("Expected version to increase from concurrent adds")
}
}

func TestSchemaEvolutionManager_Metrics(t *testing.T) {
initialSchema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
},
nil,
)

mgr := NewSchemaEvolutionManager(initialSchema)

// Add column
_ = mgr.AddColumn("col1", arrow.BinaryTypes.String)
_ = mgr.AddColumn("col2", arrow.PrimitiveTypes.Float64)

// Drop column
_ = mgr.DropColumn("col1")

// Verify metrics are tracked (implementation should update Prometheus counters)
// This test verifies no panics occur during metrics operations
}
