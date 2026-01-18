package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaEvolution_Additive(t *testing.T) {
	// Initial Schema: id (int64), vector (fixed_size_list<float32>[4])
	initialFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}
	initialSchema := arrow.NewSchema(initialFields, nil)

	manager := NewSchemaEvolutionManager(initialSchema, "test_dataset")

	// 1. Add new column "tags" (string)
	newFields := append(initialFields, arrow.Field{Name: "tags", Type: arrow.BinaryTypes.String}) //nolint:gocritic
	newSchema := arrow.NewSchema(newFields, nil)

	err := manager.Evolve(newSchema)
	require.NoError(t, err)

	// Verify current schema
	current := manager.GetCurrentSchema()
	assert.Equal(t, 3, current.NumFields())
	assert.Equal(t, "tags", current.Field(2).Name)
	assert.Equal(t, uint64(2), manager.GetCurrentVersion())

	// 2. Add another column "score" (float64)
	newFields2 := append(newFields, arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64}) //nolint:gocritic
	newSchema2 := arrow.NewSchema(newFields2, nil)

	err = manager.Evolve(newSchema2)
	require.NoError(t, err)

	current = manager.GetCurrentSchema()
	assert.Equal(t, 4, current.NumFields())
	assert.Equal(t, "score", current.Field(3).Name)
	assert.Equal(t, uint64(3), manager.GetCurrentVersion())
}

func TestSchemaEvolution_TypeMismatch(t *testing.T) {
	initialFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}
	initialSchema := arrow.NewSchema(initialFields, nil)

	manager := NewSchemaEvolutionManager(initialSchema, "test_dataset")

	// Try to change "id" to String
	tmFields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
	}
	tmSchema := arrow.NewSchema(tmFields, nil)

	err := manager.Evolve(tmSchema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

func TestSchemaEvolution_DroppedColumnReused(t *testing.T) {
	initialFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "temp", Type: arrow.PrimitiveTypes.Int32},
	}
	initialSchema := arrow.NewSchema(initialFields, nil)

	manager := NewSchemaEvolutionManager(initialSchema, "test_dataset")

	// Drop "temp"
	err := manager.DropColumn("temp")
	require.NoError(t, err)

	assert.False(t, manager.IsColumnAvailable("temp", manager.GetCurrentVersion()))
	assert.True(t, manager.IsColumnDropped("temp"))

	// Try to re-add "temp" via Evolve
	// Even with same type, we currently disallow reuse of dropped names (per implementation)
	reAddFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "temp", Type: arrow.PrimitiveTypes.Int32},
	}
	reAddSchema := arrow.NewSchema(reAddFields, nil)

	err = manager.Evolve(reAddSchema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "was dropped")
}
