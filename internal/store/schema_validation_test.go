package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaEvolution_Validation(t *testing.T) {
	mem := memory.NewGoAllocator()
	pool := memory.NewGoAllocator()
	_ = mem
	_ = pool

	// Initial Schema
	initialFields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
	}
	initialSchema := arrow.NewSchema(initialFields, nil)

	mgr := NewSchemaEvolutionManager(initialSchema, "test_dataset")

	// Case 1: Additive Change (New Column)
	t.Run("AdditiveChange", func(t *testing.T) {
		newFields := append([]arrow.Field{}, initialFields...)
		newFields = append(newFields, arrow.Field{Name: "tags", Type: arrow.BinaryTypes.String})
		newSchema := arrow.NewSchema(newFields, nil) // nolint:govet // shadow ok

		err := mgr.ValidateCompatibility(newSchema)
		require.NoError(t, err)

		err = mgr.Evolve(newSchema)
		require.NoError(t, err)

		assert.Equal(t, 3, mgr.GetColumnCount())
	})

	// Case 2: Type Mismatch (Existing Column)
	t.Run("TypeMismatch", func(t *testing.T) {
		// Try changing 'id' to Int64
		mismatchFields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		}
		mismatchSchema := arrow.NewSchema(mismatchFields, nil)

		err := mgr.ValidateCompatibility(mismatchSchema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "type mismatch")
	})

	// Case 3: Dropped Column Reuse
	t.Run("DroppedColumnReuse", func(t *testing.T) {
		// Drop 'tags'
		err := mgr.DropColumn("tags")
		require.NoError(t, err)
		assert.True(t, mgr.IsColumnDropped("tags"))

		// Try to re-add 'tags' via schema evolution
		reuseFields := []arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
			{Name: "tags", Type: arrow.BinaryTypes.String},
		}
		reuseSchema := arrow.NewSchema(reuseFields, nil)

		err = mgr.ValidateCompatibility(reuseSchema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was dropped")
	})

	// Case 4: Partial Schema (Subset of columns) -> Should be allowed
	t.Run("PartialSchema", func(t *testing.T) {
		// Schema with only 'id'
		partialFields := []arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
		}
		partialSchema := arrow.NewSchema(partialFields, nil)

		err := mgr.ValidateCompatibility(partialSchema)
		require.NoError(t, err)
		// partial schema shouldn't trigger errors or drops
	})
}
