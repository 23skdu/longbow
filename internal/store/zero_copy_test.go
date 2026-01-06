package store


import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/23skdu/longbow/internal/query"
)

func TestZeroCopyRecordBatch(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "float64", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// Helper to create a batch
	createBatch := func(ids []int64, vals []float64) arrow.RecordBatch {
		b0 := array.NewInt64Builder(pool)
		defer b0.Release()
		b0.AppendValues(ids, nil)
		c0 := b0.NewInt64Array()
		defer c0.Release()

		b1 := array.NewFloat64Builder(pool)
		defer b1.Release()
		b1.AppendValues(vals, nil)
		c1 := b1.NewFloat64Array()
		defer c1.Release()

		return array.NewRecordBatch(schema, []arrow.Array{c0, c1}, int64(len(ids)))
	}

	t.Run("No Deletions", func(t *testing.T) {
		rec := createBatch([]int64{1, 2, 3}, []float64{1.1, 2.2, 3.3})
		defer rec.Release()

		// Empty bitset
		deleted := query.NewBitset()

		res, err := ZeroCopyRecordBatch(pool, rec, deleted)
		require.NoError(t, err)
		defer res.Release()

		assert.Equal(t, int64(3), res.NumRows())
		assert.Equal(t, rec, res, "Should preserve original batch if no deletions")
	})

	t.Run("Partial Deletions", func(t *testing.T) {
		rec := createBatch([]int64{1, 2, 3, 4, 5}, []float64{10.0, 20.0, 30.0, 40.0, 50.0})
		defer rec.Release()

		// Delete indices 1 (row2) and 3 (row4)
		deleted := query.NewBitset()
		deleted.Set(1)
		deleted.Set(3)

		res, err := ZeroCopyRecordBatch(pool, rec, deleted)
		require.NoError(t, err)
		defer res.Release()

		assert.Equal(t, int64(3), res.NumRows())

		// check contents: rows 0, 2, 4 -> 1, 3, 5
		c0 := res.Column(0).(*array.Int64)
		c1 := res.Column(1).(*array.Float64)

		assert.Equal(t, int64(1), c0.Value(0))
		assert.Equal(t, int64(3), c0.Value(1))
		assert.Equal(t, int64(5), c0.Value(2))

		assert.Equal(t, 10.0, c1.Value(0))
		assert.Equal(t, 30.0, c1.Value(1))
		assert.Equal(t, 50.0, c1.Value(2))
	})

	t.Run("All Deleted", func(t *testing.T) {
		rec := createBatch([]int64{1, 2}, []float64{1.1, 2.2})
		defer rec.Release()

		deleted := query.NewBitset()
		deleted.Set(0)
		deleted.Set(1)

		res, err := ZeroCopyRecordBatch(pool, rec, deleted)
		require.NoError(t, err)
		defer res.Release()

		assert.Equal(t, int64(0), res.NumRows())
	})

	t.Run("Nil Bitset", func(t *testing.T) {
		rec := createBatch([]int64{1}, []float64{1.1})
		defer rec.Release()

		res, err := ZeroCopyRecordBatch(pool, rec, nil)
		require.NoError(t, err)
		defer res.Release()

		assert.Equal(t, int64(1), res.NumRows())
	})
}

func TestRetainRecordBatch(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "f", Type: arrow.PrimitiveTypes.Int32}}, nil,
	)
	b := array.NewInt32Builder(pool)
	defer b.Release()
	b.Append(1)
	col := b.NewInt32Array()
	defer col.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{col}, 1)

	// Initial Retain count is 1
	// RetainRecordBatch calls Retain, so count becomes 2

	res := RetainRecordBatch(rec)

	// We release rec (original)
	rec.Release()

	// res should still be valid/usable
	assert.Equal(t, int64(1), res.NumRows())
	res.Release() // Final release
}
