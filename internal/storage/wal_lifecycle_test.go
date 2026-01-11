package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWAL_Lifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	wal := NewWAL(tmpDir)

	// Create a sample record batch
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vec", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	valBuilder.AppendValues([]float32{1.1, 1.2, 2.1, 2.2}, nil)
	vecBuilder.Append(true)
	vecBuilder.Append(true)

	rec := b.NewRecordBatch()
	defer rec.Release()

	// 1. Write
	err := wal.Write("test_dataset", 1, time.Now().UnixNano(), rec)
	require.NoError(t, err)

	// 2. Sync
	err = wal.Sync()
	require.NoError(t, err)

	// 3. Close
	err = wal.Close()
	require.NoError(t, err)

	// 4. Re-open (simulated by creating new WAL instance pointing to same dir)
	// Currently StdWAL keeps appending.
	// We need a Recovery verification mechanism (Reader).
	// Since we don't have a WALReader exposed in interface yet (it is likely implicitly in recovery logic),
	// we will check file existence and size for now.

	walFile := tmpDir + "/" + walFileName
	info, err := os.Stat(walFile)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))

	// Verify Header structure (basic check)
	data, err := os.ReadFile(walFile)
	require.NoError(t, err)
	// Header is 32 bytes
	assert.GreaterOrEqual(t, len(data), 32)

	// Checksum (4) | Seq (8) ...
	// Just verify we wrote something significant
	fmt.Printf("WAL file size: %d bytes\n", len(data))
}

func TestWAL_MultiWrite(t *testing.T) {
	tmpDir := t.TempDir()
	wal := NewWAL(tmpDir)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	for i := 0; i < 10; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		rec := b.NewRecordBatch()
		err := wal.Write("dataset", uint64(i), time.Now().UnixNano(), rec)
		require.NoError(t, err)
		rec.Release()
	}

	err := wal.Sync()
	require.NoError(t, err)
}
