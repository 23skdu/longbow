
package store

import (
"context"
"os"
"path/filepath"
"testing"

"github.com/parquet-go/parquet-go"
"github.com/stretchr/testify/require"
)

func TestDuckDBAdapter_QuerySnapshot(t *testing.T) {
// Setup temporary directory
tmpDir := t.TempDir()
snapshotDir := filepath.Join(tmpDir, snapshotDirName)
err := os.MkdirAll(snapshotDir, 0o755)
require.NoError(t, err)

// Create a dummy parquet file
datasetName := "test_dataset"
filePath := filepath.Join(snapshotDir, datasetName+".parquet")

type Row struct {
ID    int32   `parquet:"id"`
Value float64 `parquet:"value"`
}

rows := []Row{
{ID: 1, Value: 1.1},
{ID: 2, Value: 2.2},
{ID: 3, Value: 3.3},
}

err = parquet.WriteFile(filePath, rows)
require.NoError(t, err)

adapter := NewDuckDBAdapter(tmpDir)

tests := []struct {
name      string
query     string
wantErr   bool
wantCount int64
}{
{
name:      "Select All",
query:     "SELECT * FROM test_dataset",
wantErr:   false,
wantCount: 3,
},
{
name:      "Filter",
query:     "SELECT * FROM test_dataset WHERE id > 1",
wantErr:   false,
wantCount: 2,
},
{
name:      "Invalid Query",
query:     "SELECT * FROM non_existent",
wantErr:   true,
wantCount: 0,
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
rdr, cleanup, err := adapter.QuerySnapshot(context.Background(), datasetName, tt.query)
if tt.wantErr {
require.Error(t, err)
return
}
require.NoError(t, err)
defer cleanup()

var count int64
for rdr.Next() {
rec := rdr.RecordBatch()
count += rec.NumRows()
}
require.NoError(t, rdr.Err())
require.Equal(t, tt.wantCount, count)
})
}
}
