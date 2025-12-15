
package store

import (
"encoding/json"
"os"
"path/filepath"
"testing"

"github.com/parquet-go/parquet-go"
)

type TestRecord struct {
ID    int32     `parquet:"id"`
Value float64   `parquet:"value"`
Label string    `parquet:"label"`
}

func createTestParquetFile(t *testing.T, dir, name string) {
path := filepath.Join(dir, snapshotDirName)
if err := os.MkdirAll(path, 0755); err != nil {
t.Fatalf("failed to create snapshot dir: %v", err)
}

f, err := os.Create(filepath.Join(path, name+".parquet"))
if err != nil {
t.Fatalf("failed to create parquet file: %v", err)
}
defer f.Close()

w := parquet.NewGenericWriter[TestRecord](f)
records := []TestRecord{
{ID: 1, Value: 10.5, Label: "A"},
{ID: 2, Value: 20.0, Label: "B"},
{ID: 3, Value: 30.5, Label: "C"},
}

if _, err := w.Write(records); err != nil {
t.Fatalf("failed to write records: %v", err)
}
if err := w.Close(); err != nil {
t.Fatalf("failed to close writer: %v", err)
}
}

func TestDuckDBAdapter_QuerySnapshot(t *testing.T) {
tmpDir := t.TempDir()
adapter := NewDuckDBAdapter(tmpDir)
datasetName := "test_dataset"

createTestParquetFile(t, tmpDir, datasetName)

tests := []struct {
name      string
query     string
wantCount int
wantErr   bool
}{
{
name:      "Select All",
query:     "SELECT * FROM " + datasetName,
wantCount: 3,
wantErr:   false,
},
{
name:      "Filter Query",
query:     "SELECT * FROM " + datasetName + " WHERE id > 1",
wantCount: 2,
wantErr:   false,
},
{
name:      "Aggregation",
query:     "SELECT COUNT(*) as count FROM " + datasetName,
wantCount: 1,
wantErr:   false,
},
{
name:      "Invalid Query",
query:     "SELECT * FROM non_existent_table",
wantCount: 0,
wantErr:   true,
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
gotJSON, err := adapter.QuerySnapshot(datasetName, tt.query)
if (err != nil) != tt.wantErr {
t.Errorf("QuerySnapshot() error = %v, wantErr %v", err, tt.wantErr)
return
}
if !tt.wantErr {
var results []map[string]interface{}
if err := json.Unmarshal([]byte(gotJSON), &results); err != nil {
t.Fatalf("failed to unmarshal result json: %v", err)
}
if len(results) != tt.wantCount {
t.Errorf("QuerySnapshot() returned %d rows, want %d", len(results), tt.wantCount)
}
}
})
}
}
