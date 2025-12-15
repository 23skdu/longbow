package store

import (
"database/sql"
"encoding/json"
"fmt"
"path/filepath"

_ "github.com/marcboeker/go-duckdb"
)

// DuckDBAdapter handles analytical queries on Parquet snapshots
type DuckDBAdapter struct {
dataPath string
}

func NewDuckDBAdapter(dataPath string) *DuckDBAdapter {
return &DuckDBAdapter{dataPath: dataPath}
}

// QuerySnapshot executes a SQL query against a specific dataset's snapshot
func (d *DuckDBAdapter) QuerySnapshot(datasetName string, query string) (string, error) {
snapshotPath := filepath.Join(d.dataPath, snapshotDirName, datasetName+".parquet")

// Open in-memory DuckDB
db, err := sql.Open("duckdb", "")
if err != nil {
return "", fmt.Errorf("failed to open duckdb: %w", err)
}
defer func() { _ = db.Close() }()

// Create a view for the parquet file
// We use Sprintf carefully here. In a real prod env, we'd want stricter validation of datasetName
// to prevent path traversal, though filepath.Join helps.
createViewSQL := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet('%s')", datasetName, snapshotPath)
if _, err := db.Exec(createViewSQL); err != nil {
return "", fmt.Errorf("failed to create view for snapshot: %w", err)
}

// Execute the user's query
rows, err := db.Query(query)
if err != nil {
return "", fmt.Errorf("query execution failed: %w", err)
}
defer func() { _ = rows.Close() }()

// Convert rows to JSON
columns, err := rows.Columns()
if err != nil {
return "", err
}

var result []map[string]interface{}

for rows.Next() {
values := make([]interface{}, len(columns))
valuePtrs := make([]interface{}, len(columns))
for i := range values {
valuePtrs[i] = &values[i]
}

if err := rows.Scan(valuePtrs...); err != nil {
return "", err
}

rowMap := make(map[string]interface{})
for i, col := range columns {
var v interface{}
val := values[i]
b, ok := val.([]byte)
if ok {
v = string(b)
} else {
v = val
}
rowMap[col] = v
}
result = append(result, rowMap)
}

jsonBytes, err := json.Marshal(result)
if err != nil {
return "", err
}

return string(jsonBytes), nil
}
