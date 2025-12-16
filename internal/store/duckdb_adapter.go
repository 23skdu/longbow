
package store

import (
"context"
"database/sql"
"database/sql/driver"
"fmt"
"path/filepath"

"github.com/apache/arrow-go/v18/arrow/array"
duckdb "github.com/marcboeker/go-duckdb"
)

// DuckDBAdapter handles analytical queries on Parquet snapshots
type DuckDBAdapter struct {
dataPath string
}

func NewDuckDBAdapter(dataPath string) *DuckDBAdapter {
return &DuckDBAdapter{dataPath: dataPath}
}

// QuerySnapshot executes a SQL query against a specific dataset's snapshot
// Returns a RecordReader and a cleanup function. The caller must call cleanup() when done.
func (d *DuckDBAdapter) QuerySnapshot(ctx context.Context, datasetName, query string) (array.RecordReader, func(), error) {
snapshotPath := filepath.Join(d.dataPath, snapshotDirName, datasetName+".parquet")

// Open in-memory DuckDB
db, err := sql.Open("duckdb", "")
if err != nil {
return nil, nil, fmt.Errorf("failed to open duckdb: %w", err)
}

// We need a dedicated connection to access the driver-specific Arrow interface
conn, err := db.Conn(ctx)
if err != nil {
_ = db.Close()
return nil, nil, fmt.Errorf("failed to open conn: %w", err)
}

// Initialize Arrow interface from the connection
var ar *duckdb.Arrow
err = conn.Raw(func(c interface{}) error {
dc, ok := c.(driver.Conn)
if !ok {
return fmt.Errorf("not a duckdb driver connection")
}
var err error
ar, err = duckdb.NewArrowFromConn(dc)
return err
})
if err != nil {
_ = conn.Close()
_ = db.Close()
return nil, nil, fmt.Errorf("failed to init arrow: %w", err)
}

// Create a view for the parquet file
// We use ExecContext on the specific connection to ensure visibility
createViewSQL := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_parquet('%s')", datasetName, snapshotPath)
if _, err := conn.ExecContext(ctx, createViewSQL); err != nil {
_ = conn.Close()
_ = db.Close()
return nil, nil, fmt.Errorf("failed to create view for snapshot: %w", err)
}

// Execute the user's query via Arrow interface
rdr, err := ar.QueryContext(ctx, query)
if err != nil {
_ = conn.Close()
_ = db.Close()
return nil, nil, fmt.Errorf("query execution failed: %w", err)
}

cleanup := func() {
rdr.Release()
_ = conn.Close()
_ = db.Close()
}

return rdr, cleanup, nil
}
