package adbc

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/pkg/version"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

var _ adbc.Connection = (*Connection)(nil)

type Connection struct {
	db     *Database
	store  *store.VectorStore
	logger zerolog.Logger
}

func newConnection(db *Database) adbc.Connection {
	vs := db.store
	if vs == nil {
		vs = db.initStore()
	}
	return &Connection{db: db, store: vs, logger: db.logger}
}

func (c *Connection) Close() error {
	return nil
}

func (c *Connection) SetOption(key, value string) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) NewStatement() (adbc.Statement, error) {
	return newStatement(c), nil
}

func (c *Connection) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	pool := memory.NewGoAllocator()

	builder := array.NewRecordBuilder(pool, adbc.GetInfoSchema)
	defer builder.Release()

	infoNameBuilder := builder.Field(0).(*array.Uint32Builder)
	unionB := builder.Field(1).(*array.DenseUnionBuilder)
	strB := unionB.Child(0).(*array.StringBuilder)
	boolB := unionB.Child(1).(*array.BooleanBuilder)
	int64B := unionB.Child(2).(*array.Int64Builder)

	wanted := make(map[adbc.InfoCode]bool)
	for _, code := range infoCodes {
		wanted[code] = true
	}

	addInfoStr := func(code adbc.InfoCode, val string) {
		if len(wanted) > 0 && !wanted[code] {
			return
		}
		infoNameBuilder.Append(uint32(code))
		unionB.Append(0)
		strB.Append(val)
	}

	addInfoBool := func(code adbc.InfoCode, val bool) {
		if len(wanted) > 0 && !wanted[code] {
			return
		}
		infoNameBuilder.Append(uint32(code))
		unionB.Append(1)
		boolB.Append(val)
	}

	addInfoInt64 := func(code adbc.InfoCode, val int64) {
		if len(wanted) > 0 && !wanted[code] {
			return
		}
		infoNameBuilder.Append(uint32(code))
		unionB.Append(2)
		int64B.Append(val)
	}

	addInfoStr(adbc.InfoVendorName, "23skdu")
	addInfoStr(adbc.InfoVendorVersion, version.Version)
	addInfoStr(adbc.InfoVendorArrowVersion, "18.8.0")
	addInfoBool(adbc.InfoVendorSql, true)
	addInfoBool(adbc.InfoVendorSubstrait, false)
	addInfoStr(adbc.InfoDriverName, "longbow-adbc")
	addInfoStr(adbc.InfoDriverVersion, version.Version)
	addInfoStr(adbc.InfoDriverArrowVersion, "18.8.0")
	addInfoInt64(adbc.InfoDriverADBCVersion, adbc.AdbcVersion1_1_0)

	rec := builder.NewRecordBatch()
	return array.NewRecordReader(adbc.GetInfoSchema, []arrow.RecordBatch{rec})
}

func (c *Connection) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	if c.store == nil {
		return nil, adbc.Error{Code: adbc.StatusNotFound, Msg: "no store attached"}
	}

	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String},
		{Name: "db_schema_name", Type: arrow.BinaryTypes.String},
		{Name: "table_name", Type: arrow.BinaryTypes.String},
		{Name: "table_type", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	catalogB := builder.Field(0).(*array.StringBuilder)
	schemaB := builder.Field(1).(*array.StringBuilder)
	tableB := builder.Field(2).(*array.StringBuilder)
	typeB := builder.Field(3).(*array.StringBuilder)

	c.store.IterateDatasets(func(name string, ds *store.Dataset) {
		catalogB.Append("longbow")
		schemaB.Append("default")
		tableB.Append(name)
		typeB.Append("table")
	})

	rec := builder.NewRecordBatch()
	return array.NewRecordReader(schema, []arrow.RecordBatch{rec})
}

func (c *Connection) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	if c.store == nil {
		return nil, adbc.Error{Code: adbc.StatusNotFound, Msg: "no store attached"}
	}

	var found *arrow.Schema
	c.store.IterateDatasets(func(name string, ds *store.Dataset) {
		if name == tableName {
			found = ds.GetSchema()
		}
	})

	if found == nil {
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  fmt.Sprintf("dataset %q not found", tableName),
		}
	}

	return found, nil
}

func (c *Connection) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "table_type", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	typeBuilder := builder.Field(0).(*array.StringBuilder)
	typeBuilder.Append("table")
	typeBuilder.Append("view")

	rec := builder.NewRecordBatch()
	return array.NewRecordReader(schema, []arrow.RecordBatch{rec})
}

func (c *Connection) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) Commit(ctx context.Context) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) Rollback(ctx context.Context) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) FlightServer() flight.FlightServer {
	return nil
}

func parseTableFromSQL(sql string) string {
	sql = strings.TrimSpace(sql)
	upper := strings.ToUpper(sql)

	if idx := strings.Index(upper, " FROM "); idx >= 0 {
		rest := strings.TrimSpace(sql[idx+6:])
		end := strings.IndexAny(rest, " \t\n;,()")
		if end < 0 {
			return rest
		}
		return rest[:end]
	}

	if idx := strings.Index(upper, " INTO "); idx >= 0 {
		rest := strings.TrimSpace(sql[idx+6:])
		end := strings.IndexAny(rest, " \t\n;,()")
		if end < 0 {
			return rest
		}
		return rest[:end]
	}

	if idx := strings.Index(upper, " DELETE FROM "); idx >= 0 {
		rest := strings.TrimSpace(sql[idx+13:])
		end := strings.IndexAny(rest, " \t\n;,()")
		if end < 0 {
			return rest
		}
		return rest[:end]
	}

	return strings.TrimSpace(sql)
}

func parseKFromSQL(sql string) int {
	upper := strings.ToUpper(sql)
	if idx := strings.Index(upper, " LIMIT "); idx >= 0 {
		rest := strings.TrimSpace(sql[idx+7:])
		end := strings.IndexAny(rest, " \t\n;,")
		numStr := rest
		if end >= 0 {
			numStr = rest[:end]
		}
		if k, err := strconv.Atoi(numStr); err == nil && k > 0 {
			return k
		}
	}
	return 10
}
