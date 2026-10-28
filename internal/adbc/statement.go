package adbc

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

var (
	_ adbc.Statement                         = (*Statement)(nil)
	_ adbc.StatementExecuteSchema            = (*Statement)(nil)
	_ adbc.StatementExecuteSchemaWithContext = (*Statement)(nil)
)

type Statement struct {
	conn   *Connection
	query  string
	stream array.RecordReader
	logger zerolog.Logger
}

func newStatement(conn *Connection) adbc.Statement {
	return &Statement{conn: conn, logger: conn.logger}
}

func (s *Statement) Close() error {
	return nil
}

func (s *Statement) SetOption(key, value string) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *Statement) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

func (s *Statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.conn.store == nil || strings.TrimSpace(s.query) == "" {
		return s.stubExecuteQuery()
	}
	return s.executeSearch(ctx)
}

// ExecuteSchema gets the schema of the result set of a query without executing it (ADBC 1.1.0).
func (s *Statement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	if s.conn == nil || s.conn.store == nil || strings.TrimSpace(s.query) == "" {
		return arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "score", Type: arrow.PrimitiveTypes.Float32},
		}, nil), nil
	}

	query := strings.TrimSpace(s.query)
	upper := strings.ToUpper(query)

	if upper == "SHOW TABLES" || upper == "SHOW DATASETS" {
		return arrow.NewSchema([]arrow.Field{
			{Name: "table_name", Type: arrow.BinaryTypes.String},
			{Name: "table_type", Type: arrow.BinaryTypes.String},
		}, nil), nil
	}

	if strings.HasPrefix(upper, "DESCRIBE ") || strings.HasPrefix(upper, "DESC ") {
		return arrow.NewSchema([]arrow.Field{
			{Name: "column_name", Type: arrow.BinaryTypes.String},
			{Name: "data_type", Type: arrow.BinaryTypes.String},
			{Name: "nullable", Type: arrow.FixedWidthTypes.Boolean},
		}, nil), nil
	}

	if strings.HasPrefix(upper, "SELECT") && strings.Contains(upper, " FROM ") {
		tableName := parseTableFromSQL(query)
		if tableName == "" {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "could not parse table name from query",
			}
		}

		if extractVectorFromSQL(query) != nil {
			return arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
				{Name: "score", Type: arrow.PrimitiveTypes.Float32},
			}, nil), nil
		}

		var tableSchema *arrow.Schema
		s.conn.store.IterateDatasets(func(name string, ds *store.Dataset) {
			if name == tableName {
				tableSchema = ds.GetSchema()
			}
		})
		if tableSchema == nil {
			return nil, adbc.Error{
				Code: adbc.StatusNotFound,
				Msg:  fmt.Sprintf("dataset %q not found", tableName),
			}
		}
		return tableSchema, nil
	}

	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unsupported SQL query: %q", query),
	}
}

func (s *Statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return -1, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *Statement) Prepare(ctx context.Context) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *Statement) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *Statement) Bind(ctx context.Context, values arrow.RecordBatch) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *Statement) BindStream(ctx context.Context, stream array.RecordReader) error {
	s.stream = stream
	return nil
}

func (s *Statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *Statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *Statement) stubExecuteQuery() (array.RecordReader, int64, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	reader, err := NewAdbcRecordReader(schema, nil)
	if err != nil {
		return nil, -1, err
	}

	return reader, -1, nil
}

func (s *Statement) executeSearch(ctx context.Context) (array.RecordReader, int64, error) {
	query := strings.TrimSpace(s.query)
	upper := strings.ToUpper(query)

	if query == "" {
		return s.stubExecuteQuery()
	}

	if upper == "SHOW TABLES" || upper == "SHOW DATASETS" {
		return s.showTables(ctx)
	}

	if strings.HasPrefix(upper, "DESCRIBE ") {
		tableName := strings.TrimSpace(query[len("DESCRIBE "):])
		return s.describeTable(ctx, tableName)
	}
	if strings.HasPrefix(upper, "DESC ") {
		tableName := strings.TrimSpace(query[len("DESC "):])
		return s.describeTable(ctx, tableName)
	}

	if strings.HasPrefix(upper, "SELECT") && strings.Contains(upper, " FROM ") {
		return s.executeSelect(ctx, query)
	}

	// Not a recognized SQL dialect — return structured StatusNotImplemented error
	return nil, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unsupported SQL query: %q", query),
	}
}

func (s *Statement) showTables(_ context.Context) (array.RecordReader, int64, error) {
	vs := s.conn.store
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "table_name", Type: arrow.BinaryTypes.String},
		{Name: "table_type", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()

	nameBuilder := builder.Field(0).(*array.StringBuilder)
	typeBuilder := builder.Field(1).(*array.StringBuilder)

	vs.IterateDatasets(func(name string, ds *store.Dataset) {
		nameBuilder.Append(name)
		typeBuilder.Append("table")
	})

	rec := builder.NewRecordBatch()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	return reader, -1, err
}

func (s *Statement) describeTable(_ context.Context, tableName string) (array.RecordReader, int64, error) {
	vs := s.conn.store
	var tableSchema *arrow.Schema
	vs.IterateDatasets(func(name string, ds *store.Dataset) {
		if name == tableName {
			tableSchema = ds.GetSchema()
		}
	})

	if tableSchema == nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  fmt.Sprintf("dataset %q not found", tableName),
		}
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "column_name", Type: arrow.BinaryTypes.String},
		{Name: "data_type", Type: arrow.BinaryTypes.String},
		{Name: "nullable", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()

	nameBuilder := builder.Field(0).(*array.StringBuilder)
	typeBuilder := builder.Field(1).(*array.StringBuilder)
	nullableBuilder := builder.Field(2).(*array.BooleanBuilder)

	for _, field := range tableSchema.Fields() {
		nameBuilder.Append(field.Name)
		typeBuilder.Append(field.Type.String())
		nullableBuilder.Append(field.Nullable)
	}

	rec := builder.NewRecordBatch()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	return reader, -1, err
}

func (s *Statement) executeSelect(ctx context.Context, query string) (array.RecordReader, int64, error) {
	vs := s.conn.store
	tableName := parseTableFromSQL(query)
	k := parseKFromSQL(query)

	if tableName == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "could not parse table name from query",
		}
	}

	vector := extractVectorFromSQL(query)

	if vector != nil {
		return s.executeVectorSearch(ctx, vs, tableName, vector, k)
	}

	// Check the dataset exists before attempting scan
	found := false
	vs.IterateDatasets(func(name string, ds *store.Dataset) {
		if name == tableName {
			found = true
		}
	})
	if !found {
		return nil, -1, adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  fmt.Sprintf("dataset %q not found", tableName),
		}
	}

	return s.executeScan(ctx, vs, tableName)
}

func extractVectorFromSQL(query string) []float32 {
	upper := strings.ToUpper(query)

	idx := strings.Index(upper, "VECTOR")
	if idx < 0 {
		return nil
	}

	rest := query[idx:]
	bracketStart := strings.Index(rest, "[")
	if bracketStart < 0 {
		return nil
	}

	bracketEnd := strings.Index(rest, "]")
	if bracketEnd < 0 || bracketEnd <= bracketStart {
		return nil
	}

	content := rest[bracketStart+1 : bracketEnd]
	parts := strings.Split(content, ",")
	vector := make([]float32, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		val, err := strconv.ParseFloat(p, 32)
		if err != nil {
			return nil
		}
		vector = append(vector, float32(val))
	}

	if len(vector) == 0 {
		return nil
	}
	return vector
}

func (s *Statement) executeVectorSearch(ctx context.Context, vs *store.VectorStore, tableName string, vector []float32, k int) (array.RecordReader, int64, error) {
	results, err := vs.SearchHybrid(ctx, tableName, vector, "", k, 1.0, 60, 0, 0, false)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("search failed: %v", err),
		}
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Uint32Builder)
	scoreBuilder := builder.Field(1).(*array.Float32Builder)

	for _, r := range results {
		idBuilder.Append(uint32(r.ID))
		scoreBuilder.Append(r.Score)
	}

	rec := builder.NewRecordBatch()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	return reader, -1, err
}

func (s *Statement) executeScan(ctx context.Context, vs *store.VectorStore, tableName string) (array.RecordReader, int64, error) {
	k := parseKFromSQL(s.query)

	dim := 128
	var foundDim int
	vs.IterateDatasets(func(name string, ds *store.Dataset) {
		if name == tableName && ds.GetSchema() != nil {
			for _, f := range ds.GetSchema().Fields() {
				if f.Name == "vector" {
					if fsl, ok := f.Type.(*arrow.FixedSizeListType); ok {
						foundDim = int(fsl.Len())
					}
				}
			}
		}
	})
	if foundDim > 0 {
		dim = foundDim
	}

	queryVec := make([]float32, dim)
	for i := range queryVec {
		queryVec[i] = 1.0 / float32(i+1)
	}

	results, err := vs.SearchHybrid(ctx, tableName, queryVec, "", k, 1.0, 60, 0, 0, false)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("scan failed: %v", err),
		}
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Uint32Builder)
	scoreBuilder := builder.Field(1).(*array.Float32Builder)

	for _, r := range results {
		idBuilder.Append(uint32(r.ID))
		scoreBuilder.Append(r.Score)
	}

	rec := builder.NewRecordBatch()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	return reader, -1, err
}
