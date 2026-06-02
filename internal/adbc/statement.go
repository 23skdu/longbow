package adbc

import (
	"context"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type Statement struct {
	conn   *Connection
	query  string
	stream array.RecordReader
}

func NewStatement(conn *Connection) adbc.Statement {
	return &Statement{conn: conn}
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
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	reader := &AdbcRecordReader{
		schema: schema,
	}

	return reader, -1, nil
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

// AdbcRecordReader implements array.RecordReader
type AdbcRecordReader struct {
	refCount   int64
	schema     *arrow.Schema
	records    []arrow.RecordBatch
	currentIdx int
	err        error
}

func (r *AdbcRecordReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *AdbcRecordReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		for _, rec := range r.records {
			rec.Release()
		}
		r.records = nil
	}
}

func (r *AdbcRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *AdbcRecordReader) Next() bool {
	if r.currentIdx < len(r.records) {
		r.currentIdx++
		return true
	}
	return false
}

func (r *AdbcRecordReader) RecordBatch() arrow.RecordBatch {
	if r.currentIdx > 0 && r.currentIdx <= len(r.records) {
		return r.records[r.currentIdx-1]
	}
	return nil
}

func (r *AdbcRecordReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

func (r *AdbcRecordReader) Err() error {
	return r.err
}
