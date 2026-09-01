package adbc

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
)

type AdbcRecordReader struct {
	refCount   int64
	schema     *arrow.Schema
	records    []arrow.RecordBatch
	currentIdx int
	err        error
}

func NewAdbcRecordReader(schema *arrow.Schema, records []arrow.RecordBatch) (*AdbcRecordReader, error) {
	r := &AdbcRecordReader{
		schema:   schema,
		records:  records,
		refCount: 1,
	}
	for _, rec := range records {
		rec.Retain()
	}
	for _, rec := range records {
		if !rec.Schema().Equal(schema) {
			r.Release()
			return nil, fmt.Errorf("adbc: record schema does not match AdbcRecordReader schema")
		}
	}
	return r, nil
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
