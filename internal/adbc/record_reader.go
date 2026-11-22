package adbc

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
)

// AdbcRecordReader represents a adbc record reader value.
type AdbcRecordReader struct {
	refCount   int64
	schema     *arrow.Schema
	records    []arrow.RecordBatch
	currentIdx int
	err        error
}

// NewAdbcRecordReader constructs a new AdbcRecordReader.
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

// Retain runs the retain operation.
func (r *AdbcRecordReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

// Release runs the release operation.
func (r *AdbcRecordReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		for _, rec := range r.records {
			rec.Release()
		}
		r.records = nil
	}
}

// Schema runs the schema operation.
func (r *AdbcRecordReader) Schema() *arrow.Schema {
	return r.schema
}

// Next runs the next operation.
func (r *AdbcRecordReader) Next() bool {
	if r.currentIdx < len(r.records) {
		r.currentIdx++
		return true
	}
	return false
}

// RecordBatch runs the record batch operation.
func (r *AdbcRecordReader) RecordBatch() arrow.RecordBatch {
	if r.currentIdx > 0 && r.currentIdx <= len(r.records) {
		return r.records[r.currentIdx-1]
	}
	return nil
}

// Record runs the record operation.
func (r *AdbcRecordReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

// Err runs the err operation.
func (r *AdbcRecordReader) Err() error {
	return r.err
}
