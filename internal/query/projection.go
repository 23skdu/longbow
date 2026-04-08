package query

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Projection struct {
	Columns []string
	schema  *arrow.Schema
}

func NewProjection(schema *arrow.Schema, columns []string) (*Projection, error) {
	if len(columns) == 0 {
		return &Projection{Columns: nil, schema: schema}, nil
	}

	validColumns := make([]string, 0, len(columns))
	for _, col := range columns {
		idx := schema.FieldIndices(col)
		if len(idx) == 0 {
			return nil, fmt.Errorf("column %q not found in schema", col)
		}
		validColumns = append(validColumns, col)
	}

	return &Projection{Columns: validColumns, schema: schema}, nil
}

func (p *Projection) Apply(rec arrow.Record) (arrow.Record, error) {
	if p == nil || len(p.Columns) == 0 {
		return rec, nil
	}

	numRows := rec.NumRows()
	origSchema := rec.Schema()

	colIndices := make([]int, len(p.Columns))
	for i, colName := range p.Columns {
		indices := origSchema.FieldIndices(colName)
		if len(indices) == 0 {
			return nil, fmt.Errorf("column %q not found", colName)
		}
		colIndices[i] = indices[0]
	}

	newFields := make([]arrow.Field, len(colIndices))
	newCols := make([]arrow.Array, len(colIndices))

	for i, colIdx := range colIndices {
		newFields[i] = origSchema.Field(colIdx)
		newCols[i] = rec.Column(colIdx)
		newCols[i].Retain()
	}

	newSchema := arrow.NewSchema(newFields, nil)
	record := array.NewRecord(newSchema, newCols, numRows)
	record.Retain()

	return record, nil
}

func (p *Projection) CanPushdown(filters []Filter) bool {
	if p == nil || len(p.Columns) == 0 {
		return true
	}

	filterCols := make(map[string]bool)
	for _, f := range filters {
		filterCols[f.Field] = true
	}

	for col := range filterCols {
		found := false
		for _, projCol := range p.Columns {
			if col == projCol {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func (p *Projection) RequiredColumns() []string {
	if p == nil {
		return nil
	}
	return p.Columns
}

type ProjectionEvaluator struct {
	proj       *Projection
	colIndices []int
}

func NewProjectionEvaluator(proj *Projection, schema *arrow.Schema) (*ProjectionEvaluator, error) {
	if proj == nil || len(proj.Columns) == 0 {
		return &ProjectionEvaluator{}, nil
	}

	colIndices := make([]int, len(proj.Columns))
	for i, colName := range proj.Columns {
		indices := schema.FieldIndices(colName)
		if len(indices) == 0 {
			return nil, fmt.Errorf("column %q not found in schema", colName)
		}
		colIndices[i] = indices[0]
	}

	return &ProjectionEvaluator{
		proj:       proj,
		colIndices: colIndices,
	}, nil
}

func (pe *ProjectionEvaluator) ApplyToRecord(rec arrow.Record) (arrow.Record, error) {
	if pe.proj == nil || len(pe.proj.Columns) == 0 {
		return rec, nil
	}

	numRows := rec.NumRows()
	newCols := make([]arrow.Array, len(pe.colIndices))

	for i, colIdx := range pe.colIndices {
		newCols[i] = rec.Column(colIdx)
		newCols[i].Retain()
	}

	newFields := make([]arrow.Field, len(pe.colIndices))
	for i, colIdx := range pe.colIndices {
		newFields[i] = rec.Schema().Field(colIdx)
	}

	newSchema := arrow.NewSchema(newFields, nil)
	record := array.NewRecord(newSchema, newCols, numRows)
	record.Retain()

	return record, nil
}

func (pe *ProjectionEvaluator) ApplyToBatch(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	if pe.proj == nil || len(pe.proj.Columns) == 0 {
		return rec, nil
	}

	numRows := rec.NumRows()
	numCols := len(pe.colIndices)

	newCols := make([]arrow.Array, numCols)
	newFields := make([]arrow.Field, numCols)

	for i, colIdx := range pe.colIndices {
		field := rec.Schema().Field(colIdx)
		newFields[i] = field
		newCols[i] = rec.Column(colIdx)
		newCols[i].Retain()
	}

	newSchema := arrow.NewSchema(newFields, nil)
	batch := array.NewRecordBatch(newSchema, newCols, numRows)
	batch.Retain()

	return batch, nil
}

func ProjectRecord(mem memory.Allocator, rec arrow.Record, columns []string) (arrow.Record, error) {
	if len(columns) == 0 {
		return rec, nil
	}

	schema := rec.Schema()
	colIndices := make([]int, len(columns))

	for i, colName := range columns {
		indices := schema.FieldIndices(colName)
		if len(indices) == 0 {
			return nil, fmt.Errorf("column %q not found", colName)
		}
		colIndices[i] = indices[0]
	}

	numRows := rec.NumRows()
	newFields := make([]arrow.Field, len(colIndices))
	newCols := make([]arrow.Array, len(colIndices))

	for i, colIdx := range colIndices {
		field := schema.Field(colIdx)
		newFields[i] = field
		newCols[i] = rec.Column(colIdx)
		newCols[i].Retain()
	}

	newSchema := arrow.NewSchema(newFields, nil)
	newRecord := array.NewRecord(newSchema, newCols, numRows)
	newRecord.Retain()

	return newRecord, nil
}
