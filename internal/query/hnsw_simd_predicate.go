package query

import (
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// HNSWSIMDPredicate implements types.HNSWPredicate using SIMD kernels for accelerated
// metadata filtering during HNSW traversal.
type HNSWSIMDPredicate struct {
	records []arrow.RecordBatch
	colIdx  int
	op      simd.CompareOp
	valInt  int64
	valF64  float64
	dt      arrow.Type
}

// NewHNSWSIMDPredicate creates a new SIMD-accelerated predicate for HNSW traversal.
// It returns nil if the column is not found or the type is not supported.
func NewHNSWSIMDPredicate(records []arrow.RecordBatch, colName string, op simd.CompareOp, value any) *HNSWSIMDPredicate {
	if len(records) == 0 {
		return nil
	}
	schema := records[0].Schema()
	indices := schema.FieldIndices(colName)
	if len(indices) == 0 {
		return nil
	}
	colIdx := indices[0]
	dt := schema.Field(colIdx).Type.ID()

	// Supported types for SIMD acceleration
	switch dt {
	case arrow.INT64, arrow.INT32, arrow.FLOAT32, arrow.FLOAT64:
		// OK
	default:
		return nil
	}

	p := &HNSWSIMDPredicate{
		records: records,
		colIdx:  colIdx,
		op:      op,
		dt:      dt,
	}

	switch v := value.(type) {
	case int64:
		p.valInt = v
	case int:
		p.valInt = int64(v)
	case int32:
		p.valInt = int64(v)
	case float64:
		p.valF64 = v
	case float32:
		p.valF64 = float64(v)
	}

	return p
}

func (p *HNSWSIMDPredicate) IsMatch(id uint32) bool {
	batchIdx := int(id / uint32(types.ChunkSize))
	rowIdx := int(id % uint32(types.ChunkSize))
	if batchIdx >= len(p.records) {
		return false
	}
	rec := p.records[batchIdx]
	if rowIdx >= int(rec.NumRows()) {
		return false
	}
	col := rec.Column(p.colIdx)

	switch p.dt {
	case arrow.INT64:
		val := col.(*array.Int64).Value(rowIdx)
		return p.compareInt64(val)
	case arrow.INT32:
		val := col.(*array.Int32).Value(rowIdx)
		return p.compareInt32(val)
	case arrow.FLOAT32:
		val := col.(*array.Float32).Value(rowIdx)
		return p.compareFloat32(val)
	case arrow.FLOAT64:
		val := col.(*array.Float64).Value(rowIdx)
		return p.compareFloat64(val)
	}
	return true
}

func (p *HNSWSIMDPredicate) MatchBatch(ids []uint32, dst []byte) {
	n := len(ids)
	if n == 0 {
		return
	}

	switch p.dt {
	case arrow.INT64:
		buf := make([]int64, n)
		for i, id := range ids {
			batchIdx := int(id / uint32(types.ChunkSize))
			rowIdx := int(id % uint32(types.ChunkSize))
			buf[i] = p.records[batchIdx].Column(p.colIdx).(*array.Int64).Value(rowIdx)
		}
		_ = simd.MatchInt64(buf, p.valInt, p.op, dst)
	case arrow.INT32:
		buf := make([]int32, n)
		for i, id := range ids {
			batchIdx := int(id / uint32(types.ChunkSize))
			rowIdx := int(id % uint32(types.ChunkSize))
			buf[i] = p.records[batchIdx].Column(p.colIdx).(*array.Int32).Value(rowIdx)
		}
		_ = simd.MatchInt32(buf, int32(p.valInt), p.op, dst)
	case arrow.FLOAT32:
		buf := make([]float32, n)
		for i, id := range ids {
			batchIdx := int(id / uint32(types.ChunkSize))
			rowIdx := int(id % uint32(types.ChunkSize))
			buf[i] = p.records[batchIdx].Column(p.colIdx).(*array.Float32).Value(rowIdx)
		}
		_ = simd.MatchFloat32(buf, float32(p.valF64), p.op, dst)
	case arrow.FLOAT64:
		buf := make([]float64, n)
		for i, id := range ids {
			batchIdx := int(id / uint32(types.ChunkSize))
			rowIdx := int(id % uint32(types.ChunkSize))
			buf[i] = p.records[batchIdx].Column(p.colIdx).(*array.Float64).Value(rowIdx)
		}
		_ = simd.MatchFloat64(buf, p.valF64, p.op, dst)
	default:
		// Fallback to scalar
		for i, id := range ids {
			if p.IsMatch(id) {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	}
}

func (p *HNSWSIMDPredicate) compareInt64(val int64) bool {
	switch p.op {
	case simd.CompareEq: return val == p.valInt
	case simd.CompareNeq: return val != p.valInt
	case simd.CompareGt: return val > p.valInt
	case simd.CompareGe: return val >= p.valInt
	case simd.CompareLt: return val < p.valInt
	case simd.CompareLe: return val <= p.valInt
	}
	return true
}

func (p *HNSWSIMDPredicate) compareInt32(val int32) bool {
	v := int64(val)
	switch p.op {
	case simd.CompareEq: return v == p.valInt
	case simd.CompareNeq: return v != p.valInt
	case simd.CompareGt: return v > p.valInt
	case simd.CompareGe: return v >= p.valInt
	case simd.CompareLt: return v < p.valInt
	case simd.CompareLe: return v <= p.valInt
	}
	return true
}

func (p *HNSWSIMDPredicate) compareFloat32(val float32) bool {
	v := float64(val)
	switch p.op {
	case simd.CompareEq: return v == p.valF64
	case simd.CompareNeq: return v != p.valF64
	case simd.CompareGt: return v > p.valF64
	case simd.CompareGe: return v >= p.valF64
	case simd.CompareLt: return v < p.valF64
	case simd.CompareLe: return v <= p.valF64
	}
	return true
}

func (p *HNSWSIMDPredicate) compareFloat64(val float64) bool {
	switch p.op {
	case simd.CompareEq: return val == p.valF64
	case simd.CompareNeq: return val != p.valF64
	case simd.CompareGt: return val > p.valF64
	case simd.CompareGe: return val >= p.valF64
	case simd.CompareLt: return val < p.valF64
	case simd.CompareLe: return val <= p.valF64
	}
	return true
}
