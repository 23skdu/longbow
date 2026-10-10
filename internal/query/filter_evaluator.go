package query

import (
	"fmt"
	"github.com/23skdu/longbow/internal/metrics"
	"strconv"
	"strings"
	"time"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type filterOp interface {
	Match(rowIdx int) bool
	MatchBitmap(dst []byte)
	FilterBatch(indices []int) []int
	Bind(col arrow.Array) error
	Reset(rec arrow.RecordBatch) error
	Compound() bool
	MatchValue(val interface{}) bool
}

// compoundFilterOp handles AND/OR/NOT logic over child filterOps

type FilterEvaluator struct {
	ops []filterOp
}

// NewFilterEvaluator creates a new evaluator, pre-binding filters to RecordBatch columns.
// Supports compound expressions (AND/OR/NOT) with nested field paths (dot notation).
func NewFilterEvaluator(rec arrow.RecordBatch, filters []Filter) (*FilterEvaluator, error) {
	if len(filters) == 0 {
		return &FilterEvaluator{}, nil
	}

	ops := make([]filterOp, 0, len(filters))
	schema := rec.Schema()

	for _, f := range filters {
		op, err := buildFilterOp(*schema, rec, &f)
		if err != nil {
			return nil, err
		}
		if op != nil {
			ops = append(ops, op)
		}
	}

	if len(ops) == 0 && len(filters) > 0 {
		return nil, fmt.Errorf("failed to bind any filters to schema fields")
	}
	return &FilterEvaluator{ops: ops}, nil
}

// SetStringColumnCodes applies precomputed dictionary codes to all string filter operations targeting colIdx.
func (e *FilterEvaluator) SetStringColumnCodes(colIdx int, codes []uint16, dict *StringDictionary) {
	for _, op := range e.ops {
		if sOp, ok := op.(*stringFilterOp); ok && sOp.colIdx == colIdx {
			sOp.SetDictionaryCodes(codes, dict)
		}
	}
}

func buildFilterOp(schema arrow.Schema, rec arrow.RecordBatch, f *Filter) (filterOp, error) {
	logic := strings.ToUpper(f.Logic)
	if logic != "" {
		return buildCompoundOp(schema, rec, logic, f.Filters)
	}

	if f.Subquery != nil {
		return buildSubqueryOp(schema, rec, f)
	}

	isNested := strings.Contains(f.Field, ".")

	if isNested {
		colIndices, col, nestedType, err := resolveFilterColumnEx(schema, rec, f.Field)
		if err != nil || col == nil {
			return nil, nil
		}

		opStr := strings.ToLower(f.Operator)
		_ = colIndices
		colIdx := colIndices[0]

		var innerOp filterOp
		switch nestedType.ID() {
		case arrow.INT64:
			val, err := strconv.ParseInt(f.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid int64 value %q for field %s", f.Value, f.Field)
			}
			innerOp = &int64FilterOp{val: val, operator: opStr, colIdx: colIdx}
		case arrow.INT32:
			val, err := strconv.ParseInt(f.Value, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid int32 value %q for field %s", f.Value, f.Field)
			}
			innerOp = &int32FilterOp{val: int32(val), operator: opStr, colIdx: colIdx}
		case arrow.UINT64:
			val, err := strconv.ParseUint(f.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid uint64 value %q for field %s", f.Value, f.Field)
			}
			innerOp = &uint64FilterOp{val: val, operator: opStr, colIdx: colIdx}
		case arrow.FLOAT32:
			val, err := strconv.ParseFloat(f.Value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid float32 value %q for field %s", f.Value, f.Field)
			}
			innerOp = &float32FilterOp{val: float32(val), operator: opStr, colIdx: colIdx}
		case arrow.FLOAT64:
			val, err := strconv.ParseFloat(f.Value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float64 value %q for field %s", f.Value, f.Field)
			}
			innerOp = &float64FilterOp{val: val, operator: opStr, colIdx: colIdx}
		case arrow.STRING:
			innerOp = &stringFilterOp{val: f.Value, operator: opStr, colIdx: colIdx}
		case arrow.BOOL:
			val := strings.ToLower(f.Value) == "true"
			innerOp = &boolFilterOp{val: val, operator: opStr, colIdx: colIdx}
		default:
			return nil, nil
		}

		return &nestedFilterOp{fieldPath: f.Field, colIndices: colIndices, op: innerOp, outerCol: col}, nil
	}

	colIndices, col, nestedType, err := resolveFilterColumnEx(schema, rec, f.Field)
	if err != nil || col == nil {
		return nil, nil
	}

	opStr := strings.ToLower(f.Operator)
	colIdx := colIndices[0]

	switch nestedType.ID() {

	case arrow.INT64:
		val, err := strconv.ParseInt(f.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int64 value %q for field %s", f.Value, f.Field)
		}
		return &int64FilterOp{col: col.(*array.Int64), val: val, operator: opStr, colIdx: colIdx}, nil
	case arrow.INT32:
		val, err := strconv.ParseInt(f.Value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid int32 value %q for field %s", f.Value, f.Field)
		}
		return &int32FilterOp{col: col.(*array.Int32), val: int32(val), operator: opStr, colIdx: colIdx}, nil
	case arrow.UINT64:
		val, err := strconv.ParseUint(f.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid uint64 value %q for field %s", f.Value, f.Field)
		}
		return &uint64FilterOp{col: col.(*array.Uint64), val: val, operator: opStr, colIdx: colIdx}, nil
	case arrow.FLOAT32:
		val, err := strconv.ParseFloat(f.Value, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid float32 value %q for field %s", f.Value, f.Field)
		}
		return &float32FilterOp{col: col.(*array.Float32), val: float32(val), operator: opStr, colIdx: colIdx}, nil
	case arrow.FLOAT64:
		val, err := strconv.ParseFloat(f.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float64 value %q for field %s", f.Value, f.Field)
		}
		return &float64FilterOp{col: col.(*array.Float64), val: val, operator: opStr, colIdx: colIdx}, nil
	case arrow.STRING:
		return &stringFilterOp{col: col.(*array.String), val: f.Value, operator: opStr, colIdx: colIdx}, nil
	case arrow.BOOL:
		val := strings.ToLower(f.Value) == "true"
		return &boolFilterOp{col: col.(*array.Boolean), val: val, operator: opStr, colIdx: colIdx}, nil
	default:
		return nil, nil
	}
}

func resolveFilterColumnEx(schema arrow.Schema, rec arrow.RecordBatch, fieldPath string) ([]int, arrow.Array, arrow.DataType, error) {
	parts := strings.Split(fieldPath, ".")
	if len(parts) == 0 {
		return nil, nil, nil, fmt.Errorf("empty field path")
	}

	rootIdx := schema.FieldIndices(parts[0])
	if len(rootIdx) == 0 {
		return nil, nil, nil, nil
	}

	indices, dt, err := resolveNestedField(schema, fieldPath)
	if err != nil {
		return nil, nil, nil, err
	}

	return indices, rec.Column(rootIdx[0]), dt, nil
}

func resolveFilterColumn(schema arrow.Schema, rec arrow.RecordBatch, fieldPath string) ([]int, arrow.Array, error) {
	indices, _, err := resolveNestedField(schema, fieldPath)
	if err != nil {
		return nil, nil, err
	}
	if len(indices) == 0 {
		return nil, nil, nil
	}
	return indices, rec.Column(indices[0]), nil
}

// Matches returns true if the row satisfies all filters

func (e *FilterEvaluator) Matches(rowIdx int) bool {
	// Unrolled check for performance (Go compiler can optimize this)
	for i := 0; i < len(e.ops); i++ {
		if !e.ops[i].Match(rowIdx) {
			return false
		}
	}
	return true
}

// MatchesBatch evaluates filters for a slice of row indices and returns a subset of matching indices.
// This uses vectorized FilterBatch operations for improved performance.
func (e *FilterEvaluator) MatchesBatch(rowIndices []int) []int {
	start := time.Now()
	defer func() {
		metrics.FilterEvaluatorOpsTotal.WithLabelValues("MatchesBatch").Inc()
		metrics.FilterEvaluatorDurationSeconds.WithLabelValues("MatchesBatch").Observe(time.Since(start).Seconds())
	}()

	if len(e.ops) == 0 {
		return rowIndices
	}

	result := rowIndices
	// Chain filters: output of one is input to next
	// This reduces the working set size progressively
	for _, op := range e.ops {
		result = op.FilterBatch(result)
		if len(result) == 0 {
			metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesBatch", "intermediate").Add(float64(len(result)))
			return nil
		}
	}
	metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesBatch", "intermediate").Add(float64(len(result)))
	return result
}

// MatchesBatchFused evaluates all filters in a single pass without creating intermediate slices.
// This reduces memory allocations and improves cache locality compared to MatchesBatch.
func (e *FilterEvaluator) MatchesBatchFused(rowIndices []int) []int {
	start := time.Now()
	defer func() {
		metrics.FilterEvaluatorOpsTotal.WithLabelValues("MatchesBatchFused").Inc()
		metrics.FilterEvaluatorDurationSeconds.WithLabelValues("MatchesBatchFused").Observe(time.Since(start).Seconds())
	}()

	if len(e.ops) == 0 {
		return rowIndices
	}

	if len(rowIndices) == 0 {
		return nil
	}

	result := make([]int, 0, len(rowIndices))

	for _, idx := range rowIndices {
		matches := true
		for _, op := range e.ops {
			if !op.Match(idx) {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, idx)
		}
	}

	if len(result) == 0 {
		return nil
	}

	metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesBatchFused", "indices").Add(float64(len(result)))
	return result
}

// MatchesAll evaluates all filters on the entire batch using SIMD and returns matching row indices.
// Compound filters (AND/OR/NOT) and flat filters are handled separately for optimal performance.
func (e *FilterEvaluator) MatchesAll(batchLen int) ([]int, error) {
	start := time.Now()
	defer func() {
		metrics.FilterEvaluatorOpsTotal.WithLabelValues("MatchesAll").Inc()
		metrics.FilterEvaluatorDurationSeconds.WithLabelValues("MatchesAll").Observe(time.Since(start).Seconds())
	}()

	if len(e.ops) == 0 {
		indices := make([]int, batchLen)
		for i := 0; i < batchLen; i++ {
			indices[i] = i
		}
		return indices, nil
	}

	flatOps := make([]filterOp, 0, len(e.ops))
	compoundOps := make([]filterOp, 0, len(e.ops))
	for _, op := range e.ops {
		if op.Compound() {
			compoundOps = append(compoundOps, op)
		} else {
			flatOps = append(flatOps, op)
		}
	}

	var bitmap []byte
	if len(flatOps) > 0 {
		sortedFlat := selectOpsBySelectivity(flatOps)
		bitmap = make([]byte, batchLen)
		sortedFlat[0].MatchBitmap(bitmap)

		if isBitmapAllZeros(bitmap) {
			metrics.BloomFilterEarlyExitsTotal.Inc()
			return []int{}, nil
		}

		if len(sortedFlat) > 1 {
			tmp := make([]byte, batchLen)
			for i := 1; i < len(sortedFlat); i++ {
				sortedFlat[i].MatchBitmap(tmp)
				if err := simd.AndBytes(bitmap, tmp); err != nil {
					return nil, err
				}
				if isBitmapAllZeros(bitmap) {
					metrics.BloomFilterEarlyExitsTotal.Inc()
					return []int{}, nil
				}
			}
		}
	}

	if len(compoundOps) > 0 {
		compoundBitmap := make([]byte, batchLen)
		for _, cop := range compoundOps {
			cop.MatchBitmap(compoundBitmap)
			if bitmap == nil {
				bitmap = compoundBitmap
			} else {
				if err := simd.AndBytes(bitmap, compoundBitmap); err != nil {
					return nil, err
				}
			}
			if isBitmapAllZeros(bitmap) {
				metrics.BloomFilterEarlyExitsTotal.Inc()
				return []int{}, nil
			}
		}
	}

	if bitmap == nil {
		bitmap = make([]byte, batchLen)
		for i := range bitmap {
			bitmap[i] = 1
		}
	}

	indices := make([]int, 0, batchLen/2)
	for i, b := range bitmap {
		if b != 0 {
			indices = append(indices, i)
		}
	}
	metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesAll", "indices").Add(float64(len(indices)))
	return indices, nil
}

// Reset binds the evaluator to a new record batch, reusing the existing filter operations.
func (e *FilterEvaluator) Reset(rec arrow.RecordBatch) error {
	if len(e.ops) == 0 {
		return nil
	}

	for _, op := range e.ops {
		if err := op.Reset(rec); err != nil {
			return err
		}
	}
	return nil
}

func (e *FilterEvaluator) EvaluateToArrowBoolean(mem memory.Allocator, rows int) (*array.Boolean, error) {
	if len(e.ops) == 0 {
		b := array.NewBooleanBuilder(mem)
		b.Reserve(rows)
		for i := 0; i < rows; i++ {
			b.Append(true)
		}
		return b.NewBooleanArray(), nil
	}

	flatOps := make([]filterOp, 0, len(e.ops))
	compoundOps := make([]filterOp, 0, len(e.ops))
	for _, op := range e.ops {
		if op.Compound() {
			compoundOps = append(compoundOps, op)
		} else {
			flatOps = append(flatOps, op)
		}
	}

	var bitmap []byte
	if len(flatOps) > 0 {
		bitmap = make([]byte, rows)
		flatOps[0].MatchBitmap(bitmap)
		if len(flatOps) > 1 {
			tmp := make([]byte, rows)
			for i := 1; i < len(flatOps); i++ {
				flatOps[i].MatchBitmap(tmp)
				if err := simd.AndBytes(bitmap, tmp); err != nil {
					return nil, err
				}
			}
		}
	}

	if len(compoundOps) > 0 {
		cb := make([]byte, rows)
		for _, cop := range compoundOps {
			cop.MatchBitmap(cb)
			if bitmap == nil {
				bitmap = cb
			} else {
				if err := simd.AndBytes(bitmap, cb); err != nil {
					return nil, err
				}
			}
		}
	}

	if bitmap == nil {
		bitmap = make([]byte, rows)
		for i := range bitmap {
			bitmap[i] = 1
		}
	}

	b := array.NewBooleanBuilder(mem)
	b.Reserve(rows)
	bools := make([]bool, rows)
	for i, v := range bitmap {
		bools[i] = v != 0
	}
	b.AppendValues(bools, nil)
	return b.NewBooleanArray(), nil
}

func estimateSelectivity(op filterOp, sampleSize int) float64 {
	if op.Compound() {
		return 0.5
	}

	var filterType string
	var sampleCount int

	switch o := op.(type) {
	case *int64FilterOp:
		filterType = "int64"
		sampleCount = o.col.Len()
	case *float32FilterOp:
		filterType = "float32"
		sampleCount = o.col.Len()
	case *float64FilterOp:
		filterType = "float64"
		sampleCount = o.col.Len()
	case *stringFilterOp:
		filterType = "string"
		sampleCount = o.col.Len()
	case *nestedFilterOp:
		filterType = "nested"
		return estimateSelectivity(o.op, sampleSize)
	default:
		return 0.5
	}

	if sampleCount == 0 {
		metrics.BloomFilterSelectivityHistogram.WithLabelValues(filterType).Observe(1.0)
		return 1.0
	}

	sample := sampleSize
	if sample > sampleCount {
		sample = sampleCount
	}

	matchCount := 0
	for i := 0; i < sample; i++ {
		if op.Match(i) {
			matchCount++
		}
	}

	selectivity := float64(matchCount) / float64(sample)
	metrics.BloomFilterSelectivityHistogram.WithLabelValues(filterType).Observe(selectivity)
	return selectivity
}

// isBitmapAllZeros checks if a bitmap contains all zeros (no matches).
// Uses a fast SIMD-like approach for better performance.
func isBitmapAllZeros(bitmap []byte) bool {
	metrics.BloomFilterBitmapZeroChecksTotal.Inc()

	for _, b := range bitmap {
		if b != 0 {
			return false
		}
	}
	return true
}

// selectOpsBySelectivity reorders filter operations by estimated selectivity.
// Filters with higher selectivity (fewer matches) should run first for better early exit.
func selectOpsBySelectivity(ops []filterOp) []filterOp {
	if len(ops) <= 1 {
		return ops
	}

	type selectivityPair struct {
		op          filterOp
		selectivity float64
	}

	pairs := make([]selectivityPair, len(ops))
	for i, op := range ops {
		pairs[i] = selectivityPair{op: op, selectivity: estimateSelectivity(op, 100)}
	}

	// Sort by selectivity ascending (higher selectivity = fewer matches = run first)
	// This means filters that reject more rows run first, enabling early exit.
	for i := 0; i < len(pairs)-1; i++ {
		for j := i + 1; j < len(pairs); j++ {
			if pairs[j].selectivity < pairs[i].selectivity {
				pairs[i], pairs[j] = pairs[j], pairs[i]
			}
		}
	}

	result := make([]filterOp, len(ops))
	for i, p := range pairs {
		result[i] = p.op
	}
	return result
}
