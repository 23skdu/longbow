package query

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// matchInt64WithError wraps SIMD MatchInt64 and logs errors
func matchInt64WithError(src []int64, val int64, op simd.CompareOp, dst []byte) {
	if err := simd.MatchInt64(src, val, op, dst); err != nil {
		log.Printf("SIMD MatchInt64 error: %v", err)
	}
}

// matchFloat32WithError wraps SIMD MatchFloat32 and logs errors
func matchFloat32WithError(src []float32, val float32, op simd.CompareOp, dst []byte) {
	if err := simd.MatchFloat32(src, val, op, dst); err != nil {
		log.Printf("SIMD MatchFloat32 error: %v", err)
	}
}

// filterOp represents a typed operation on a specific column
type filterOp interface {
	Match(rowIdx int) bool
	MatchBitmap(dst []byte)
	FilterBatch(indices []int) []int
	Bind(col arrow.Array) error
} //nolint:interfacebloat

type int64FilterOp struct {
	col      *array.Int64
	val      int64
	operator string
	colIdx   int
}

func (o *int64FilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.INT64 {
		return fmt.Errorf("expected int64 column, got %s", col.DataType())
	}
	o.col = col.(*array.Int64)
	return nil
}

func (o *int64FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}

func (o *int64FilterOp) MatchBitmap(dst []byte) {
	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">", "gt":
		op = simd.CompareGt
	case ">=", "ge":
		op = simd.CompareGe
	case "<", "lt":
		op = simd.CompareLt
	case "<=", "le":
		op = simd.CompareLe
	default:
		for i := 0; i < len(dst); i++ {
			if o.Match(i) {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
		return
	}

	matchInt64WithError(o.col.Int64Values(), o.val, op, dst)

	if o.col.NullN() > 0 {
		offset := o.col.Data().Offset()
		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i + offset) {
				dst[i] = 0
			}
		}
	}
}

func (o *int64FilterOp) FilterBatch(indices []int) []int {
	if len(indices) == 0 {
		return nil
	}

	values := make([]int64, len(indices))
	for i, idx := range indices {
		values[i] = o.col.Value(idx)
	}

	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">", "gt":
		op = simd.CompareGt
	case ">=", "ge":
		op = simd.CompareGe
	case "<", "lt":
		op = simd.CompareLt
	case "<=", "le":
		op = simd.CompareLe
	default:
		result := make([]int, 0, len(indices))
		for _, idx := range indices {
			if o.Match(idx) {
				result = append(result, idx)
			}
		}
		return result
	}

	bitmap := make([]byte, len(indices))
	matchInt64WithError(values, o.val, op, bitmap)

	result := make([]int, 0, len(indices))
	hasNulls := o.col.NullN() > 0

	for i, b := range bitmap {
		if b == 1 {
			idx := indices[i]
			if !hasNulls || !o.col.IsNull(idx) {
				result = append(result, idx)
			}
		}
	}
	return result
}

type float32FilterOp struct {
	col      *array.Float32
	val      float32
	operator string
	colIdx   int
}

func (o *float32FilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.FLOAT32 {
		return fmt.Errorf("expected float32 column, got %s", col.DataType())
	}
	o.col = col.(*array.Float32)
	return nil
}

func (o *float32FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}

func (o *float32FilterOp) MatchBitmap(dst []byte) {
	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">", "gt":
		op = simd.CompareGt
	case ">=", "ge":
		op = simd.CompareGe
	case "<", "lt":
		op = simd.CompareLt
	case "<=", "le":
		op = simd.CompareLe
	default:
		for i := 0; i < len(dst); i++ {
			if o.Match(i) {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
		return
	}

	matchFloat32WithError(o.col.Float32Values(), o.val, op, dst)

	if o.col.NullN() > 0 {
		offset := o.col.Data().Offset()
		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i + offset) {
				dst[i] = 0
			}
		}
	}
}

func (o *float32FilterOp) FilterBatch(indices []int) []int {
	if len(indices) == 0 {
		return nil
	}

	values := make([]float32, len(indices))
	for i, idx := range indices {
		values[i] = o.col.Value(idx)
	}

	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">", "gt":
		op = simd.CompareGt
	case ">=", "ge":
		op = simd.CompareGe
	case "<", "lt":
		op = simd.CompareLt
	case "<=", "le":
		op = simd.CompareLe
	default:
		result := make([]int, 0, len(indices))
		for _, idx := range indices {
			if o.Match(idx) {
				result = append(result, idx)
			}
		}
		return result
	}

	bitmap := make([]byte, len(indices))
	matchFloat32WithError(values, o.val, op, bitmap)

	result := make([]int, 0, len(indices))
	hasNulls := o.col.NullN() > 0

	for i, b := range bitmap {
		if b == 1 {
			idx := indices[i]
			if !hasNulls || !o.col.IsNull(idx) {
				result = append(result, idx)
			}
		}
	}
	return result
}

type float64FilterOp struct {
	col      *array.Float64
	val      float64
	operator string
	colIdx   int
}

func (o *float64FilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.FLOAT64 {
		return fmt.Errorf("expected float64 column, got %s", col.DataType())
	}
	o.col = col.(*array.Float64)
	return nil
}

func (o *float64FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}

func (o *float64FilterOp) MatchBitmap(dst []byte) {
	for i := 0; i < len(dst); i++ {
		if o.Match(i) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func (o *float64FilterOp) FilterBatch(indices []int) []int {
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if o.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}

type stringFilterOp struct {
	col      *array.String
	val      string
	operator string
	colIdx   int
}

func (o *stringFilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.STRING {
		return fmt.Errorf("expected string column, got %s", col.DataType())
	}
	o.col = col.(*array.String)
	return nil
}

func (o *stringFilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}

func (o *stringFilterOp) MatchBitmap(dst []byte) {
	for i := 0; i < len(dst); i++ {
		if o.Match(i) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func (o *stringFilterOp) FilterBatch(indices []int) []int {
	// String comparison is hard to vectorize without fancy SIMD (PCMPESTRM) or fixed width.
	// Fallback to loop for now.
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if o.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}

// FilterEvaluator pre-processes filters for a specific RecordBatch to enable fast scanning
type FilterEvaluator struct {
	ops []filterOp
}

// NewFilterEvaluator creates a new evaluator, pre-binding filters to RecordBatch columns
func NewFilterEvaluator(rec arrow.RecordBatch, filters []Filter) (*FilterEvaluator, error) {
	if len(filters) == 0 {
		return &FilterEvaluator{}, nil
	}

	ops := make([]filterOp, 0, len(filters))
	schema := rec.Schema()

	for _, f := range filters {
		indices := schema.FieldIndices(f.Field)
		if len(indices) == 0 {
			continue // Or return error? For now follow store_helpers.go behavior
		}
		colIdx := indices[0]
		col := rec.Column(colIdx)
		opStr := strings.ToLower(f.Operator)

		switch col.DataType().ID() {
		case arrow.INT64:
			val, err := strconv.ParseInt(f.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid int64 value %q for field %s", f.Value, f.Field)
			}
			ops = append(ops, &int64FilterOp{
				col:      col.(*array.Int64),
				val:      val,
				operator: opStr,
				colIdx:   colIdx,
			})
		case arrow.FLOAT32:
			val, err := strconv.ParseFloat(f.Value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid float32 value %q for field %s", f.Value, f.Field)
			}
			ops = append(ops, &float32FilterOp{
				col:      col.(*array.Float32),
				val:      float32(val),
				operator: opStr,
				colIdx:   colIdx,
			})
		case arrow.FLOAT64:
			val, err := strconv.ParseFloat(f.Value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float64 value %q for field %s", f.Value, f.Field)
			}
			ops = append(ops, &float64FilterOp{
				col:      col.(*array.Float64),
				val:      val,
				operator: opStr,
				colIdx:   colIdx,
			})
		case arrow.STRING:
			ops = append(ops, &stringFilterOp{
				col:      col.(*array.String),
				val:      f.Value,
				operator: opStr,
				colIdx:   colIdx,
			})
		default:
			// Fallback to slow evaluator or error
			// For now, let's keep it simple and handle common types
		}
	}
	if len(ops) == 0 && len(filters) > 0 {
		return nil, fmt.Errorf("failed to bind any filters to schema fields")
	}
	return &FilterEvaluator{ops: ops}, nil
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
// This is optimal for dense scans.
// MatchesAll evaluates all filters on the entire batch using SIMD and returns matching row indices.
// This is optimal for dense scans.
func (e *FilterEvaluator) MatchesAll(batchLen int) ([]int, error) {
	if len(e.ops) == 0 {
		// Return all indices
		indices := make([]int, batchLen)
		for i := 0; i < batchLen; i++ {
			indices[i] = i
		}
		return indices, nil
	}

	// Allocate bitmaps
	// Note: batchLen serves as capacity.
	// We assume all columns in the batch have the same length.
	// Ops access columns which know their length, but dst must be sized correctly.
	// We pass dst of batchLen.

	bitmap := make([]byte, batchLen)
	// Initialize first op
	e.ops[0].MatchBitmap(bitmap)

	if len(e.ops) > 1 {
		tmp := make([]byte, batchLen)
		for i := 1; i < len(e.ops); i++ {
			e.ops[i].MatchBitmap(tmp)
			// Intersect (AND)
			if err := simd.AndBytes(bitmap, tmp); err != nil {
				return nil, err
			}
		}
	}

	// Collect indices
	indices := make([]int, 0, batchLen/2) // Pre-allocate estimate
	for i, b := range bitmap {
		if b != 0 {
			indices = append(indices, i)
		}
	}
	return indices, nil
}

// Reset binds the evaluator to a new record batch, reusing the existing filter operations.
func (e *FilterEvaluator) Reset(rec arrow.RecordBatch) error {
	if len(e.ops) == 0 {
		return nil
	}

	for _, op := range e.ops {
		var colIdx int
		// We need to access colIdx from the op. Since it's stored in the struct, we type switch.
		// Alternatively, we could add ColIdx() to the interface, but that exposes implementation detail.
		// Or we can just store colIdx in FilterEvaluator alongside ops?
		// FilterEvaluator currently has `ops []filterOp`.
		// Let's modify FilterEvaluator to store indices mapping ops to columns.

		// Actually, let's just stick to the plan:
		// We need to retrieve the column from the batch using the stored index.
		switch o := op.(type) {
		case *int64FilterOp:
			colIdx = o.colIdx
		case *float32FilterOp:
			colIdx = o.colIdx
		case *float64FilterOp:
			colIdx = o.colIdx
		case *stringFilterOp:
			colIdx = o.colIdx
		default:
			return fmt.Errorf("unknown filter op type")
		}

		if colIdx < 0 || colIdx >= int(rec.NumCols()) {
			return fmt.Errorf("column index %d out of bounds", colIdx)
		}

		col := rec.Column(colIdx)
		if err := op.Bind(col); err != nil {
			return err
		}
	}
	return nil
}

// EvaluateToArrowBoolean returns an Arrow Boolean Array mask for the batch.
func (e *FilterEvaluator) EvaluateToArrowBoolean(mem memory.Allocator, rows int) (*array.Boolean, error) {
	if len(e.ops) == 0 {
		// All match
		b := array.NewBooleanBuilder(mem)
		b.Reserve(rows)
		for i := 0; i < rows; i++ {
			b.Append(true)
		}
		return b.NewBooleanArray(), nil
	}

	// Use temporary byte bitmap
	bitmap := make([]byte, rows)
	e.ops[0].MatchBitmap(bitmap)

	if len(e.ops) > 1 {
		tmp := make([]byte, rows)
		for i := 1; i < len(e.ops); i++ {
			e.ops[i].MatchBitmap(tmp)
			if err := simd.AndBytes(bitmap, tmp); err != nil {
				return nil, err
			}
		}
	}

	// Pack to Arrow Boolean
	// Arrow booleans are bit-packed.
	// We can't just pass []byte (0/1).
	// We use builder for safety, though slowish.
	// Optimization: Manual bit packing.
	b := array.NewBooleanBuilder(mem)
	b.Reserve(rows)
	// Optimize this later with direct bit packing if needed
	for _, v := range bitmap {
		b.Append(v != 0)
	}
	return b.NewBooleanArray(), nil
}
