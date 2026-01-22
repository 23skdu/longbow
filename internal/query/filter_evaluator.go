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
	start := time.Now()
	defer func() {
		metrics.StringFilterOpsTotal.WithLabelValues(o.operator, "optimized").Inc()
		metrics.StringFilterDurationSeconds.WithLabelValues(o.operator, "optimized").Observe(time.Since(start).Seconds())
	}()

	valLen := len(o.val)

	switch o.operator {
	case "=", "eq", "==":
		metrics.StringFilterEqualLengthTotal.Inc()

		if valLen == 0 {
			for i := 0; i < len(dst); i++ {
				switch {
				case o.col.IsNull(i):
					dst[i] = 0
				case o.col.ValueLen(i) == 0:
					dst[i] = 1
				default:
					dst[i] = 0
				}
			}
			return
		}

		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i) {
				dst[i] = 0
				continue
			}
			if o.col.ValueLen(i) != valLen {
				dst[i] = 0
				continue
			}

			s := o.col.Value(i)
			metrics.StringFilterComparisonsTotal.Inc()
			metrics.StringFilterBytesComparedTotal.Add(float64(valLen))

			match := true
			for j := 0; j < valLen; j++ {
				if s[j] != o.val[j] {
					match = false
					break
				}
			}
			if match {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}

	case "!=", "neq":
		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i) {
				dst[i] = 0
				continue
			}
			if o.col.ValueLen(i) != valLen {
				dst[i] = 1
				continue
			}

			s := o.col.Value(i)
			metrics.StringFilterComparisonsTotal.Inc()
			metrics.StringFilterBytesComparedTotal.Add(float64(valLen))

			match := true
			for j := 0; j < valLen; j++ {
				if s[j] != o.val[j] {
					match = false
					break
				}
			}
			if match {
				dst[i] = 0
			} else {
				dst[i] = 1
			}
		}

	case ">", "gt":
		for i := 0; i < len(dst); i++ {
			switch {
			case o.col.IsNull(i):
				dst[i] = 0
			case o.col.Value(i) > o.val:
				dst[i] = 1
			default:
				dst[i] = 0
			}
		}

	case "<", "lt":
		for i := 0; i < len(dst); i++ {
			switch {
			case o.col.IsNull(i):
				dst[i] = 0
			case o.col.Value(i) < o.val:
				dst[i] = 1
			default:
				dst[i] = 0
			}
		}

	case ">=", "ge":
		for i := 0; i < len(dst); i++ {
			switch {
			case o.col.IsNull(i):
				dst[i] = 0
			case o.col.Value(i) >= o.val:
				dst[i] = 1
			default:
				dst[i] = 0
			}
		}

	case "<=", "le":
		for i := 0; i < len(dst); i++ {
			switch {
			case o.col.IsNull(i):
				dst[i] = 0
			case o.col.Value(i) <= o.val:
				dst[i] = 1
			default:
				dst[i] = 0
			}
		}

	default:
		metrics.StringFilterOpsTotal.WithLabelValues(o.operator, "slow").Inc()
		for i := 0; i < len(dst); i++ {
			if o.Match(i) {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
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
// Uses Bloom filter-inspired optimization: reorders filters by selectivity and early exits when all rows are rejected.
func (e *FilterEvaluator) MatchesAll(batchLen int) ([]int, error) {
	start := time.Now()
	defer func() {
		metrics.FilterEvaluatorOpsTotal.WithLabelValues("MatchesAll").Inc()
		metrics.FilterEvaluatorDurationSeconds.WithLabelValues("MatchesAll").Observe(time.Since(start).Seconds())
	}()

	if len(e.ops) == 0 {
		// Return all indices
		indices := make([]int, batchLen)
		for i := 0; i < batchLen; i++ {
			indices[i] = i
		}
		return indices, nil
	}

	// Reorder filters by selectivity (high selectivity = few matches = run first)
	// This enables early exit optimization
	sortedOps := selectOpsBySelectivity(e.ops)

	// Allocate bitmaps
	bitmap := make([]byte, batchLen)
	sortedOps[0].MatchBitmap(bitmap)

	// Early exit if first filter rejects all rows
	if isBitmapAllZeros(bitmap) {
		metrics.BloomFilterEarlyExitsTotal.Inc()
		metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesAll", "early_exit").Add(0)
		return []int{}, nil
	}

	if len(sortedOps) > 1 {
		tmp := make([]byte, batchLen)
		for i := 1; i < len(sortedOps); i++ {
			sortedOps[i].MatchBitmap(tmp)
			// Intersect (AND)
			if err := simd.AndBytes(bitmap, tmp); err != nil {
				return nil, err
			}

			// Early exit if bitmap becomes all zeros (all remaining rows rejected)
			if isBitmapAllZeros(bitmap) {
				metrics.BloomFilterEarlyExitsTotal.Inc()
				metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesAll", "early_exit").Add(float64(len(tmp)))
				return []int{}, nil
			}
		}
	}

	// Collect indices
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

// estimateSelectivity estimates how selective a filter is (0-1, where 1 means all rows match).
// Higher selectivity means fewer rows pass the filter, which is better for early exit optimization.
func estimateSelectivity(op filterOp, sampleSize int) float64 {
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
	default:
		return 0.5 // Default to neutral selectivity for unknown types
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
