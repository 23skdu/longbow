package query

import (
	"bytes"
	"fmt"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type stringFilterOp struct {
	col      *array.String
	val      string
	operator string
	colIdx   int

	dict    *StringDictionary
	valCode uint16
	hasCode bool
	codes   []uint16
}

func (o *stringFilterOp) SetDictionaryCodes(codes []uint16, dict *StringDictionary) {
	o.codes = codes
	o.dict = dict
	if dict != nil {
		o.valCode, o.hasCode = dict.Lookup(o.val)
	}
}

func (o *stringFilterOp) Compound() bool { return false }
func (o *stringFilterOp) MatchValue(val interface{}) bool {
	v, ok := val.(string)
	if !ok {
		return false
	}
	return o.compareString(v)
}
func (o *stringFilterOp) compareString(v string) bool {
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
func (o *stringFilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.STRING {
		return fmt.Errorf("expected String column, got %s", col.DataType())
	}
	o.col = col.(*array.String)
	if o.dict == nil {
		o.dict = NewStringDictionary()
	}
	o.codes = o.dict.EncodeArray(o.col)
	o.valCode, o.hasCode = o.dict.Lookup(o.val)
	return nil
}

func (o *stringFilterOp) Reset(rec arrow.RecordBatch) error {
	if o.colIdx < 0 || o.colIdx >= int(rec.NumCols()) {
		return fmt.Errorf("column index %d out of bounds", o.colIdx)
	}
	return o.Bind(rec.Column(o.colIdx))
}

func (o *stringFilterOp) Match(rowIdx int) bool {
	if o.col.NullN() > 0 && o.col.IsNull(rowIdx) {
		return false
	}

	if o.codes != nil && rowIdx < len(o.codes) {
		switch o.operator {
		case "=", "eq", "==":
			if !o.hasCode {
				return false
			}
			return o.codes[rowIdx] == o.valCode
		case "!=", "neq":
			if !o.hasCode {
				return true
			}
			return o.codes[rowIdx] != o.valCode
		}
	}

	data := o.col.Data()
	off := data.Offset()
	idx := rowIdx + off
	offsets := arrow.Int32Traits.CastFromBytes(data.Buffers()[1].Bytes())
	dataBuf := data.Buffers()[2].Bytes()

	s := offsets[idx]
	e := offsets[idx+1]
	val := dataBuf[s:e]
	valBytes := unsafe.Slice(unsafe.StringData(o.val), len(o.val)) // #nosec G103

	switch o.operator {
	case "=", "eq", "==":
		return bytes.Equal(val, valBytes)
	case "!=", "neq":
		return !bytes.Equal(val, valBytes)
	case ">", "gt":
		return bytes.Compare(val, valBytes) > 0
	case "<", "lt":
		return bytes.Compare(val, valBytes) < 0
	case ">=", "ge":
		return bytes.Compare(val, valBytes) >= 0
	case "<=", "le":
		return bytes.Compare(val, valBytes) <= 0
	}
	return false
}

func (o *stringFilterOp) MatchBitmap(dst []byte) {
	start := time.Now()
	defer func() {
		metrics.StringFilterOpsTotal.WithLabelValues(o.operator, "optimized").Inc()
		metrics.StringFilterDurationSeconds.WithLabelValues(o.operator, "optimized").Observe(time.Since(start).Seconds())
	}()

	if o.codes != nil && len(o.codes) == len(dst) {
		switch o.operator {
		case "=", "eq", "==":
			metrics.StringFilterEqualLengthTotal.Inc()
			if !o.hasCode {
				clear(dst)
				return
			}
			_ = simd.MatchUint16(o.codes, o.valCode, simd.CompareEq, dst)
			if o.col.NullN() > 0 {
				offset := o.col.Data().Offset()
				for i := 0; i < len(dst); i++ {
					if o.col.IsNull(i + offset) {
						dst[i] = 0
					}
				}
			}
			return
		case "!=", "neq":
			if !o.hasCode {
				for i := range dst {
					dst[i] = 1
				}
			} else {
				_ = simd.MatchUint16(o.codes, o.valCode, simd.CompareNeq, dst)
			}
			if o.col.NullN() > 0 {
				offset := o.col.Data().Offset()
				for i := 0; i < len(dst); i++ {
					if o.col.IsNull(i + offset) {
						dst[i] = 0
					}
				}
			}
			return
		}
	}

	data := o.col.Data()
	off := data.Offset()
	offsets := arrow.Int32Traits.CastFromBytes(data.Buffers()[1].Bytes())
	dataBuf := data.Buffers()[2].Bytes()
	n := len(dst)
	valBytes := unsafe.Slice(unsafe.StringData(o.val), len(o.val)) // #nosec G103
	valLen := len(valBytes)

	hasNulls := o.col.NullN() > 0
	var validity []byte
	if hasNulls {
		validity = data.Buffers()[0].Bytes()
	}

	switch o.operator {
	case "=", "eq", "==":
		metrics.StringFilterEqualLengthTotal.Inc()

		if valLen == 0 {
			for i := 0; i < n; i++ {
				idx := off + i
				if hasNulls && (validity[idx/8]>>(idx%8))&1 == 0 {
					dst[i] = 0
				} else if offsets[idx+1]-offsets[idx] == 0 {
					dst[i] = 1
				} else {
					dst[i] = 0
				}
			}
			return
		}

		for i := 0; i < n; i++ {
			idx := off + i
			if hasNulls && (validity[idx/8]>>(idx%8))&1 == 0 {
				dst[i] = 0
				continue
			}
			s := offsets[idx]
			e := offsets[idx+1]
			if int(e-s) != valLen {
				dst[i] = 0
				continue
			}
			metrics.StringFilterComparisonsTotal.Inc()
			metrics.StringFilterBytesComparedTotal.Add(float64(valLen))
			if bytes.Equal(dataBuf[s:e], valBytes) {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}

	case "!=", "neq":
		for i := 0; i < n; i++ {
			idx := off + i
			if hasNulls && (validity[idx/8]>>(idx%8))&1 == 0 {
				dst[i] = 0
				continue
			}
			s := offsets[idx]
			e := offsets[idx+1]
			if int(e-s) != valLen {
				dst[i] = 1
				continue
			}
			metrics.StringFilterComparisonsTotal.Inc()
			metrics.StringFilterBytesComparedTotal.Add(float64(valLen))
			if bytes.Equal(dataBuf[s:e], valBytes) {
				dst[i] = 0
			} else {
				dst[i] = 1
			}
		}

	case ">", "gt":
		for i := 0; i < n; i++ {
			idx := off + i
			if hasNulls && (validity[idx/8]>>(idx%8))&1 == 0 {
				dst[i] = 0
				continue
			}
			s := offsets[idx]
			e := offsets[idx+1]
			if bytes.Compare(dataBuf[s:e], valBytes) > 0 {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}

	case "<", "lt":
		for i := 0; i < n; i++ {
			idx := off + i
			if hasNulls && (validity[idx/8]>>(idx%8))&1 == 0 {
				dst[i] = 0
				continue
			}
			s := offsets[idx]
			e := offsets[idx+1]
			if bytes.Compare(dataBuf[s:e], valBytes) < 0 {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}

	case ">=", "ge":
		for i := 0; i < n; i++ {
			idx := off + i
			if hasNulls && (validity[idx/8]>>(idx%8))&1 == 0 {
				dst[i] = 0
				continue
			}
			s := offsets[idx]
			e := offsets[idx+1]
			if bytes.Compare(dataBuf[s:e], valBytes) >= 0 {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}

	case "<=", "le":
		for i := 0; i < n; i++ {
			idx := off + i
			if hasNulls && (validity[idx/8]>>(idx%8))&1 == 0 {
				dst[i] = 0
				continue
			}
			s := offsets[idx]
			e := offsets[idx+1]
			if bytes.Compare(dataBuf[s:e], valBytes) <= 0 {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}

	default:
		metrics.StringFilterOpsTotal.WithLabelValues(o.operator, "slow").Inc()
		for i := 0; i < n; i++ {
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

type boolFilterOp struct {
	col      *array.Boolean
	val      bool
	operator string
	colIdx   int
}

func (o *boolFilterOp) Compound() bool { return false }
func (o *boolFilterOp) MatchValue(val interface{}) bool {
	if v, ok := val.(bool); ok {
		return o.compareBool(v)
	}
	return false
}
func (o *boolFilterOp) compareBool(v bool) bool {
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	}
	return false
}
func (o *boolFilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.BOOL {
		return fmt.Errorf("expected boolean column, got %s", col.DataType())
	}
	o.col = col.(*array.Boolean)
	return nil
}

func (o *boolFilterOp) Reset(rec arrow.RecordBatch) error {
	if o.colIdx < 0 || o.colIdx >= int(rec.NumCols()) {
		return fmt.Errorf("column index %d out of bounds", o.colIdx)
	}
	return o.Bind(rec.Column(o.colIdx))
}
func (o *boolFilterOp) Match(rowIdx int) bool {
	data := o.col.Data().Buffers()[1].Bytes()
	offset := o.col.Data().Offset()
	idx := rowIdx + offset

	// Check null bitmap first
	if o.col.NullN() > 0 {
		validity := o.col.Data().Buffers()[0].Bytes()
		if len(validity) > 0 && (validity[idx/8]>>(idx%8))&1 == 0 {
			return false
		}
	}

	bit := (data[idx/8] >> (idx % 8)) & 1
	return o.compareBool(bit == 1)
}
func (o *boolFilterOp) MatchBitmap(dst []byte) {
	dataBuf := o.col.Data().Buffers()[1].Bytes()
	offset := o.col.Data().Offset()
	n := len(dst)
	if n == 0 {
		return
	}

	// wantTrue: we want dest byte = 1 when the bool value matches.
	// This is true when (val == true AND op is equality) OR (val == false AND op is inequality).
	wantTrue := (o.val && (o.operator == "=" || o.operator == "eq" || o.operator == "==")) ||
		(!o.val && (o.operator == "!=" || o.operator == "neq"))

	for i := 0; i < n; i += 8 {
		sb := dataBuf[(i+offset)/8]
		end := i + 8
		if end > n {
			end = n
		}
		for j := i; j < end; j++ {
			bit := (sb >> ((j + offset) % 8)) & 1
			if wantTrue {
				dst[j] = bit
			} else {
				dst[j] = 1 ^ bit
			}
		}
	}

	// Null bitmap: 1 = valid, 0 = null. Zero out null entries.
	if o.col.NullN() > 0 {
		validity := o.col.Data().Buffers()[0].Bytes()
		if len(validity) > 0 {
			for i := 0; i < n; i++ {
				if (validity[(i+offset)/8]>>((i+offset)%8))&1 == 0 {
					dst[i] = 0
				}
			}
		}
	}
}
func (o *boolFilterOp) FilterBatch(indices []int) []int {
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if o.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}
