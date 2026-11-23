package engine

import (
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// buildP3TextBatch builds the minimal id/text Arrow batch used by text corpus
// white-box tests. It is local to the engine test binary because the production
// implementation now lives in the textsearch domain.
func buildP3TextBatch(offset, rows int, textFn func(int) string) arrow.RecordBatch {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "text", Type: arrow.BinaryTypes.String},
	}, nil)
	ids := array.NewStringBuilder(pool)
	texts := array.NewStringBuilder(pool)
	defer ids.Release()
	defer texts.Release()
	for i := 0; i < rows; i++ {
		ids.Append(strconv.Itoa(offset + i))
		texts.Append(textFn(offset + i))
	}
	return array.NewRecordBatch(schema, []arrow.Array{ids.NewArray(), texts.NewArray()}, int64(rows))
}
