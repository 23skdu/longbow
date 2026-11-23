package textutil

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TokenizeAlphaNum(s string) []string {
	var tokens []string
	var cur strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			cur.WriteByte(c)
		} else if cur.Len() > 0 {
			tokens = append(tokens, cur.String())
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}
	return tokens
}

func ReadRowStringValue(rec arrow.RecordBatch, colIdx map[string]int, meta map[string]any, row int, field string) string {
	if idx, ok := colIdx[field]; ok {
		col := rec.Column(idx)
		if !col.IsNull(row) {
			if sa, ok := col.(*array.String); ok {
				return sa.Value(row)
			}
			return fmt.Sprintf("%v", col.GetOneForMarshal(row))
		}
	}
	if meta != nil {
		if v, ok := meta[field]; ok && v != nil {
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}
