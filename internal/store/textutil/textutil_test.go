package textutil

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestTokenizeAlphaNum(t *testing.T) {
	require.Equal(t, []string{"abc", "123", "def"}, TokenizeAlphaNum("Abc, 123_DEF!"))
	require.Empty(t, TokenizeAlphaNum("---"))
}

func TestReadRowStringValue(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	ids := array.NewStringBuilder(pool)
	scores := array.NewInt64Builder(pool)
	defer ids.Release()
	defer scores.Release()
	ids.Append("doc-1")
	scores.Append(7)
	record := array.NewRecordBatch(schema, []arrow.Array{ids.NewArray(), scores.NewArray()}, 1)
	defer record.Release()

	colIdx := map[string]int{"id": 0, "score": 1}
	meta := map[string]any{"fallback": "from-meta"}

	require.Equal(t, "doc-1", ReadRowStringValue(record, colIdx, nil, 0, "id"))
	require.Equal(t, "7", ReadRowStringValue(record, colIdx, nil, 0, "score"))
	require.Equal(t, "from-meta", ReadRowStringValue(record, colIdx, meta, 0, "fallback"))
	require.Empty(t, ReadRowStringValue(record, colIdx, meta, 0, "missing"))
}

func FuzzTokenizeAlphaNum(f *testing.F) {
	f.Add("Hello, WORLD 123")
	f.Add("")
	f.Add("---")
	f.Add("abc_def-42")

	f.Fuzz(func(t *testing.T, input string) {
		tokens := TokenizeAlphaNum(input)
		if len(tokens) > len(input) && input != "" {
			t.Fatalf("token count %d exceeds input length %d", len(tokens), len(input))
		}
		for _, token := range tokens {
			if token == "" {
				t.Fatal("tokenizer emitted an empty token")
			}
			for _, r := range token {
				if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
					t.Fatalf("token %q contains unexpected rune %q", token, r)
				}
			}
		}
		require.Equal(t, tokens, TokenizeAlphaNum(input))
	})
}
