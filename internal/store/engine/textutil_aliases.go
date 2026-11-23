package engine

import (
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/23skdu/longbow/internal/store/textsearch"
	"github.com/23skdu/longbow/internal/store/textutil"
)

// tokenizeAlphaNum keeps existing engine call sites and tests working after the
// helper moved to the shared textutil package.
func tokenizeAlphaNum(s string) []string { return textutil.TokenizeAlphaNum(s) }

// readRowStringValue keeps existing engine call sites working after the helper
// moved to the shared textutil package.
func readRowStringValue(rec arrow.RecordBatch, colIdx map[string]int, meta map[string]any, row int, field string) string {
	return textutil.ReadRowStringValue(rec, colIdx, meta, row, field)
}

// evaluateAdvancedTextQuery keeps engine white-box tests working after the
// text search AST moved to its own domain package.
func evaluateAdvancedTextQuery(query, docLower string, docTokens []string, defaultFuzzy int) (bool, []string) {
	return textsearch.EvaluateAdvancedTextQuery(query, docLower, docTokens, defaultFuzzy)
}
