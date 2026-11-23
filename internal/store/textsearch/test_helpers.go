// alias-only: not re-exported
package textsearch

// EvaluateAdvancedTextQuery evaluates an already supported advanced query
// string against a document. It is a narrow helper for host white-box tests
// that cannot access the unexported AST node interface.
func EvaluateAdvancedTextQuery(query, docLower string, docTokens []string, defaultFuzzy int) (bool, []string) {
	ast, err := ParseAdvancedTextQuery(query)
	if err != nil || ast == nil {
		return false, nil
	}
	return ast.evaluate(docLower, docTokens, defaultFuzzy)
}

// CorpusFor builds the cached corpus for a dataset and discards the result.
// It is a narrow helper for host white-box tests that verify corpus locking.
func (m *AdvancedTextSearchManager) CorpusFor(dataset string) error {
	_, err := m.corpusFor(dataset)
	return err
}
