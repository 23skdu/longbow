package metal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// FuzzEngine tests the engine with random inputs
func FuzzEngine(f *testing.F) {
	f.Fuzz(func(t *testing.T, query string, docCount int) {
		// Limit docCount to reasonable range
		if docCount < 0 || docCount > 1000 {
			docCount = docCount % 1000
		}

		engine, err := NewEngine()
		require.NoError(t, err)
		require.NotNil(t, engine)

		// Create documents
		docs := make([]string, docCount)
		for i := 0; i < docCount; i++ {
			docs[i] = query // Simple case
		}

		_, _ = engine.Score(context.Background(), query, docs)
		_, _ = engine.ScoreBatch(context.Background(), []string{query}, docs)

		_ = engine.Close()
	})
}

// FuzzEngineEmptyInputs tests edge cases with empty inputs
func FuzzEngineEmptyInputs(f *testing.F) {
	f.Fuzz(func(t *testing.T, query string) {
		engine, err := NewEngine()
		require.NoError(t, err)
		require.NotNil(t, engine)

		// Empty documents
		_, _ = engine.Score(context.Background(), query, []string{})
		_, _ = engine.ScoreBatch(context.Background(), []string{query}, []string{})
		_, _ = engine.ScoreBatch(context.Background(), []string{}, []string{query})

		_ = engine.Close()
	})
}

// FuzzEngineLongStrings tests with very long strings
func FuzzEngineLongStrings(f *testing.F) {
	f.Fuzz(func(t *testing.T, queryLen, docLen int) {
		// Limit lengths
		if queryLen < 0 {
			queryLen = -queryLen
		}
		if docLen < 0 {
			docLen = -docLen
		}
		if queryLen > 10000 {
			queryLen = 10000
		}
		if docLen > 10000 {
			docLen = 10000
		}

		engine, err := NewEngine()
		require.NoError(t, err)
		require.NotNil(t, engine)

		query := string(make([]byte, queryLen))
		doc := string(make([]byte, docLen))

		_, _ = engine.Score(context.Background(), query, []string{doc})

		_ = engine.Close()
	})
}
