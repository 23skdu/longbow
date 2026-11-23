package textsearch

import (
	"strings"
	"testing"

	"github.com/23skdu/longbow/internal/store/textutil"
	"github.com/stretchr/testify/require"
)

func TestTextQueryParserAndScoringHelpers(t *testing.T) {
	ast, err := ParseAdvancedTextQuery(`"vector db" AND auth~1`)
	require.NoError(t, err)
	require.NotNil(t, ast)

	_, err = ParseAdvancedTextQuery(`)`)
	require.Error(t, err)

	require.Equal(t, 0, LevenshteinDistance("kitten", "kitten"))
	require.Equal(t, 1, LevenshteinDistance("ab", "ba"))

	highlighted, _ := GenerateSnippetHighlight("vector database", []string{"vector"}, "<mark>", "</mark>", 80)
	require.Contains(t, highlighted, "<mark>vector</mark>")
}

func FuzzTextQueryParserAndScoring(f *testing.F) {
	f.Add("vector AND db")
	f.Add(`"exact phrase"`)
	f.Add("auth~1 OR NOT legacy")
	f.Add("(((broken")
	f.Add("")

	f.Fuzz(func(t *testing.T, query string) {
		ast, err := ParseAdvancedTextQuery(query)
		if err == nil {
			if ast == nil {
				t.Fatal("successful parse must return a non-nil AST")
			}
			_, _ = EvaluateAdvancedTextQuery(query, strings.ToLower(query), textutil.TokenizeAlphaNum(query), 1)
		}
		if distance := LevenshteinDistance(query, query); distance != 0 {
			t.Fatalf("LevenshteinDistance(%q, %q) = %d, want 0", query, query, distance)
		}
	})
}
