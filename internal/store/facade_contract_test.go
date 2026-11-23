package store_test

import (
	"testing"
	"unicode/utf8"

	store "github.com/23skdu/longbow/internal/store"
	"github.com/stretchr/testify/require"
)

func TestFacadePureHelpers(t *testing.T) {
	require.NotNil(t, store.LevenshteinDistance)
	require.NotNil(t, store.ParseAdvancedTextQuery)
	require.NotNil(t, store.DefaultSLAPolicy)
	require.NotNil(t, store.NewRateLimiter)

	require.Equal(t, 0, store.LevenshteinDistance("kitten", "kitten"))
	require.Equal(t, 3, store.LevenshteinDistance("kitten", "sitting"))

	_, err := store.ParseAdvancedTextQuery(`"vector db" AND (auth~1 OR NOT legacy)`)
	require.NoError(t, err)
	_, err = store.ParseAdvancedTextQuery(`)`)
	require.Error(t, err)

	require.Equal(t, 99.99, store.DefaultSLAPolicy(store.CloudTierEnterprise).AvailabilityTarget)
	require.NotNil(t, store.NewRateLimiter(0))
}

func FuzzFacadeTextHelpers(f *testing.F) {
	f.Add("kitten", "kitten")
	f.Add("kitten", "sitting")
	f.Add("", "vector")
	f.Add("vector", "")

	f.Fuzz(func(t *testing.T, a, b string) {
		distance := store.LevenshteinDistance(a, b)
		maxLen := utf8.RuneCountInString(a)
		if bLen := utf8.RuneCountInString(b); bLen > maxLen {
			maxLen = bLen
		}
		if distance < 0 || distance > maxLen {
			t.Fatalf("LevenshteinDistance(%q, %q) = %d outside [0,%d]", a, b, distance, maxLen)
		}
		if a == b && distance != 0 {
			t.Fatalf("LevenshteinDistance(%q, %q) = %d, want 0", a, b, distance)
		}
		if a == "" {
			if want := utf8.RuneCountInString(b); distance != want {
				t.Fatalf("LevenshteinDistance(\"\", %q) = %d, want %d", b, distance, want)
			}
		}
	})
}
