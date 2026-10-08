package store

// Tests pinning the semantics of temporal as-of search (roadmap R11b).
//
// SearchAsOf allocates a query vector and never fills it. That looks like a bug
// and was tracked as one, until the request type was read: TemporalSearchRequest
// carries no query vector at all - only a dataset, a search type, k, a timestamp
// and filters. There is nothing to populate, so the operation was never a
// similarity search. It is an enumeration: "give me k records that existed as of
// timestamp T".
//
// These tests pin what that means in practice, because the consequence is not
// obvious and is not the same as the other modes in the matrix. Routing an
// enumeration through a vector-index search ranks the visible records by their
// distance to the zero vector, i.e. by norm. So:
//
//   - the set returned is the k smallest-norm records visible at that timestamp,
//     not the k most recent, and not an arbitrary k;
//   - distances are distances to the origin and carry no semantic meaning; and
//   - which k you get depends on the index and its traversal, so this mode's
//     numbers are not comparable against a mode that takes a real query vector.
//
// That last point is why temporal results must not be compared against the rest
// of the benchmark matrix.

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/core"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The zero query vector is load-bearing: records are ranked by distance to the
// origin, so the smallest-norm visible record must come first.
func TestSearchAsOfRanksByDistanceToOrigin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(4)
	now := time.Now().UnixNano()

	// Norms: id1 ~1.0, id2 ~2.0, id3 ~3.0. All visible at `now`.
	require.NoError(t, ti.Add(1, []float32{1, 0, 0, 0}, now-1000, nil))
	require.NoError(t, ti.Add(2, []float32{2, 0, 0, 0}, now-1000, nil))
	require.NoError(t, ti.Add(3, []float32{3, 0, 0, 0}, now-1000, nil))

	results, err := ti.SearchAsOf(context.Background(), now, 3)
	require.NoError(t, err)
	require.NotEmpty(t, results)

	// Ascending distance from the origin, not insertion order and not recency.
	for i := 1; i < len(results); i++ {
		assert.GreaterOrEqual(t, results[i].Distance, results[i-1].Distance,
			"results are not ordered by distance to the origin")
	}
	assert.Equal(t, lbtypes.VectorID(1), results[0].ID,
		"expected the smallest-norm record first")
}

// Records that did not exist yet must be invisible, which is the entire point of
// the timestamp argument.
func TestSearchAsOfExcludesRecordsAddedAfterTheTimestamp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(4)
	now := time.Now().UnixNano()

	require.NoError(t, ti.Add(1, []float32{1, 0, 0, 0}, now-1000, nil))
	require.NoError(t, ti.Add(2, []float32{0.5, 0, 0, 0}, now+1_000_000, nil))

	results, err := ti.SearchAsOf(context.Background(), now, 10)
	require.NoError(t, err)

	for _, r := range results {
		assert.NotEqual(t, lbtypes.VectorID(2), r.ID,
			"a record added after the as-of timestamp must not be returned")
	}
	assert.NotEmpty(t, results)
}

// k bounds the enumeration, so a larger k cannot return fewer records than a
// smaller one for the same timestamp.
func TestSearchAsOfIsMonotonicInK(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ti := NewTemporalIndex(4)
	now := time.Now().UnixNano()

	for i := 1; i <= 8; i++ {
		require.NoError(t, ti.Add(uint64(i), []float32{float32(i), 0, 0, 0}, now-1000, nil))
	}

	small, err := ti.SearchAsOf(context.Background(), now, 3)
	require.NoError(t, err)
	large, err := ti.SearchAsOf(context.Background(), now, 8)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, len(large), len(small),
		"a larger k must not return fewer records")
}

// This is the property the roadmap caveat rests on: as-of search does not take a
// query vector, so it cannot be measuring relevance the way every other mode does.
// The reflection check is the real guard - if TemporalSearchRequest ever grows a
// vector field, this fails and forces the semantics to be revisited rather than
// quietly changed underneath the tests that pin the current behaviour.
func TestAsOfRequestCarriesNoQueryVector(t *testing.T) {
	req := &core.TemporalSearchRequest{
		Dataset:    "ds",
		SearchType: "as_of",
		K:          10,
		Timestamp:  time.Now().UnixNano(),
	}

	// A request with no vector must still be valid - that is the documented
	// contract of this mode.
	assert.NoError(t, req.Validate())

	rt := reflect.TypeOf(*req)
	vectorFields := []string{}
	for i := 0; i < rt.NumField(); i++ {
		name := strings.ToLower(rt.Field(i).Name)
		if strings.Contains(name, "vector") || name == "query" || name == "embedding" {
			vectorFields = append(vectorFields, rt.Field(i).Name)
		}
	}
	assert.Empty(t, vectorFields,
		"TemporalSearchRequest now carries %v; as-of search ranks by distance to the "+
			"origin because there is no query vector, so adding one changes what this "+
			"mode measures and the tests in this file need revisiting", vectorFields)
}
