package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTicketQuerySafe_RangeAndFederated(t *testing.T) {
	raw := []byte(`{
		"name": "docs",
		"range_search": {
			"dataset": "docs",
			"vector": [0.1, 0.2, 0.3],
			"threshold": 0.45,
			"max_results": 25,
			"filters": [{"field": "category", "operator": "=", "value": "security"}]
		},
		"federated_search": {
			"collections": ["col_a", "col_b"],
			"tags": ["finance", "legal"],
			"vector": [0.4, 0.5, 0.6],
			"k": 15,
			"rrf_k": 40
		},
		"geo_search": {
			"dataset": "places",
			"search_type": "hybrid",
			"center": {"lat": 37.77, "lon": -122.41},
			"radius_km": 12.5,
			"query_vector": [1.0, 0.0],
			"k": 5
		},
		"temporal_search": {
			"dataset": "events",
			"search_type": "sliding_window_time",
			"duration": "5s",
			"k": 8
		}
	}`)

	q, err := ParseTicketQuerySafe(raw)
	require.NoError(t, err)

	require.NotNil(t, q.RangeSearch)
	assert.Equal(t, "docs", q.RangeSearch.Dataset)
	assert.InDelta(t, float32(0.45), q.RangeSearch.Threshold, 1e-5)
	assert.Equal(t, 25, q.RangeSearch.MaxResults)
	assert.Equal(t, []float32{0.1, 0.2, 0.3}, q.RangeSearch.Vector)
	require.Len(t, q.RangeSearch.Filters, 1)
	assert.Equal(t, "category", q.RangeSearch.Filters[0].Field)

	require.NotNil(t, q.FederatedSearch)
	assert.Equal(t, []string{"col_a", "col_b"}, q.FederatedSearch.Collections)
	assert.Equal(t, []string{"finance", "legal"}, q.FederatedSearch.Tags)
	assert.Equal(t, []float32{0.4, 0.5, 0.6}, q.FederatedSearch.Vector)
	assert.Equal(t, 15, q.FederatedSearch.K)
	assert.Equal(t, 40, q.FederatedSearch.RRFK)

	require.NotNil(t, q.GeoSearch)
	assert.Equal(t, "hybrid", q.GeoSearch.SearchType)
	assert.Equal(t, []float32{1.0, 0.0}, q.GeoSearch.QueryVector)

	require.NotNil(t, q.TemporalSearch)
	assert.Equal(t, "sliding_window_time", q.TemporalSearch.SearchType)
	assert.Equal(t, 5*time.Second, q.TemporalSearch.Duration)
}

func FuzzParseRangeAndFederatedRequests(f *testing.F) {
	f.Add([]byte(`{"dataset":"ds","vector":[1.0,2.0],"threshold":0.5,"max_results":10}`), []byte(`{"collections":["a"],"vector":[1.0],"k":5}`))
	f.Add([]byte(`{}`), []byte(`{}`))
	f.Add([]byte(`{"vector":`), []byte(`{"tags":["x"`))
	f.Fuzz(func(t *testing.T, rangeJSON, fedJSON []byte) {
		var r RangeSearchRequest
		_ = ParseRangeSearchRequest(rangeJSON, &r)
		var fd FederatedSearchRequest
		_ = ParseFederatedSearchRequest(fedJSON, &fd)
	})
}
