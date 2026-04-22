package store

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsIntegration_HNSWSearch(t *testing.T) {
	// 1. Setup Metrics Handler
	// promauto registers with the default registry
	handler := promhttp.Handler()
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// 2. Setup Index
	config := DefaultArrowHNSWConfig()
	ds := &Dataset{Name: "test_dataset"}
	// Set name for labeling
	idx := NewArrowHNSW(ds, &config)

	// Insert some data to ensure search does something
	err := idx.InsertWithVector(0, []float32{1.0, 0.0}, 0)
	require.NoError(t, err)

	// Get initial count of search operations
	initialSearchOps := getMetricCount(t, ts.URL, "longbow_hnsw_search_ops_total", "dataset=\"test_dataset\"")

	// 3. Perform Searches
	numSearches := 5
	for i := 0; i < numSearches; i++ {
		_, err := idx.SearchVectors(context.Background(), []float32{1.0, 0.0}, 10, nil, SearchOptions{})
		require.NoError(t, err)
	}

	// 4. Verify Metrics
	finalSearchOps := getMetricCount(t, ts.URL, "longbow_hnsw_search_ops_total", "dataset=\"test_dataset\"")
	assert.Equal(t, initialSearchOps+float64(numSearches), finalSearchOps, "Search ops total should increment")

	// Verify duration metric exists
	durationCount := getMetricCount(t, ts.URL, "longbow_hnsw_search_duration_seconds_count", "")
	assert.True(t, durationCount >= float64(numSearches), "Search duration histogram count should increment")
}

func TestMetricsIntegration_HNSWInsert(t *testing.T) {
	handler := promhttp.Handler()
	ts := httptest.NewServer(handler)
	defer ts.Close()

	config := DefaultArrowHNSWConfig()
	ds := &Dataset{Name: "insert_dataset"}
	idx := NewArrowHNSW(ds, &config)

	initialInsertOps := getMetricCount(t, ts.URL, "longbow_hnsw_insert_ops_total", "dataset=\"insert_dataset\"")
	initialNodesAdded := getMetricCount(t, ts.URL, "longbow_hnsw_nodes_added_total", "dataset=\"insert_dataset\"")

	numInserts := 10
	for i := 0; i < numInserts; i++ {
		err := idx.InsertWithVector(uint32(i), []float32{0.5, 0.5}, 0)
		require.NoError(t, err)
	}

	finalInsertOps := getMetricCount(t, ts.URL, "longbow_hnsw_insert_ops_total", "dataset=\"insert_dataset\"")
	finalNodesAdded := getMetricCount(t, ts.URL, "longbow_hnsw_nodes_added_total", "dataset=\"insert_dataset\"")

	assert.Equal(t, initialInsertOps+float64(numInserts), finalInsertOps, "Insert ops total should increment")
	assert.Equal(t, initialNodesAdded+float64(numInserts), finalNodesAdded, "Nodes added total should increment")

	nodeCount := getMetricCount(t, ts.URL, "longbow_hnsw_node_count", "dataset=\"insert_dataset\"")
	assert.Equal(t, float64(idx.Len()), nodeCount, "Node count gauge should match HNSW state")
}

// Helper to scrape metrics and find a specific value
func getMetricCount(t *testing.T, url, metricName, labelMatch string) float64 {
	resp, err := http.Get(url + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.Contains(line, metricName) {
			if labelMatch == "" || strings.Contains(line, labelMatch) {
				var val float64
				// Format: metric_name{labels} value
				parts := strings.Fields(line)
				if len(parts) < 2 {
					continue
				}
				fmt.Sscanf(parts[len(parts)-1], "%f", &val)
				return val
			}
		}
	}
	return 0
}
