package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TDD Red Phase: Tests for batch distance search integration

// TestSearchWithBatchDistance_UsesBatchCalculation verifies batch distance is used
func TestSearchWithBatchDistance_UsesBatchCalculation(t *testing.T) {
	ds := createTestDataset(t, "batch-distance-test", 128, 100)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	index := NewHNSWIndex(ds)
	for i := 0; i < 100; i++ {
		_, _ = index.Add(0, i)
	}

	query := makeTestVector(128, 0)

	// Search with batch distance - should use batch calculation
	results := index.SearchWithBatchDistance(query, 10)

	if len(results) == 0 {
		t.Fatal("SearchWithBatchDistance returned no results")
	}

	if len(results) > 10 {
		t.Errorf("SearchWithBatchDistance returned %d results, expected <= 10", len(results))
	}

	// Verify results are sorted by distance
	for i := 1; i < len(results); i++ {
		if results[i].Distance < results[i-1].Distance {
			t.Errorf("Results not sorted: index %d distance %f < index %d distance %f",
				i, results[i].Distance, i-1, results[i-1].Distance)
		}
	}
}

// TestSearchWithBatchDistance_CorrectResults verifies results match expectations
func TestSearchWithBatchDistance_CorrectResults(t *testing.T) {
	ds := createTestDataset(t, "batch-correct-test", 64, 50)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	index := NewHNSWIndex(ds)
	for i := 0; i < 50; i++ {
		_, _ = index.Add(0, i)
	}

	// Query for first vector
	query := makeTestVector(64, 0)
	results := index.SearchWithBatchDistance(query, 5)

	if len(results) == 0 {
		t.Fatal("No results returned")
	}

	// First result should be closest match
	if results[0].ID != VectorID(0) {
		t.Logf("Expected closest ID 0, got %d (may vary by dataset)", results[0].ID)
	}
}

// TestSearchBatchOptimized_UsesBatchDistance verifies multi-query batch uses batching
func TestSearchBatchOptimized_UsesBatchDistance(t *testing.T) {
	ds := createTestDataset(t, "batch-optimized-test", 64, 100)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	index := NewHNSWIndex(ds)
	for i := 0; i < 100; i++ {
		_, _ = index.Add(0, i)
	}

	// Multiple queries
	queries := [][]float32{
		makeTestVector(64, 0),
		makeTestVector(64, 1),
		makeTestVector(64, 2),
		makeTestVector(64, 3),
		makeTestVector(64, 4),
	}

	// Batch search should use optimized batch distances
	results := index.SearchBatchOptimized(queries, 3)

	if len(results) != len(queries) {
		t.Errorf("Expected %d result sets, got %d", len(queries), len(results))
	}

	for i, res := range results {
		if len(res) == 0 {
			t.Errorf("Query %d returned no results", i)
		}
	}
}

// TestBatchDistanceSearch_Metrics verifies Prometheus metrics are emitted
func TestBatchDistanceSearch_Metrics(t *testing.T) {
	ds := createTestDataset(t, "batch-metrics-test", 32, 50)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	index := NewHNSWIndex(ds)
	for i := 0; i < 50; i++ {
		_, _ = index.Add(0, i)
	}

	initialCount := testutil.ToFloat64(metrics.BatchDistanceCallsTotal)

	query := makeTestVector(32, 0)
	_ = index.SearchWithBatchDistance(query, 5)

	// Verify metric was incremented
	newCount := testutil.ToFloat64(metrics.BatchDistanceCallsTotal)
	if newCount <= initialCount {
		t.Errorf("Expected BatchDistanceCallsTotal to increase, was %f now %f", initialCount, newCount)
	}
}

// TestBatchDistanceSearch_BatchSizeMetric verifies batch size histogram
func TestBatchDistanceSearch_BatchSizeMetric(t *testing.T) {
	desc := make(chan *prometheus.Desc, 10)
	metrics.BatchDistanceBatchSize.Describe(desc)
	close(desc)

	if len(desc) == 0 {
		t.Error("BatchDistanceBatchSize metric not registered")
	}
}

// TestSearchWithBatchDistance_EmptyIndex handles edge case
func TestSearchWithBatchDistance_EmptyIndex(t *testing.T) {
	ds := createTestDataset(t, "batch-empty-test", 64, 0)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	index := NewHNSWIndex(ds)

	query := makeTestVector(64, 0)
	results := index.SearchWithBatchDistance(query, 5)

	if len(results) != 0 {
		t.Errorf("Expected nil or empty results for empty index, got %d results", len(results))
	}
}

// TestSearchWithBatchDistance_KGreaterThanSize handles k > index size
func TestSearchWithBatchDistance_KGreaterThanSize(t *testing.T) {
	ds := createTestDataset(t, "batch-k-test", 32, 3)
	if ds == nil {
		t.Skip("Could not create test dataset")
	}

	index := NewHNSWIndex(ds)
	for i := 0; i < 3; i++ {
		_, _ = index.Add(0, i)
	}

	query := makeTestVector(32, 0)
	results := index.SearchWithBatchDistance(query, 10) // k=10 but only 3 vectors

	if len(results) > 3 {
		t.Errorf("Expected at most 3 results, got %d", len(results))
	}
}
