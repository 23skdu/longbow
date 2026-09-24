package main

import (
	"testing"
)

func TestStatsOperations(t *testing.T) {
	stats := &Stats{}

	stats.IngestOps.Add(5)
	stats.DenseSearches.Add(10)
	stats.SparseSearches.Add(3)
	stats.FilteredSearches.Add(2)
	stats.HybridSearches.Add(4)
	stats.GlobalSearches.Add(1)
	stats.BM25Searches.Add(7)
	stats.RerankedSearches.Add(6)
	stats.CacheHits.Add(8)
	stats.CacheMisses.Add(2)
	stats.DeleteOps.Add(1)
	stats.Errors.Add(0)

	if got := stats.IngestOps.Load(); got != 5 {
		t.Errorf("IngestOps = %d; want 5", got)
	}
	if got := stats.DenseSearches.Load(); got != 10 {
		t.Errorf("DenseSearches = %d; want 10", got)
	}

	totalSearches := stats.DenseSearches.Load() + stats.SparseSearches.Load() +
		stats.FilteredSearches.Load() + stats.HybridSearches.Load() +
		stats.GlobalSearches.Load() + stats.BM25Searches.Load() + stats.RerankedSearches.Load()
	if totalSearches != 33 {
		t.Errorf("totalSearches = %d; want 33", totalSearches)
	}

	totalCache := stats.CacheHits.Load() + stats.CacheMisses.Load()
	if totalCache != 10 {
		t.Errorf("totalCache = %d; want 10", totalCache)
	}
	hitRate := float64(stats.CacheHits.Load()) / float64(totalCache) * 100
	if hitRate != 80.0 {
		t.Errorf("hitRate = %f; want 80.0", hitRate)
	}
}

func TestSearchModes(t *testing.T) {
	modes := []SearchMode{
		DenseSearch,
		SparseSearch,
		FilteredSearch,
		HybridSearch,
		GlobalSearch,
		BM25Search,
		RerankedSearch,
	}

	names := []string{"Dense", "Sparse", "Filtered", "Hybrid", "Global", "BM25", "Reranked"}

	if len(modes) != len(names) {
		t.Fatalf("modes count (%d) != names count (%d)", len(modes), len(names))
	}

	for i, m := range modes {
		if int(m) != i {
			t.Errorf("SearchMode %s = %d; want %d", names[i], m, i)
		}
	}
}

func TestBuildSoakSchema(t *testing.T) {
	sch := buildSoakSchema(128)
	if sch == nil {
		t.Fatal("expected non-nil schema")
	}
	if len(sch.Fields()) != 10 {
		t.Fatalf("expected 10 fields, got %d", len(sch.Fields()))
	}
	if sch.Field(0).Name != "id" {
		t.Errorf("first field name = %s; want id", sch.Field(0).Name)
	}
	if sch.Field(1).Name != "embedding" {
		t.Errorf("second field name = %s; want embedding", sch.Field(1).Name)
	}
}

