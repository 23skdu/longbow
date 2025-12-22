package store

import (
	"fmt"
	"sync"
	"testing"
)

// TestZeroAllocTicketParserConcurrent tests that parser is thread-safe
// Run with: go test -race -run TestZeroAllocTicketParserConcurrent
func TestZeroAllocTicketParserConcurrent(t *testing.T) {
	const numGoroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines*iterations)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Each goroutine parses with unique dataset name
				json := fmt.Sprintf(`{"name":"dataset_%d_%d","limit":%d}`, id, i, id*1000+i)

				// Use the pool-based function (to be implemented)
				result, err := ParseTicketQuerySafe([]byte(json))
				if err != nil {
					errChan <- fmt.Errorf("goroutine %d iter %d: parse error: %v", id, i, err)
					continue
				}

				// Verify result is correct for THIS goroutine
				expectedName := fmt.Sprintf("dataset_%d_%d", id, i)
				expectedLimit := int64(id*1000 + i)

				if result.Name != expectedName {
					errChan <- fmt.Errorf("goroutine %d iter %d: expected name %q, got %q",
						id, i, expectedName, result.Name)
				}
				if result.Limit != expectedLimit {
					errChan <- fmt.Errorf("goroutine %d iter %d: expected limit %d, got %d",
						id, i, expectedLimit, result.Limit)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errChan)

	errors := make([]error, 0, numGoroutines)
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		t.Errorf("Got %d errors during concurrent parsing:", len(errors))
		for i, err := range errors {
			if i < 10 { // Show first 10
				t.Errorf("  %v", err)
			}
		}
	}
}

// TestZeroAllocParserPoolMetrics tests that pool metrics are tracked
func TestZeroAllocParserPoolMetrics(t *testing.T) {
	// Reset counters
	initialGets := GetParserPoolStats().Gets
	initialPuts := GetParserPoolStats().Puts

	// Parse some data
	for i := 0; i < 10; i++ {
		json := fmt.Sprintf(`{"name":"test_%d","limit":%d}`, i, i)
		_, err := ParseTicketQuerySafe([]byte(json))
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
	}

	stats := GetParserPoolStats()
	if stats.Gets < initialGets+10 {
		t.Errorf("Expected at least %d gets, got %d", initialGets+10, stats.Gets)
	}
	if stats.Puts < initialPuts+10 {
		t.Errorf("Expected at least %d puts, got %d", initialPuts+10, stats.Puts)
	}
}
