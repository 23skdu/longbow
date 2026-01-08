package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMergeSortedStreams(t *testing.T) {
	// Scenario 1: Basic merge of 3 sorted streams
	ch1 := make(chan []SearchResult, 3)
	ch2 := make(chan []SearchResult, 3)
	ch3 := make(chan []SearchResult, 3)

	go func() {
		ch1 <- []SearchResult{{ID: 1, Score: 0.1}, {ID: 2, Score: 0.4}}
		ch1 <- []SearchResult{{ID: 3, Score: 0.7}}
		close(ch1)
	}()

	go func() {
		ch2 <- []SearchResult{{ID: 4, Score: 0.2}, {ID: 5, Score: 0.5}}
		close(ch2)
	}()

	go func() {
		ch3 <- []SearchResult{{ID: 6, Score: 0.3}, {ID: 7, Score: 0.6}}
		ch3 <- []SearchResult{{ID: 8, Score: 0.9}}
		close(ch3)
	}()

	merged := MergeSortedStreams([]<-chan []SearchResult{ch1, ch2, ch3}, 10)

	results := make([]SearchResult, 0, 6)
	for r := range merged {
		results = append(results, r)
	}

	assert.Equal(t, 8, len(results))
	// Check strict ordering
	for i := 1; i < len(results); i++ {
		assert.LessOrEqual(t, results[i-1].Score, results[i].Score, "Results should be sorted by score")
	}
}

func TestMergeSortedStreams_EmptyChannels(t *testing.T) {
	ch1 := make(chan []SearchResult)
	close(ch1)

	ch2 := make(chan []SearchResult, 1)
	ch2 <- []SearchResult{{ID: 1, Score: 0.5}}
	close(ch2)

	merged := MergeSortedStreams([]<-chan []SearchResult{ch1, ch2}, 5)

	results := make([]SearchResult, 0, 4)
	for r := range merged {
		results = append(results, r)
	}

	assert.Equal(t, 1, len(results))
	assert.Equal(t, float32(0.5), results[0].Score)
}

func TestMergeSortedStreams_LimitK(t *testing.T) {
	ch1 := make(chan []SearchResult, 1)

	go func() {
		ch1 <- []SearchResult{{ID: 1, Score: 0.1}, {ID: 2, Score: 0.2}, {ID: 3, Score: 0.3}}
		close(ch1)
	}()

	// Request only top 2
	merged := MergeSortedStreams([]<-chan []SearchResult{ch1}, 2)

	count := 0
	for range merged {
		count++
	}
	assert.Equal(t, 2, count)
}

func TestMergeSortedStreams_InterleavedDelays(t *testing.T) {
	ch1 := make(chan []SearchResult)
	ch2 := make(chan []SearchResult)

	go func() {
		ch1 <- []SearchResult{{ID: 1, Score: 0.1}}
		time.Sleep(50 * time.Millisecond)
		ch1 <- []SearchResult{{ID: 2, Score: 0.9}}
		close(ch1)
	}()

	go func() {
		time.Sleep(10 * time.Millisecond)
		ch2 <- []SearchResult{{ID: 3, Score: 0.2}}
		ch2 <- []SearchResult{{ID: 4, Score: 0.5}}
		close(ch2)
	}()

	merged := MergeSortedStreams([]<-chan []SearchResult{ch1, ch2}, 10)

	results := make([]SearchResult, 0, 4)
	for r := range merged {
		results = append(results, r)
	}

	// 0.1, 0.2, 0.5, 0.9
	expectedScores := []float32{0.1, 0.2, 0.5, 0.9}
	assert.Equal(t, len(expectedScores), len(results))
	for i, s := range expectedScores {
		assert.Equal(t, s, results[i].Score)
	}
}
