package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// The benchmarks in this file use only the exported API, so the same file
// compiles against the pre-index VersionHistory as well. That is what makes a
// before/after comparison possible on the same machine at the same time: the
// paired in-process comparisons live in version_history_test.go, and these
// ones are the ones that can be run against the old implementation unchanged.

func newVersionHistoryAPI(nIDs, versionsPerID, maxVersions int) (*VersionHistory, []uint64) {
	vec := []float32{0.5, 0.25, 0.125}
	vh := NewVersionHistory(VersionHistoryConfig{MaxVersions: maxVersions, RetentionPeriod: time.Hour})
	ids := make([]uint64, nIDs)
	for i := range ids {
		ids[i] = uint64(i)
	}
	for i, id := range ids {
		for v := 0; v < versionsPerID; v++ {
			vh.Add(id, vec, float32(v), int64(v)*int64(nIDs)+int64(i), nil)
		}
	}
	return vh, ids
}

// versionHistoryAPIQuery is the timestamp that selects the middle version of
// every id.
func versionHistoryAPIQuery(nIDs, versionsPerID int) int64 {
	return int64(versionsPerID/2)*int64(nIDs) + int64(nIDs-1)
}

func BenchmarkVersionHistory_API_Batch(b *testing.B) {
	for _, nIDs := range []int{1000, 10000, 100000} {
		for _, versions := range []int{1, 10, 100} {
			vh, ids := newVersionHistoryAPI(nIDs, versions, versions+1)
			query := versionHistoryAPIQuery(nIDs, versions)
			out := make(map[uint64]VersionedVector, nIDs)
			// One warm-up batch, outside the timed loop: it publishes the
			// snapshot on the indexed implementation and is a plain read on
			// the old one.
			vh.GetVersionsAtBatch(ids, query, out)

			b.Run(fmt.Sprintf("ids=%d/versions=%d", nIDs, versions), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					clear(out)
					vh.GetVersionsAtBatch(ids, query, out)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(nIDs), "ns/id")
			})
		}
	}
}

func BenchmarkVersionHistory_API_Add(b *testing.B) {
	vec := []float32{0.5, 0.25, 0.125}
	const nIDs = 1000
	for _, held := range []int{1, 10, 100} {
		vh, ids := newVersionHistoryAPI(nIDs, held, held+1)
		ts := int64(nIDs * held)

		b.Run(fmt.Sprintf("versions=%d", held), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				ts++
				vh.Add(ids[i%len(ids)], vec, 1, ts, nil)
				i++
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/add")
		})
	}
}

// BenchmarkVersionHistory_API_SearchAsOfFullCorpus measures the end-to-end
// temporal search with every id live at the query timestamp, which is the shape
// where the history batch is the largest share of the work. The stock
// BenchmarkTemporalIndex_SearchAsOf queries at n/2 of a corpus whose timestamps
// are 10 apart, so only a twentieth of the ids are live and the batch is a
// third of the query; this one is the other end of the range.
func BenchmarkVersionHistory_API_SearchAsOfFullCorpus(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{100000} {
		ti := newColumnarBenchmarkIndex(b, n, true, 0, 1)
		validIDs := ti.temporalTree.Load().GetUniqueIDsInRange(0, int64(n-1))
		if len(validIDs) != n {
			b.Fatalf("expected %d live ids, got %d", n, len(validIDs))
		}
		ts := int64(n - 1)
		out := make(map[uint64]VersionedVector, n)
		// Warm-up: publishes the snapshot on the indexed implementation and
		// is a plain read on the old one.
		ti.SearchAsOf(ctx, ts, 10)
		ti.history.GetVersionsAtBatch(validIDs, ts, out)

		b.Run(fmt.Sprintf("n=%d/search-as-of", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				if _, err := ti.SearchAsOf(ctx, ts+int64(i), 10); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("n=%d/history-batch", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				clear(out)
				ti.history.GetVersionsAtBatch(validIDs, ts, out)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/id")
		})
	}
}
