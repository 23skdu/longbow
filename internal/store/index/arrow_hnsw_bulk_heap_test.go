package index

// Roadmap Item 33: heap traffic of a bulk ingest.
//
// The item's criterion is heap bytes during a large ingest, so the benchmark
// measures that and nothing else: it ingests one corpus through the bulk
// linker in the flush sizes the store uses, and reports the allocations the
// linker makes. No searches, no queries, no server.
//
// Run it two ways. -benchmem gives exact B/op and allocs/op, which is the
// number to gate on. -memprofile gives the site breakdown, which is the
// number to work from:
//
//	go test ./internal/store/index/ -run XXX -bench BenchmarkBulkIngestHeap \
//	  -benchtime=1x -benchmem -memprofile /tmp/bulk.pprof
//	go tool pprof -top -sample_index=alloc_space /tmp/bulk.pprof

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
)

// bulkIngestConfig is the shape production ingests at: M=16, layer-0 budget 48,
// efConstruction 200, one worker. The allocation profile is sensitive to
// EfConstruction (candidate slices are sized from it) and to MMax0 (every
// adjacency list is sized by it), so a benchmark on other values would not
// measure the thing.
func bulkIngestConfig(dims int) types.ArrowHNSWConfig {
	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat32
	cfg.Dims = dims
	cfg.M = 16
	cfg.MMax = 32
	cfg.MMax0 = 48
	cfg.EfConstruction = 200
	cfg.Workers = 1
	return cfg
}

// buildBulkIndex ingests corpus in flushes of rowsPerFlush through AddBatch,
// which is the path a store takes for every batch at or above
// BulkInsertThreshold.
func buildBulkIndex(b *testing.B, corpus [][]float32, rowsPerFlush int) *ArrowHNSW {
	b.Helper()
	rec := float32RecordBatch(b, corpus)
	ds := NewMockDataset("bench_bulk_heap", rec.Schema())
	ds.Records = append(ds.Records, rec)
	idx := NewArrowHNSW(ds, ptrConfig(bulkIngestConfig(len(corpus[0]))), nil)

	n := len(corpus)
	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for i := 0; i < n; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}
	for off := 0; off < n; off += rowsPerFlush {
		end := off + rowsPerFlush
		if end > n {
			end = n
		}
		if _, err := idx.AddBatch(context.Background(),
			[]arrow.RecordBatch{rec}, rowIdxs[off:end], batchIdxs[off:end]); err != nil {
			b.Fatalf("AddBatch at offset %d: %v", off, err)
		}
	}
	return idx
}

// ptrConfig returns a pointer to a copy of cfg, for the constructors that take
// one. The benchmark must not share a single config across iterations: the
// constructors store parts of it.
func ptrConfig(cfg types.ArrowHNSWConfig) *types.ArrowHNSWConfig {
	c := cfg
	return &c
}

// BenchmarkBulkIngestHeap is the gate for Item 33. One iteration ingests 100k
// float32 dim-128 vectors in ten 10k flushes.
//
// The corpus is built before ResetTimer, so the reported B/op is the ingest
// and not the fixture. Run it with -benchtime=1x: a second iteration would
// measure a second index in the same process, which is a different machine.
func BenchmarkBulkIngestHeap(b *testing.B) {
	const dims, n, flush = 128, 100000, 10000
	corpus := uniformCorpus(b, 20261016, n, dims)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildBulkIndex(b, corpus, flush)
	}
	b.StopTimer()
}
