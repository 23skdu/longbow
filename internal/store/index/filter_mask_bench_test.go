package index

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// filterProbeRounds is the number of passes timed inside one benchmark
// iteration. The fastest pass is the one reported, which is the only statistic
// that stays meaningful on a machine shared with other workloads; the framework
// mean is still reported as ns/op.
const filterProbeRounds = 9

// probeStream builds a candidate stream of ids drawn from [0, universe) in a
// deterministic, non-sequential order, which is what a graph traversal hands the
// filter: candidates arrive in distance order, so consecutive probes are
// scattered across the id space.
func probeStream(universe, probes int, seed int64) []uint32 {
	r := rand.New(rand.NewSource(seed)) // #nosec G404
	ids := make([]uint32, probes)
	for i := range ids {
		ids[i] = uint32(r.Intn(universe)) // #nosec G115
	}
	return ids
}

func selectiveFilter(universe, pct int, seed int64) *roaring.Bitmap {
	bm := roaring.New()
	r := rand.New(rand.NewSource(seed)) // #nosec G404
	for i := 0; i < universe*pct/100; i++ {
		bm.Add(uint32(r.Intn(universe))) // #nosec G115
	}
	return bm
}

// filterProbeSink keeps the probe results observable so the loop is not elided.
var filterProbeSink int

// bestPass times rounds passes over ids and returns the fastest one in
// nanoseconds. min, not mean: an interfering thread can only ever make a pass
// slower.
func bestPass(ids []uint32, probe func(uint32) bool) (float64, int) {
	best := math.MaxFloat64
	acc := 0
	for round := 0; round < filterProbeRounds; round++ {
		start := time.Now()
		hits := 0
		for _, id := range ids {
			if probe(id) {
				hits++
			}
		}
		el := float64(time.Since(start).Nanoseconds())
		acc += hits
		if el < best {
			best = el
		}
	}
	return best, acc
}

// bestPassTable is bestPass over a precomputed match table: the same loop, the
// same branch and the same hit rate, with the filter probe removed. The
// difference between the two is the cost of the probe itself.
func bestPassTable(ids []uint32, hits []bool) (float64, int) {
	best := math.MaxFloat64
	acc := 0
	for round := 0; round < filterProbeRounds; round++ {
		start := time.Now()
		sum := 0
		for i := range ids {
			if hits[i] {
				sum++
			}
		}
		el := float64(time.Since(start).Nanoseconds())
		acc += sum
		if el < best {
			best = el
		}
	}
	return best, acc
}

// benchmarkFilterProbe measures the per-candidate membership cost of one probe
// representation over a candidate stream, through the same filterMask.allows
// call site the traversal uses.
//
// Selectivity decides how roaring stores the filter, and therefore how much the
// representation matters. Below ~4096 values per 65536-id block roaring keeps an
// array container and every probe pays a binary search through the array; above
// it roaring promotes to a bitmap container whose probe is already a single word
// test and there is little left to win.
//
// delta-ns/probe subtracts the cost of the identical loop over a precomputed
// match table, so it isolates the probe itself from the loop and the branch.
func benchmarkFilterProbe(b *testing.B, universe, probes, pct int, dense bool) {
	bm := selectiveFilter(universe, pct, int64(universe+pct))
	ids := probeStream(universe, probes, int64(universe*7+pct))

	var mask *filterMask
	withFilterMask(!dense, func() {
		mask, _ = buildFilterMask(bm, nil)
	})
	if mask == nil {
		b.Fatal("no filter mask built")
	}
	if mask.densified() != dense {
		b.Fatalf("expected densified=%v, got %v", dense, mask.densified())
	}

	probe := mask.allows
	hits := make([]bool, probes)
	admitted := 0
	for i, id := range ids {
		hits[i] = probe(id)
		if hits[i] {
			admitted++
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		got, acc := bestPass(ids, probe)
		flat, _ := bestPassTable(ids, hits)
		filterProbeSink += acc
		perProbe := got / float64(probes)
		b.ReportMetric(perProbe, "ns/probe")
		b.ReportMetric(flat/float64(probes), "floor-ns/probe")
		b.ReportMetric(perProbe-flat/float64(probes), "delta-ns/probe")
	}
	b.StopTimer()
	b.ReportMetric(float64(admitted)/float64(probes), "hit-rate")
	b.ReportMetric(float64(bm.Stats().Containers), "containers")
	b.ReportMetric(float64(bm.Stats().ArrayContainers), "array-containers")
}

func filterProbeShapes(b *testing.B, dense bool) {
	for _, universe := range []int{1_000, 10_000, 200_000} {
		for _, pct := range []int{1, 10, 50, 90} {
			name := fmt.Sprintf("ids=%d/selectivity=%d%%", universe, pct)
			b.Run(name, func(b *testing.B) { benchmarkFilterProbe(b, universe, 1<<15, pct, dense) })
		}
	}
}

func BenchmarkFilterProbe_Roaring(b *testing.B) { filterProbeShapes(b, false) }
func BenchmarkFilterProbe_Dense(b *testing.B)   { filterProbeShapes(b, true) }

// BenchmarkFilterConversion reports the one-time cost of densifying a filter,
// which the end-to-end numbers have to amortise. Compare with
// BenchmarkFilterProbe: the conversion is a single pass over the filter, the
// probes are one per candidate in a traversal.
func BenchmarkFilterConversion(b *testing.B) {
	for _, universe := range []int{1_000, 10_000, 50_000, 200_000, 1_000_000} {
		for _, pct := range []int{1, 10, 50, 90} {
			b.Run(fmt.Sprintf("ids=%d/selectivity=%d%%", universe, pct), func(b *testing.B) {
				bm := selectiveFilter(universe, pct, int64(universe+pct))
				scratch := make(types.BitVector, 0, (universe+63)/64)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					mask, _ := buildFilterMask(bm, scratch)
					if mask == nil {
						b.Fatal("no mask")
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds()), "total-ns")
				b.ReportMetric(float64((universe+63)/64*8), "dense-bytes")
			})
		}
	}
}

// --- end-to-end --------------------------------------------------------------

type filteredBenchIndex struct {
	idx   *ArrowHNSW
	query []float32
}

func buildFilteredBenchIndex(tb testing.TB, n, dims int, seed int64) *filteredBenchIndex {
	tb.Helper()
	mem := memory.NewGoAllocator()
	r := rand.New(rand.NewSource(seed)) // #nosec G404

	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		vecs[i] = v
	}
	rec := MakeBatchTestRecord(mem, dims, vecs)
	rec.Retain()

	ds := NewMockDataset("filter_bench", rec.Schema())
	ds.Records = append(ds.Records, rec)

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat32
	cfg.Dims = dims
	cfg.M = 16
	cfg.EfConstruction = 100
	idx := NewArrowHNSW(ds, &cfg, nil)

	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for i := range rowIdxs {
		rowIdxs[i] = i
	}
	if _, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs); err != nil {
		tb.Fatal(err)
	}

	q := make([]float32, dims)
	for j := range q {
		q[j] = float32(r.NormFloat64())
	}

	tb.Cleanup(func() { rec.Release() })
	return &filteredBenchIndex{idx: idx, query: q}
}

// benchmarkHNSWFiltered runs k-NN searches with a filter over n% of the ids. The
// mask is rebuilt inside the search on every iteration, exactly as it is in
// production, so the measured time includes the conversion and shows where that
// conversion amortises. best-ns/search is the fastest of filterProbeRounds
// searches, which is the number to compare across variants.
func benchmarkHNSWFiltered(b *testing.B, n, dims, pct, k int, dense bool) {
	bi := buildFilteredBenchIndex(b, n, dims, 1)
	filter := selectiveFilter(n, pct, int64(pct+3))

	run := func() {
		best := math.MaxFloat64
		hits := 0
		for round := 0; round < filterProbeRounds; round++ {
			start := time.Now()
			res, err := bi.idx.SearchVectorsWithBitmap(context.Background(), bi.query, k, filter, nil)
			if err != nil {
				b.Fatal(err)
			}
			el := float64(time.Since(start).Nanoseconds())
			hits += len(res)
			if el < best {
				best = el
			}
		}
		filterProbeSink += hits
		b.ReportMetric(best, "best-ns/search")
	}

	// Warm up: the pooled search context recycles its dense buffer, so the
	// steady state is a memclr of the bitmask rather than a fresh allocation.
	withFilterMask(!dense, run)

	b.ReportAllocs()
	b.ResetTimer()
	withFilterMask(!dense, func() {
		for b.Loop() {
			run()
		}
	})
	b.StopTimer()
}

func benchmarkHNSWFilteredSuite(b *testing.B, n, dims int, sels []int) {
	for _, pct := range sels {
		b.Run(fmt.Sprintf("n=%d/selectivity=%d%%", n, pct), func(b *testing.B) {
			benchmarkHNSWFiltered(b, n, dims, pct, 10, true)
		})
	}
}

func BenchmarkHNSW_Filtered_Dense(b *testing.B) {
	benchmarkHNSWFilteredSuite(b, 10_000, 64, []int{1, 10, 50, 90})
	benchmarkHNSWFilteredSuite(b, 50_000, 64, []int{1, 10, 50, 90})
}

func BenchmarkHNSW_Filtered_Roaring(b *testing.B) {
	benchmarkHNSWFilteredSuite(b, 10_000, 64, []int{1, 10, 50, 90})
	benchmarkHNSWFilteredSuite(b, 50_000, 64, []int{1, 10, 50, 90})
}

// BenchmarkHNSW_Filtered_AB is the drift-free form of the pair above. The two
// probe representations are measured alternately against the same index, in the
// same process, so machine state (frequency, other tenants, page placement)
// moves both numbers together instead of separating them the way it does when
// two separate benchmark functions are compared across runs.
//
// Compare dense-ns/search against roaring-ns/search, and the ratio against
// dense-ns/probe-saved from BenchmarkFilterProbe to see how much of the
// per-probe saving survives into the search.
func BenchmarkHNSW_Filtered_AB(b *testing.B) {
	for _, n := range []int{10_000, 50_000} {
		for _, pct := range []int{1, 10, 50, 90} {
			b.Run(fmt.Sprintf("n=%d/selectivity=%d%%", n, pct), func(b *testing.B) {
				bi := buildFilteredBenchIndex(b, n, 64, 1)
				filter := selectiveFilter(n, pct, int64(pct+3))

				best := [2]float64{math.MaxFloat64, math.MaxFloat64}
				round := func(dense bool) {
					withFilterMask(!dense, func() {
						el := math.MaxFloat64
						hits := 0
						for r := 0; r < filterProbeRounds; r++ {
							start := time.Now()
							res, err := bi.idx.SearchVectorsWithBitmap(context.Background(), bi.query, 10, filter, nil)
							if err != nil {
								b.Fatal(err)
							}
							if ns := float64(time.Since(start).Nanoseconds()); ns < el {
								el = ns
							}
							hits += len(res)
						}
						filterProbeSink += hits
						if el < best[map[bool]int{true: 0, false: 1}[dense]] {
							best[map[bool]int{true: 0, false: 1}[dense]] = el
						}
					})
				}

				round(true)
				round(false)

				b.ResetTimer()
				for b.Loop() {
					round(true)
					round(false)
				}
				b.StopTimer()
				b.ReportMetric(best[0], "dense-ns/search")
				b.ReportMetric(best[1], "roaring-ns/search")
				b.ReportMetric(best[1]/best[0], "speedup")
			})
		}
	}
}

// BenchmarkHNSW_Filtered_SparseFilter exercises the fallback end to end: a
// three-id filter over the same index keeps the roaring probe, so this number is
// also the cost of an unselective filter before and after the change.
func BenchmarkHNSW_Filtered_SparseFilter(b *testing.B) {
	bi := buildFilteredBenchIndex(b, 10_000, 64, 1)
	bm := roaring.New()
	bm.Add(0)
	bm.Add(5_000)
	bm.Add(9_999)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := bi.idx.SearchVectorsWithBitmap(context.Background(), bi.query, 10, bm, nil); err != nil {
			b.Fatal(err)
		}
	}
}
