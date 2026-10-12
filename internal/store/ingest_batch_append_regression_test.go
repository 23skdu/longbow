package store

import (
	"fmt"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// makeUniqueStringRecord builds a record with one distinct string per row, used
// to push a column past the categorical dictionary's useful range.
func makeUniqueStringRecord(t *testing.T, rows int) arrow.RecordBatch {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bld := array.NewRecordBuilder(arrowmemory.NewGoAllocator(), schema)
	defer bld.Release()

	strBld := bld.Field(0).(*array.StringBuilder)
	listBld := bld.Field(1).(*array.FixedSizeListBuilder)
	valBld := listBld.ValueBuilder().(*array.Float32Builder)
	for i := range rows {
		strBld.Append(fmt.Sprintf("value-%d", i))
		listBld.Append(true)
		for range 4 {
			valBld.Append(0)
		}
	}

	return bld.NewRecordBatch()
}

// TestLockFreeSliceAppendIsAmortised is the guard against the O(N^2) batch
// append that made ingestion latency grow super-linearly with dataset size.
//
// The old ingestion path rebuilt the whole record slice per batch
// (`make(len+1)` + `copy`), so a dataset of N batches paid an O(N) allocation
// and copy on every one of them. Append must instead reallocate only
// logarithmically often, which is what the capacity-doubling in Append buys.
//
// The assertion is on the *number of backing arrays* the slice ever created,
// not on wall-clock: that is the quantity that turned quadratic, and counting
// it makes the test deterministic.
func TestLockFreeSliceAppendIsAmortised(t *testing.T) {
	const n = 8192

	var backingArrays int
	l := NewLockFreeSlice[int]()

	var prev []int
	for i := range n {
		l.Append(i)
		cur := l.Read()

		// A new backing array is only allocated when the previous one is full.
		if len(prev) == 0 || cap(cur) != cap(prev) {
			backingArrays++
			prev = cur
		}
	}

	require.Equal(t, n, len(l.Read()), "append must publish every element")
	require.Less(t, backingArrays, 64,
		"amortised append must reallocate logarithmically often, saw %d backing arrays for %d appends",
		backingArrays, n)
}

// TestLockFreeSliceAppendMatchesUpdateInPlace pins Append's observable result to
// the copy-on-write shape it replaced, so a caller cannot tell the two apart
// except by allocation behaviour.
func TestLockFreeSliceAppendMatchesUpdateInPlace(t *testing.T) {
	appended := NewLockFreeSlice[string]()
	reference := NewLockFreeSlice[string]()

	want := make([]string, 0, 512)
	for i := range 512 {
		s := string(rune('a' + i%26))
		appended.Append(s)

		want = append(want, s)
		reference.UpdateInPlace(append([]string(nil), want...))
	}

	require.Equal(t, reference.Read(), appended.Read())
}

// TestLockFreeSliceAppendConcurrentReadersAreStable covers the safety argument
// Append relies on: it may write into spare capacity of the backing array, but
// only past the *published* length, so a reader that already loaded the old
// header can never observe a partially written element.
//
// Run with -race.
func TestLockFreeSliceAppendConcurrentReadersAreStable(t *testing.T) {
	const (
		writerCount = 4
		perWriter   = 500
	)
	total := writerCount * perWriter

	l := NewLockFreeSlice[int]()

	writersDone := make(chan struct{})
	var writers sync.WaitGroup
	for w := range writerCount {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := range perWriter {
				// Values start at 1 so that a zero in a snapshot is
				// unambiguously a slot that was published before it was
				// written.
				l.Append(w*perWriter + i + 1)
			}
		}(w)
	}

	// Readers only check the property Append's in-place write must preserve:
	// every position below the published length holds a fully written element.
	// Concurrent appenders commit in an arbitrary order - the append is
	// serialised by writeMu, not ordered - so ordering is not the invariant,
	// but "a value that was never written" (zero) or "a value written twice"
	// would both be.
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := l.Read()
				if len(snap) > total {
					t.Errorf("reader %d observed %d elements, more than were written", r, len(snap))
					return
				}
				local := make(map[int]struct{}, len(snap))
				for _, v := range snap {
					if v == 0 {
						t.Errorf("reader %d observed an unwritten slot in %v", r, snap)
						return
					}
					if _, dup := local[v]; dup {
						t.Errorf("reader %d observed element %d twice in %v", r, v, snap)
						return
					}
					local[v] = struct{}{}
				}
			}
		}()
	}

	go func() {
		writers.Wait()
		close(writersDone)
	}()

	<-writersDone
	close(stop)
	readers.Wait()

	final := l.Read()
	require.Len(t, final, total)

	seen := make(map[int]bool, total)
	for _, v := range final {
		require.False(t, seen[v], "append must not duplicate element %d", v)
		seen[v] = true
	}
}

// TestDatasetRowCounterTracksRecords pins the O(1) row counter to the batch
// list it replaces. GetRowCount used to be an O(N) sum over every batch, which
// is why the ingest path needed an O(N) loop of its own to derive the global
// row offset of the incoming batch.
func TestDatasetRowCounterTracksRecords(t *testing.T) {
	ds := NewDataset("counter", arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.PrimitiveTypes.Float32},
	}, nil))

	require.Equal(t, int64(0), ds.GetRowCount(), "a fresh dataset has no rows")

	var recs []arrow.RecordBatch
	var want int64
	for batch := range 5 {
		rec := makeCountingRecord(t, int64(10*(batch+1)))
		rec.Retain()
		idx := ds.appendRecordLocked(rec, -1)
		recs = append(recs, rec)
		want += rec.NumRows()

		require.Equal(t, batch, idx, "append must return the sequential batch index")
		require.Equal(t, want, ds.GetRowCount())
		require.Equal(t, len(recs), len(ds.Records.Read()))
		require.Equal(t, len(recs), len(ds.BatchNodes.Read()), "batch and NUMA lists must stay aligned")
	}

	// replaceRecords is the O(N) counterpart used by compaction and eviction.
	survivors := recs[:2]
	ds.replaceRecords(survivors)
	require.Equal(t, survivors[0].NumRows()+survivors[1].NumRows(), ds.GetRowCount(),
		"replaceRecords must recompute the counter")
}

// TestCategoricalColumnRetiresAboveCardinality guards the ingest-time string
// dictionary against the memory leak that made it retain a uint16 per row per
// string column for the dataset's lifetime.
//
// Above 65535 distinct values the dictionary saturates and silently encodes
// everything as code 0, so the array never helped a filter - it was only ever
// a leak. Such a column must be retired, and must stay retired.
func TestCategoricalColumnRetiresAboveCardinality(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high-cardinality test in short mode")
	}

	ds := NewDataset("cardinality", arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil))

	// One value per row: 40k distinct values stays inside the dictionary.
	small := makeUniqueStringRecord(t, 40_000)
	small.Retain()
	defer small.Release()

	batchIdx := ds.appendRecordLocked(small, -1)
	ds.indexCategoricalColumns(small, batchIdx)

	codes, dict := ds.GetStringCodes("category", batchIdx)
	require.NotNil(t, codes, "a low-cardinality column must still be dictionary encoded")
	require.NotNil(t, dict)
	require.Len(t, codes, int(small.NumRows()))
	distinct := make(map[uint16]struct{}, len(codes))
	for _, c := range codes {
		distinct[c] = struct{}{}
	}
	require.Greater(t, len(distinct), 1, "distinct values must get distinct codes")

	// Above the cap the column is dropped and its retained codes released.
	wide := makeUniqueStringRecord(t, 70_000)
	wide.Retain()
	defer wide.Release()

	wideIdx := ds.appendRecordLocked(wide, -1)
	ds.indexCategoricalColumns(wide, wideIdx)

	codes, dict = ds.GetStringCodes("category", wideIdx)
	require.Nil(t, codes, "a saturated column must not retain per-row codes")
	require.Nil(t, dict)
	require.True(t, ds.isHighCardinality("category"), "the column must stay retired")

	// Retirement is sticky: a later small batch must not resurrect the codes.
	ds.indexCategoricalColumns(small, wideIdx)
	codes, _ = ds.GetStringCodes("category", wideIdx)
	require.Nil(t, codes, "a retired column must not come back")
}
