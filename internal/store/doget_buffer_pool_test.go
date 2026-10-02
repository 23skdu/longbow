package store

import (
	"bytes"
	"io"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

type captureFlightStream struct {
	mu   sync.Mutex
	msgs []*flight.FlightData
}

func (c *captureFlightStream) Send(fd *flight.FlightData) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, &flight.FlightData{
		DataHeader:  append([]byte(nil), fd.DataHeader...),
		DataBody:    append([]byte(nil), fd.DataBody...),
		AppMetadata: append([]byte(nil), fd.AppMetadata...),
	})
	return nil
}

func (c *captureFlightStream) headers() [][]byte {
	out := make([][]byte, len(c.msgs))
	for i, m := range c.msgs {
		out[i] = m.DataHeader
	}
	return out
}

func (c *captureFlightStream) bodies() [][]byte {
	out := make([][]byte, len(c.msgs))
	for i, m := range c.msgs {
		out[i] = m.DataBody
	}
	return out
}

type discardFlightStream struct{}

func (discardFlightStream) Send(*flight.FlightData) error { return nil }

type flightMsgReader struct {
	msgs []*flight.FlightData
	pos  int
}

func (r *flightMsgReader) Recv() (*flight.FlightData, error) {
	if r.pos >= len(r.msgs) {
		return nil, io.EOF
	}
	msg := r.msgs[r.pos]
	r.pos++
	return msg, nil
}

func doGetTestSchema(includeVectors bool, extraCols int) *arrow.Schema {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}
	if includeVectors {
		fields = append(fields, arrow.Field{Name: "vector", Type: arrow.BinaryTypes.Binary})
	}
	for i := 0; i < extraCols; i++ {
		fields = append(fields, arrow.Field{Name: "extra", Type: arrow.PrimitiveTypes.Float64})
	}
	return arrow.NewSchema(fields, nil)
}

func doGetMeasuredRowBytes(dim int) int {
	return 16 + dim*4
}

func newDoGetTestRecord(tb testing.TB, schema *arrow.Schema, rows, dim int) arrow.RecordBatch {
	tb.Helper()
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	for i := 0; i < schema.NumFields(); i++ {
		switch schema.Field(i).Type.ID() {
		case arrow.UINT64:
			col := b.Field(i).(*array.Uint64Builder)
			for r := 0; r < rows; r++ {
				col.Append(uint64(r))
			}
		case arrow.FLOAT32:
			col := b.Field(i).(*array.Float32Builder)
			for r := 0; r < rows; r++ {
				col.Append(float32(r) + 0.25)
			}
		case arrow.FLOAT64:
			col := b.Field(i).(*array.Float64Builder)
			for r := 0; r < rows; r++ {
				col.Append(float64(r) + 0.5)
			}
		case arrow.BINARY:
			col := b.Field(i).(*array.BinaryBuilder)
			val := make([]byte, dim*4)
			for r := 0; r < rows; r++ {
				for j := range val {
					val[j] = byte(r*31 + j)
				}
				col.Append(val)
			}
		default:
			tb.Fatalf("unsupported test column type %s", schema.Field(i).Type)
		}
	}
	rec := b.NewRecordBatch()
	b.Release()
	return rec
}

func TestEstimateIPCResponseBytes_MonotonicInTopK(t *testing.T) {
	schema := doGetTestSchema(true, 2)
	prev := 0
	for _, rows := range []int{-1, 0, 1, 10, 100, 1000, 10000} {
		got := estimateIPCResponseBytes(rows, schema, 0)
		require.Greater(t, got, 0, "rows=%d", rows)
		if rows > 0 {
			require.Greater(t, got, prev, "estimate must grow with top-k: rows=%d", rows)
		} else {
			require.GreaterOrEqual(t, got, prev, "rows=%d", rows)
		}
		prev = got
	}
}

func TestEstimateIPCResponseBytes_MonotonicInSchemaWidth(t *testing.T) {
	narrow := doGetTestSchema(false, 0)
	mid := doGetTestSchema(false, 1)
	wide := doGetTestSchema(true, 4)

	for _, rows := range []int{1, 100, 1000} {
		n := estimateIPCResponseBytes(rows, narrow, 0)
		m := estimateIPCResponseBytes(rows, mid, 0)
		w := estimateIPCResponseBytes(rows, wide, 0)
		require.Greater(t, n, 0)
		require.Greater(t, m, n, "extra fixed column must raise estimate (rows=%d)", rows)
		require.Greater(t, w, m, "wider projection must raise estimate (rows=%d)", rows)
	}
}

func TestEstimateIPCResponseBytes_MeasuredAndNilSchema(t *testing.T) {
	schema := doGetTestSchema(false, 0)
	fields := len(schema.Fields())

	require.Greater(t, estimateIPCResponseBytes(10, nil, 0), 0)

	want := ipcSchemaMessageBytes + ipcRecordMessageBytes + fields*ipcFieldMetadataBytes + 100*4096
	require.Equal(t, want, estimateIPCResponseBytes(100, schema, 4096))

	require.Greater(t,
		estimateIPCResponseBytes(100, nil, 8192),
		estimateIPCResponseBytes(100, nil, 4096),
		"measured row size must be monotonic")

	require.Equal(t,
		estimateIPCResponseBytes(0, schema, 0),
		estimateIPCResponseBytes(-7, schema, 0),
		"negative row counts clamp to zero")
}

func TestEstimateIPCResponseBytes_CoversSerializedBody(t *testing.T) {
	for _, dim := range []int{3, 64, 384} {
		for _, rows := range []int{1, 128, 1024} {
			schema := doGetTestSchema(true, 0)
			rec := newDoGetTestRecord(t, schema, rows, dim)

			stream := &captureFlightStream{}
			w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
			require.NoError(t, w.Write(rec))
			require.NoError(t, w.Close())
			rec.Release()

			maxBody := 0
			for _, m := range stream.msgs {
				if len(m.DataBody) > maxBody {
					maxBody = len(m.DataBody)
				}
			}
			est := estimateIPCResponseBytes(rows, schema, doGetMeasuredRowBytes(dim))
			require.GreaterOrEqual(t, est, maxBody,
				"estimate %d must cover serialized body %d (rows=%d dim=%d)", est, maxBody, rows, dim)
		}
	}
}

func TestPooledFlightWriter_ByteIdenticalToStock(t *testing.T) {
	schema := doGetTestSchema(true, 1)
	rec := newDoGetTestRecord(t, schema, 300, 64)
	defer rec.Release()

	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())

	stock := &captureFlightStream{}
	sw := flight.NewRecordWriter(stock, ipc.WithSchema(schema))
	require.NoError(t, sw.Write(rec))
	require.NoError(t, sw.Close())

	pooled := &captureFlightStream{}
	pw := newRecordWriterWithPool(pool, pooled, schema, 300, doGetMeasuredRowBytes(64))
	require.NoError(t, pw.Write(rec))
	require.NoError(t, pw.Close())

	require.Len(t, pooled.msgs, len(stock.msgs))
	require.Equal(t, stock.headers(), pooled.headers())
	require.Equal(t, stock.bodies(), pooled.bodies())

	stockEmpty := &captureFlightStream{}
	require.NoError(t, flight.NewRecordWriter(stockEmpty, ipc.WithSchema(schema)).Close())

	pooledEmpty := &captureFlightStream{}
	require.NoError(t, newRecordWriterWithPool(pool, pooledEmpty, schema, 0, 0).Close())

	require.Equal(t, stockEmpty.headers(), pooledEmpty.headers())
	require.Equal(t, stockEmpty.bodies(), pooledEmpty.bodies())
}

func TestPooledFlightWriter_RoundTripReadable(t *testing.T) {
	schema := doGetTestSchema(true, 0)
	rec := newDoGetTestRecord(t, schema, 512, 32)
	defer rec.Release()

	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())
	stream := &captureFlightStream{}
	w := newRecordWriterWithPool(pool, stream, schema, 512, doGetMeasuredRowBytes(32))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	rdr, err := flight.NewRecordReader(&flightMsgReader{msgs: stream.msgs})
	require.NoError(t, err)
	defer rdr.Release()

	require.True(t, rdr.Schema().Equal(schema))

	total := int64(0)
	batches := 0
	for rdr.Next() {
		out := rdr.Record()
		total += out.NumRows()
		batches++
	}
	require.NoError(t, rdr.Err())
	require.Equal(t, 1, batches)
	require.Equal(t, int64(512), total)
}

// minAllocsPerRun returns the lowest allocs/op observed across several
// AllocsPerRun samples.
//
// AllocsPerRun reads process-global allocation counters, so it cannot isolate
// the measured closure: any background goroutine allocating inside the window
// (indexing workers, WAL replay, metrics collectors left running by earlier
// tests) is counted too. That noise is purely additive, and the two sides of a
// comparison can be polluted differently, which is what made this test fail
// intermittently in full-package runs. Taking the minimum of several samples
// picks the window least affected by unrelated activity, which makes the
// comparison reflect the code under test instead of the scheduler.
func minAllocsPerRun(runs int, f func()) float64 {
	const samples = 5
	best := math.MaxFloat64
	for i := 0; i < samples; i++ {
		if v := testing.AllocsPerRun(runs, f); v < best {
			best = v
		}
	}
	return best
}

func TestDoGetBufferPool_SizedGetReusesBuffer(t *testing.T) {
	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())
	schema := doGetTestSchema(true, 0)
	size := estimateIPCResponseBytes(1024, schema, doGetMeasuredRowBytes(128))
	payload := make([]byte, 500*1024)

	restoreGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(restoreGC)
	restoreProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(restoreProcs)
	runtime.GC()

	buf := pool.GetSized(size)
	buf.Write(payload)
	pool.Put(buf)

	const runs = 200
	pooledAllocs := minAllocsPerRun(runs, func() {
		b := pool.GetSized(size)
		b.Write(payload)
		pool.Put(b)
	})

	unpooledAllocs := minAllocsPerRun(runs, func() {
		b := &bytes.Buffer{}
		b.Write(payload)
	})

	stats := pool.Stats()
	t.Logf("pooled=%.2f allocs/op unpooled=%.2f allocs/op misses=%d gets=%d puts=%d",
		pooledAllocs, unpooledAllocs, stats.Misses, stats.Gets, stats.Puts)

	require.Less(t, pooledAllocs, unpooledAllocs, "pooled path must allocate less than the unpooled path")
	require.LessOrEqual(t, stats.Misses, stats.Gets/2, "at least half of the Gets must be served by a pooled buffer")
	require.Equal(t, stats.Gets, stats.Puts, "every buffer taken from the pool must be returned")
}

func TestPooledFlightWriter_FewerAllocsThanStock(t *testing.T) {
	schema := doGetTestSchema(true, 0)
	rows, dim := 256, 128
	rec := newDoGetTestRecord(t, schema, rows, dim)
	defer rec.Release()

	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())
	measured := doGetMeasuredRowBytes(dim)

	restoreGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(restoreGC)
	restoreProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(restoreProcs)
	runtime.GC()

	warm := newRecordWriterWithPool(pool, discardFlightStream{}, schema, rows, measured)
	require.NoError(t, warm.Write(rec))
	require.NoError(t, warm.Close())

	const runs = 200
	stockAllocs := minAllocsPerRun(runs, func() {
		w := flight.NewRecordWriter(discardFlightStream{}, ipc.WithSchema(schema))
		if err := w.Write(rec); err != nil {
			t.Error(err)
		}
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})

	pooledAllocs := minAllocsPerRun(runs, func() {
		w := newRecordWriterWithPool(pool, discardFlightStream{}, schema, rows, measured)
		if err := w.Write(rec); err != nil {
			t.Error(err)
		}
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})

	stats := pool.Stats()
	t.Logf("stock=%.2f allocs/op pooled=%.2f allocs/op misses=%d", stockAllocs, pooledAllocs, stats.Misses)

	require.Less(t, pooledAllocs, stockAllocs, "pooled flight writer must allocate less than flight.NewRecordWriter")
	require.LessOrEqual(t, stats.Misses, stats.Gets/2, "at least half of the Gets must be served by a pooled buffer")
}

func TestDoGetBufferPool_ConcurrentSizedWriters(t *testing.T) {
	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())
	schema := doGetTestSchema(true, 0)
	rec := newDoGetTestRecord(t, schema, 128, 32)
	defer rec.Release()

	const goroutines = 16
	const iters = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				rows := 1 + (g*iters+i)%64
				w := newRecordWriterWithPool(pool, discardFlightStream{}, schema, rows, doGetMeasuredRowBytes(32))
				if err := w.Write(rec); err != nil {
					t.Error(err)
					return
				}
				if err := w.Close(); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	stats := pool.Stats()
	require.Equal(t, int64(goroutines*iters), stats.Gets)
	require.Equal(t, int64(goroutines*iters), stats.Puts)
	require.LessOrEqual(t, stats.Misses, stats.Gets)
}

var benchmarkEstimateSink int

func BenchmarkEstimateIPCResponseBytes(b *testing.B) {
	schema := doGetTestSchema(true, 4)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkEstimateSink = estimateIPCResponseBytes(1024, schema, 0)
	}
}

func BenchmarkDoGetResponseBuffer_Pooled(b *testing.B) {
	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())
	schema := doGetTestSchema(true, 0)
	size := estimateIPCResponseBytes(1024, schema, doGetMeasuredRowBytes(128))
	payload := make([]byte, 500*1024)

	buf := pool.GetSized(size)
	buf.Write(payload)
	pool.Put(buf)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buf := pool.GetSized(size)
		buf.Write(payload)
		pool.Put(buf)
	}
}

func BenchmarkDoGetResponseBuffer_Unpooled(b *testing.B) {
	schema := doGetTestSchema(true, 0)
	size := estimateIPCResponseBytes(1024, schema, doGetMeasuredRowBytes(128))
	payload := make([]byte, 500*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buf := &bytes.Buffer{}
		if buf.Cap() < size {
			buf.Grow(size)
		}
		buf.Write(payload)
	}
}

func BenchmarkDoGetFlightWriter_Pooled(b *testing.B) {
	schema := doGetTestSchema(true, 0)
	rows, dim := 256, 128
	rec := newDoGetTestRecord(b, schema, rows, dim)
	defer rec.Release()

	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())
	measured := doGetMeasuredRowBytes(dim)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		w := newRecordWriterWithPool(pool, discardFlightStream{}, schema, rows, measured)
		if err := w.Write(rec); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDoGetFlightWriter_Unpooled(b *testing.B) {
	schema := doGetTestSchema(true, 0)
	rows, dim := 256, 128
	rec := newDoGetTestRecord(b, schema, rows, dim)
	defer rec.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		w := flight.NewRecordWriter(discardFlightStream{}, ipc.WithSchema(schema))
		if err := w.Write(rec); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
