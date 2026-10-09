package sharding

import (
	"container/heap"
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

// StreamAggregator consolidates results from multiple Flight streams
type StreamAggregator struct {
	mem    memory.Allocator
	logger zerolog.Logger
}

// NewStreamAggregator creates a new aggregator
//
//nolint:gocritic // Logger passed by value for constructor simplicity
func NewStreamAggregator(mem memory.Allocator, logger zerolog.Logger) *StreamAggregator {
	return &StreamAggregator{
		mem:    mem,
		logger: logger,
	}
}

// Aggregate performs a scatter-gather-merge for Flight streams.
// It executes the scatter function, collects all resulting streams, reads them into memory,
// merges (sorts) the results, and returns the top K rows as a new RecordBatch.
//
// Fault Tolerance: Failed shards are logged and ignored. Partial results are returned.
func (sa *StreamAggregator) Aggregate(ctx context.Context, sg *ScatterGather, k int, scatterFn ScatterFn) ([]arrow.RecordBatch, error) {
	// 1. Scatter
	results, err := sg.Scatter(ctx, scatterFn)
	if err != nil {
		// If Scatter completely fails (e.g. no nodes), we return error
		return nil, fmt.Errorf("scatter failed: %w", err)
	}

	var batches []arrow.RecordBatch
	var mu sync.Mutex

	// 2. Gather
	var wg sync.WaitGroup
	for _, res := range results {
		if res.Error != nil {
			sa.logger.Warn().
				Str("node", res.NodeID).
				Err(res.Error).
				Msg("Shard failed during scatter")
			continue
		}

		if res.Data == nil {
			continue
		}

		stream, ok := res.Data.(flight.FlightService_DoGetClient)
		if !ok {
			sa.logger.Error().
				Str("node", res.NodeID).
				Str("type", fmt.Sprintf("%T", res.Data)).
				Msg("Unexpected result type from scatter")
			continue
		}

		wg.Add(1)
		go func(nodeID string, s flight.FlightService_DoGetClient) {
			defer wg.Done()
			reader, err := flight.NewRecordReader(s)
			if err != nil {
				sa.logger.Warn().
					Str("node", nodeID).
					Err(err).
					Msg("Failed to create reader for shard")
				return
			}
			defer reader.Release()

			// Read all batches from this shard
			for reader.Next() {
				rec := reader.RecordBatch()
				rec.Retain() // We take ownership

				mu.Lock()
				batches = append(batches, rec)
				mu.Unlock()
			}

			if reader.Err() != nil {
				sa.logger.Warn().
					Str("node", nodeID).
					Err(reader.Err()).
					Msg("Error reading stream from shard")
			}
		}(res.NodeID, stream)
	}
	wg.Wait()

	if len(batches) == 0 {
		return nil, nil // No results found
	}

	// 3. Merge & Sort via streaming heap
	return sa.mergeAndSort(batches, k)
}

type streamHeapItem struct {
	batchIdx int
	rowIdx   int
	score    float64
}

// streamTournamentHeap implements heap.Interface for M-way merging pre-sorted streams.
type streamTournamentHeap struct {
	items     []streamHeapItem
	ascending bool
}

func (h streamTournamentHeap) Len() int { return len(h.items) }
func (h streamTournamentHeap) Less(i, j int) bool {
	if h.ascending {
		return h.items[i].score < h.items[j].score
	}
	return h.items[i].score > h.items[j].score
}
func (h streamTournamentHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *streamTournamentHeap) Push(x any) {
	h.items = append(h.items, x.(streamHeapItem))
}
func (h *streamTournamentHeap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}

// boundedHeap implements heap.Interface for maintaining the top-K items.
// When ascending (smallest items wanted), the root holds the maximum of the top-K so larger items can be rejected.
// When descending (largest items wanted), the root holds the minimum of the top-K so smaller items can be rejected.
type boundedHeap struct {
	items     []streamHeapItem
	ascending bool
}

func (h boundedHeap) Len() int { return len(h.items) }
func (h boundedHeap) Less(i, j int) bool {
	if h.ascending {
		return h.items[i].score > h.items[j].score
	}
	return h.items[i].score < h.items[j].score
}
func (h boundedHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *boundedHeap) Push(x any) {
	h.items = append(h.items, x.(streamHeapItem))
}
func (h *boundedHeap) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}

func extractScore(col arrow.Array, rowIdx int) (float64, error) {
	switch arr := col.(type) {
	case *array.Float32:
		return float64(arr.Value(rowIdx)), nil
	case *array.Float64:
		return arr.Value(rowIdx), nil
	case *array.Int32:
		return float64(arr.Value(rowIdx)), nil
	case *array.Int64:
		return float64(arr.Value(rowIdx)), nil
	default:
		return 0, fmt.Errorf("unsupported score column type: %T", col)
	}
}

func isBatchSorted(col arrow.Array, ascending bool) bool {
	n := col.Len()
	if n <= 1 {
		return true
	}
	switch arr := col.(type) {
	case *array.Float32:
		vals := arr.Float32Values()
		if ascending {
			for i := 1; i < n; i++ {
				if vals[i] < vals[i-1] {
					return false
				}
			}
		} else {
			for i := 1; i < n; i++ {
				if vals[i] > vals[i-1] {
					return false
				}
			}
		}
		return true
	case *array.Float64:
		vals := arr.Float64Values()
		if ascending {
			for i := 1; i < n; i++ {
				if vals[i] < vals[i-1] {
					return false
				}
			}
		} else {
			for i := 1; i < n; i++ {
				if vals[i] > vals[i-1] {
					return false
				}
			}
		}
		return true
	default:
		prev, _ := extractScore(col, 0)
		for i := 1; i < n; i++ {
			curr, _ := extractScore(col, i)
			if ascending && curr < prev {
				return false
			}
			if !ascending && curr > prev {
				return false
			}
			prev = curr
		}
		return true
	}
}

func (sa *StreamAggregator) buildResultBatch(schema *arrow.Schema, getCol func(bIdx, cIdx int) arrow.Array, items []streamHeapItem) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(sa.mem, schema)
	defer b.Release()

	numFields := len(schema.Fields())
	for _, it := range items {
		for colI := 0; colI < numFields; colI++ {
			srcArr := getCol(it.batchIdx, colI)
			bldr := b.Field(colI)
			if err := appendValue(bldr, srcArr, it.rowIdx); err != nil {
				return nil, err
			}
		}
	}
	return b.NewRecordBatch(), nil
}

// mergeAndSort consolidates batches using an M-way streaming tournament heap or bounded top-K heap.
func (sa *StreamAggregator) mergeAndSort(inputs []arrow.RecordBatch, k int) ([]arrow.RecordBatch, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	defer func() {
		for _, b := range inputs {
			b.Release()
		}
	}()

	if len(inputs) == 1 && int(inputs[0].NumRows()) <= k {
		inputs[0].Retain()
		return []arrow.RecordBatch{inputs[0]}, nil
	}

	schema := inputs[0].Schema()
	scoreIdx := schema.FieldIndices("score")
	ascending := false
	if len(scoreIdx) == 0 {
		scoreIdx = schema.FieldIndices("distance")
		if len(scoreIdx) == 0 {
			tbl := array.NewTableFromRecords(schema, inputs)
			defer tbl.Release()
			return sa.sliceTable(tbl, k)
		}
		ascending = true
	}
	colIdx := scoreIdx[0]

	// Determine if all input batches are individually sorted
	allSorted := true
	for _, b := range inputs {
		if b.NumRows() > 1 && !isBatchSorted(b.Column(colIdx), ascending) {
			allSorted = false
			break
		}
	}

	if allSorted {
		// M-way tournament heap: O(K log M) time and O(M) memory
		h := &streamTournamentHeap{ascending: ascending}
		heap.Init(h)
		for bIdx, b := range inputs {
			if b.NumRows() > 0 {
				sc, err := extractScore(b.Column(colIdx), 0)
				if err != nil {
					return nil, err
				}
				heap.Push(h, streamHeapItem{batchIdx: bIdx, rowIdx: 0, score: sc})
			}
		}

		selected := make([]streamHeapItem, 0, k)
		for len(selected) < k && h.Len() > 0 {
			top := heap.Pop(h).(streamHeapItem)
			selected = append(selected, top)
			nextRow := top.rowIdx + 1
			if int64(nextRow) < inputs[top.batchIdx].NumRows() {
				sc, err := extractScore(inputs[top.batchIdx].Column(colIdx), nextRow)
				if err != nil {
					return nil, err
				}
				heap.Push(h, streamHeapItem{batchIdx: top.batchIdx, rowIdx: nextRow, score: sc})
			}
		}

		res, err := sa.buildResultBatch(schema, func(bIdx, cIdx int) arrow.Array {
			return inputs[bIdx].Column(cIdx)
		}, selected)
		if err != nil {
			return nil, err
		}
		return []arrow.RecordBatch{res}, nil
	}

	// Fallback to bounded top-K heap: O(N log K) time and O(K) memory
	bh := &boundedHeap{ascending: ascending}
	heap.Init(bh)
	for bIdx, b := range inputs {
		col := b.Column(colIdx)
		numRows := int(b.NumRows())
		for r := 0; r < numRows; r++ {
			sc, err := extractScore(col, r)
			if err != nil {
				return nil, err
			}
			if bh.Len() < k {
				heap.Push(bh, streamHeapItem{batchIdx: bIdx, rowIdx: r, score: sc})
			} else if ascending {
				if sc < bh.items[0].score {
					bh.items[0] = streamHeapItem{batchIdx: bIdx, rowIdx: r, score: sc}
					heap.Fix(bh, 0)
				}
			} else {
				if sc > bh.items[0].score {
					bh.items[0] = streamHeapItem{batchIdx: bIdx, rowIdx: r, score: sc}
					heap.Fix(bh, 0)
				}
			}
		}
	}

	selected := make([]streamHeapItem, bh.Len())
	for i := len(selected) - 1; i >= 0; i-- {
		selected[i] = heap.Pop(bh).(streamHeapItem)
	}

	res, err := sa.buildResultBatch(schema, func(bIdx, cIdx int) arrow.Array {
		return inputs[bIdx].Column(cIdx)
	}, selected)
	if err != nil {
		return nil, err
	}
	return []arrow.RecordBatch{res}, nil
}

func (sa *StreamAggregator) sortAndSlice(tbl arrow.Table, colIdx, k int, ascending bool) ([]arrow.RecordBatch, error) {
	numRows := int(tbl.NumRows())
	if numRows == 0 || k <= 0 {
		return nil, nil
	}

	scoreCol := tbl.Column(colIdx)
	bh := &boundedHeap{ascending: ascending}
	heap.Init(bh)

	chunks := scoreCol.Data().Chunks()
	for cIdx, chunk := range chunks {
		chunkLen := chunk.Len()
		for r := 0; r < chunkLen; r++ {
			sc, err := extractScore(chunk, r)
			if err != nil {
				return nil, err
			}
			if bh.Len() < k {
				heap.Push(bh, streamHeapItem{batchIdx: cIdx, rowIdx: r, score: sc})
			} else if ascending {
				if sc < bh.items[0].score {
					bh.items[0] = streamHeapItem{batchIdx: cIdx, rowIdx: r, score: sc}
					heap.Fix(bh, 0)
				}
			} else {
				if sc > bh.items[0].score {
					bh.items[0] = streamHeapItem{batchIdx: cIdx, rowIdx: r, score: sc}
					heap.Fix(bh, 0)
				}
			}
		}
	}

	selected := make([]streamHeapItem, bh.Len())
	for i := len(selected) - 1; i >= 0; i-- {
		selected[i] = heap.Pop(bh).(streamHeapItem)
	}

	res, err := sa.buildResultBatch(tbl.Schema(), func(cIdx, colI int) arrow.Array {
		return tbl.Column(colI).Data().Chunk(cIdx)
	}, selected)
	if err != nil {
		return nil, err
	}
	return []arrow.RecordBatch{res}, nil
}

// sliceTable returns the first k rows from the table, correctly traversing chunk boundaries.
func (sa *StreamAggregator) sliceTable(tbl arrow.Table, k int) ([]arrow.RecordBatch, error) {
	if tbl.NumRows() == 0 {
		return nil, nil
	}
	limited := tbl.NumRows()
	if int64(k) < limited {
		limited = int64(k)
	}

	tr := array.NewTableReader(tbl, limited)
	defer tr.Release()

	var batches []arrow.RecordBatch
	var collected int64

	for tr.Next() {
		rec := tr.RecordBatch()
		toTake := rec.NumRows()

		if collected+toTake > limited {
			takeRows := limited - collected
			schema := rec.Schema()
			slicedCols := make([]arrow.Array, rec.NumCols())
			for i := 0; i < int(rec.NumCols()); i++ {
				slicedCols[i] = array.NewSlice(rec.Column(i), 0, takeRows)
			}
			slicedRec := array.NewRecord(schema, slicedCols, takeRows)
			for _, c := range slicedCols {
				c.Release() // slicedRec takes ownership
			}
			batches = append(batches, slicedRec)
			break
		}

		rec.Retain()
		batches = append(batches, rec)
		collected += toTake

		if collected == limited {
			break
		}
	}

	if tr.Err() != nil {
		for _, b := range batches {
			b.Release()
		}
		return nil, tr.Err()
	}

	return batches, nil
}

// appendValue is a helper to append a single value from srcArr[idx] to builder
func appendValue(b array.Builder, src arrow.Array, idx int) error {
	if src.IsNull(idx) {
		b.AppendNull()
		return nil
	}
	switch vb := b.(type) {
	case *array.Int8Builder:
		arr := src.(*array.Int8)
		vb.Append(arr.Value(idx))
	case *array.Uint8Builder:
		arr := src.(*array.Uint8)
		vb.Append(arr.Value(idx))
	case *array.Int16Builder:
		arr := src.(*array.Int16)
		vb.Append(arr.Value(idx))
	case *array.Uint16Builder:
		arr := src.(*array.Uint16)
		vb.Append(arr.Value(idx))
	case *array.Int32Builder:
		arr := src.(*array.Int32)
		vb.Append(arr.Value(idx))
	case *array.Uint32Builder:
		arr := src.(*array.Uint32)
		vb.Append(arr.Value(idx))
	case *array.Int64Builder:
		arr := src.(*array.Int64)
		vb.Append(arr.Value(idx))
	case *array.Uint64Builder:
		arr := src.(*array.Uint64)
		vb.Append(arr.Value(idx))
	case *array.Float32Builder:
		arr := src.(*array.Float32)
		vb.Append(arr.Value(idx))
	case *array.Float64Builder:
		arr := src.(*array.Float64)
		vb.Append(arr.Value(idx))
	case *array.StringBuilder:
		arr := src.(*array.String)
		vb.Append(arr.Value(idx))
	case *array.BinaryBuilder:
		arr := src.(*array.Binary)
		vb.Append(arr.Value(idx))
	case *array.FixedSizeBinaryBuilder:
		arr := src.(*array.FixedSizeBinary)
		vb.Append(arr.Value(idx))
	case *array.FixedSizeListBuilder:
		arr := src.(*array.FixedSizeList)
		listLen := int(arr.DataType().(*arrow.FixedSizeListType).Len())
		vb.Append(true)
		subValues := arr.ListValues()
		subBuilder := vb.ValueBuilder()
		for subIdx := idx * listLen; subIdx < (idx+1)*listLen; subIdx++ {
			if err := appendValue(subBuilder, subValues, subIdx); err != nil {
				return err
			}
		}
	default:
		if fsb, ok := src.(*array.FixedSizeBinary); ok {
			if fsbB, ok := b.(*array.FixedSizeBinaryBuilder); ok {
				fsbB.Append(fsb.Value(idx))
				return nil
			}
		}
		return fmt.Errorf("unsupported type for merge: %T", b)
	}
	return nil
}
