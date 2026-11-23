package engine

import (
	"context"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"golang.org/x/sync/errgroup"

	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
)

// GenerateFilterBitset pre-calculates a bitset of VectorIDs that match the filters.
func (d *Dataset) GenerateFilterBitset(filters []qry.Filter, filterExpr FilterExpr) (*types.Bitset, error) {
	// Generate hash
	var hash string
	for _, f := range filters {
		hash += qry.FilterHash(f) + ";"
	}

	d.filterMu.RLock()
	if bs, ok := d.filterCache[hash]; ok {
		d.filterMu.RUnlock()
		metrics.BitmapCacheHitsTotal.Inc()
		return bs.Clone(), nil
	}
	d.filterMu.RUnlock()
	metrics.BitmapCacheMissesTotal.Inc()

	d.dataMu.RLock()
	defer d.dataMu.RUnlock()

	return d.GenerateFilterBitsetLocked(filters, filterExpr, hash)
}

// GenerateFilterBitsetLocked is the variant that assumes d.dataMu is already held.
func (d *Dataset) GenerateFilterBitsetLocked(filters []qry.Filter, filterExpr FilterExpr, hash string) (*types.Bitset, error) {
	records := d.Records.Read()
	if len(records) == 0 || d.Index == nil {
		return nil, nil
	}

	bitset := types.NewBitset()

	// Fast path: use ColumnInvertedIndex for simple equality filters on indexed columns.
	// This avoids the full metadata scan (the primary bottleneck at 500K).
	if d.ColumnIndex != nil && filterExpr == nil && len(filters) > 0 {
		allIndexed := true
		for _, f := range filters {
			if f.Operator != "=" && f.Operator != "eq" && f.Operator != "==" {
				allIndexed = false
				break
			}
			if !d.ColumnIndex.HasIndex(f.Field) {
				allIndexed = false
				break
			}
		}
		if allIndexed {
			result := d.lookupIntersection(filters)
			for _, pos := range result {
				loc := types.Location{BatchIdx: pos.RecordIdx, RowIdx: pos.RowIdx}
				if vid, ok := d.Index.GetVectorID(loc); ok {
					bitset.Set(int(vid))
				}
			}
			goto cacheResult
		}
	}

	{
		idx := d.Index

		type batchResult struct {
			batchIdx int
			matches  []int
			err      error
		}

		numWorkers := runtime.NumCPU()
		if numWorkers > len(records) {
			numWorkers = len(records)
		}
		if numWorkers < 1 {
			numWorkers = 1
		}

		g, gctx := errgroup.WithContext(context.Background())
		g.SetLimit(numWorkers)

		results := make([]batchResult, len(records))
		for bi := range records {
			bi := bi
			g.Go(func() error {
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
				}

				rec := records[bi]
				eval, err := qry.NewFilterEvaluator(rec, filters)
				if err != nil {
					results[bi] = batchResult{batchIdx: bi, err: err}
					return err
				}

				// Apply precomputed 16-bit dictionary codes.
				for i, f := range rec.Schema().Fields() {
					if f.Type.ID() == arrow.STRING {
						if codes, dict := d.GetStringCodes(f.Name, bi); codes != nil {
							eval.SetStringColumnCodes(i, codes, dict)
						}
					}
				}

				matches, err := eval.MatchesAll(int(rec.NumRows()))
				if err != nil {
					results[bi] = batchResult{batchIdx: bi, err: err}
					return err
				}

				if filterExpr != nil {
					metaIdx := -1
					for i, field := range d.Schema.Fields() {
						if field.Name == "metadata" {
							metaIdx = i
							break
						}
					}
					if metaIdx >= 0 {
						metaCol := rec.Column(metaIdx)
						binData := array.NewBinaryData(metaCol.Data())
						validMatches := make([]int, 0, len(matches))
						for _, rowIdx := range matches {
							if binData.IsValid(rowIdx) {
								metaBytes := binData.Value(rowIdx)
								lazyMeta := types.NewLazyMetadata(metaBytes)
								if filterExpr.Evaluate(lazyMeta) {
									validMatches = append(validMatches, rowIdx)
								}
							}
						}
						binData.Release()
						matches = validMatches
					}
				}

				results[bi] = batchResult{batchIdx: bi, matches: matches}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			bitset.Release()
			return nil, err
		}

		for _, r := range results {
			if r.err != nil {
				bitset.Release()
				return nil, r.err
			}
			for _, rowIdx := range r.matches {
				loc := types.Location{BatchIdx: r.batchIdx, RowIdx: int(rowIdx)}
				if vid, ok := idx.GetVectorID(loc); ok {
					bitset.Set(int(vid))
				}
			}
		}
	}

cacheResult:

	// Cache a clone so the original can be released/modified if needed elsewhere
	// and the cached one stays safe.
	d.filterMu.Lock()
	if d.filterCache == nil {
		d.filterCache = make(map[string]*types.Bitset)
	}
	if len(d.filterCache) > 100 {
		// Evict first element (pseudo-LRU since map iteration is random)
		for k, v := range d.filterCache {
			v.Release()
			delete(d.filterCache, k)
			break
		}
	}
	d.filterCache[hash] = bitset
	d.filterMu.Unlock()

	return bitset.Clone(), nil
}

// lookupIntersection returns RowPositions matching all filters by intersecting
// ColumnInvertedIndex lookups. Caller must hold dataMu (at least RLock).
func (d *Dataset) lookupIntersection(filters []qry.Filter) []index.RowPosition {
	var result []index.RowPosition
	for i, f := range filters {
		positions := d.ColumnIndex.Lookup(f.Field, f.Value)
		if len(positions) == 0 {
			return nil
		}
		if i == 0 {
			result = make([]index.RowPosition, len(positions))
			copy(result, positions)
		} else {
			intersection := make([]index.RowPosition, 0, min(len(result), len(positions)))
			for _, rp := range result {
				for _, pp := range positions {
					if rp == pp {
						intersection = append(intersection, rp)
						break
					}
				}
			}
			result = intersection
			if len(result) == 0 {
				return nil
			}
		}
	}
	return result
}

// IndexStringColumn maps categorical strings to 16-bit integer IDs at ingest for vectorized SIMD filter evaluation.
func (d *Dataset) IndexStringColumn(colName string, batchIdx int, col *array.String) {
	if d == nil || col == nil {
		return
	}
	d.stringDictMu.Lock()
	defer d.stringDictMu.Unlock()

	if d.StringDicts == nil {
		d.StringDicts = make(map[string]*qry.StringDictionary)
	}
	if d.BatchStringCodes == nil {
		d.BatchStringCodes = make(map[string]map[int][]uint16)
	}

	dict, ok := d.StringDicts[colName]
	if !ok {
		dict = qry.NewStringDictionary()
		d.StringDicts[colName] = dict
	}

	codes := dict.EncodeArray(col)
	if d.BatchStringCodes[colName] == nil {
		d.BatchStringCodes[colName] = make(map[int][]uint16)
	}
	d.BatchStringCodes[colName][batchIdx] = codes
}

// maxCategoricalDistinct bounds how many distinct values a string column may
// have before the dataset stops building 16-bit dictionary codes for it.
//
// The codes exist purely to accelerate categorical equality filters with SIMD.
// A column with more distinct values than this is not categorical (it is free
// text or a unique key), and for those the dictionary was already useless:
// StringDictionary saturates at 65535 symbols and silently returns code 0 for
// everything past it, so a high-cardinality column produced a full-width array
// of zeros that was retained for the dataset's lifetime and never helped a
// single filter.
var maxCategoricalDistinct = 65535

// indexCategoricalColumns records 16-bit dictionary codes for every string
// column of a freshly ingested batch.
//
// A column that has ever been seen to exceed maxCategoricalDistinct in any
// batch is permanently retired: its codes are dropped and the evaluator falls
// back to comparing strings directly, which is what it already did for every
// value past the dictionary's saturation point.
func (d *Dataset) indexCategoricalColumns(rec arrow.RecordBatch, batchIdx int) {
	if d == nil || rec == nil {
		return
	}
	for i, f := range rec.Schema().Fields() {
		if f.Type.ID() != arrow.STRING {
			continue
		}
		if d.isHighCardinality(f.Name) {
			continue
		}
		strCol, ok := rec.Column(i).(*array.String)
		if !ok {
			continue
		}
		if d.countDistinctStringValues(strCol) > maxCategoricalDistinct {
			d.retireCategoricalColumn(f.Name)
			continue
		}
		d.IndexStringColumn(f.Name, batchIdx, strCol)
	}
}

// countDistinctStringValues counts the distinct non-null values in col without
// retaining them. Callers use it only to decide whether a column is
// categorical; the dictionary itself is built later, only if the answer is
// yes.
func (d *Dataset) countDistinctStringValues(col *array.String) int {
	if col == nil {
		return 0
	}
	n := col.Len()
	if n <= maxCategoricalDistinct {
		return n
	}
	seen := make(map[string]struct{}, 256)
	for i := 0; i < n; i++ {
		if !col.IsValid(i) {
			continue
		}
		seen[col.Value(i)] = struct{}{}
		if len(seen) > maxCategoricalDistinct {
			return len(seen)
		}
	}
	return len(seen)
}

// retireCategoricalColumn marks a column as non-categorical and releases the
// codes already retained for it.
func (d *Dataset) retireCategoricalColumn(colName string) {
	d.stringDictMu.Lock()
	defer d.stringDictMu.Unlock()

	if d.nonCategoricalCols == nil {
		d.nonCategoricalCols = make(map[string]bool)
	}
	d.nonCategoricalCols[colName] = true

	if d.BatchStringCodes != nil {
		delete(d.BatchStringCodes, colName)
	}
	if d.StringDicts != nil {
		delete(d.StringDicts, colName)
	}
}

func (d *Dataset) isHighCardinality(colName string) bool {
	d.stringDictMu.RLock()
	defer d.stringDictMu.RUnlock()
	return d.nonCategoricalCols[colName]
}

// GetStringCodes returns precomputed 16-bit dictionary codes for a string column in a batch.
func (d *Dataset) GetStringCodes(colName string, batchIdx int) ([]uint16, *qry.StringDictionary) {
	if d == nil {
		return nil, nil
	}
	d.stringDictMu.RLock()
	defer d.stringDictMu.RUnlock()
	if d.BatchStringCodes == nil || d.StringDicts == nil {
		return nil, nil
	}
	dict := d.StringDicts[colName]
	batchMap := d.BatchStringCodes[colName]
	if batchMap == nil {
		return nil, dict
	}
	return batchMap[batchIdx], dict
}
