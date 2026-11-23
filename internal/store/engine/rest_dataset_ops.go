package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// UpsertStreamEvents encodes JSON/struct vector events into an Arrow RecordBatch and waits for indexing.
func (s *VectorStore) UpsertStreamEvents(ctx context.Context, datasetName string, events []StreamVectorEvent) (int, error) {
	if !isValidResourceName(datasetName) {
		return 0, errors.New("invalid dataset name")
	}
	if len(events) == 0 {
		return 0, nil
	}
	for i := range events {
		if len(events[i].Vector) == 0 && len(events[i].Embedding) > 0 {
			events[i].Vector = events[i].Embedding
		}
	}
	rec, err := buildStreamArrowBatch(s.mem, events)
	if err != nil {
		return 0, err
	}
	defer rec.Release()

	if err := s.StoreRecordBatch(ctx, datasetName, rec); err != nil {
		return 0, err
	}
	s.workerMu.Lock()
	workersRunning := len(s.ingestionWorkerCancels) > 0
	s.workerMu.Unlock()
	if workersRunning {
		if ds, ok := s.getDataset(datasetName); ok {
			for ds.PendingIndexJobs.Load() > 0 || ds.PendingIngestion.Load() > 0 {
				select {
				case <-ctx.Done():
					return len(events), ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
	}
	return len(events), nil
}

// DeleteVectorByID marks a vector ID as deleted (tombstoned) in the target dataset.
func (s *VectorStore) DeleteVectorByID(_ context.Context, datasetName, rawID string) error {
	ds, ok := s.getDataset(datasetName)
	if !ok {
		return NewNotFoundError("dataset", datasetName)
	}

	// Resolve location under read lock, then apply tombstone under write lock
	// briefly to avoid holding the dataset write lock during resolution.
	ds.dataMu.RLock()
	loc, found := resolveVectorLocationLocked(ds, rawID)
	ds.dataMu.RUnlock()
	if !found {
		return NewNotFoundError("vector", rawID)
	}

	if setTombstoneIfAbsent(ds, loc.BatchIdx, loc.RowIdx) {
		metrics.TombstonesTotal.WithLabelValues(datasetName).Inc()
	}
	return nil
}

// GetVectorByID fetches a stored vector and its text/metadata fields by ID.
func (s *VectorStore) GetVectorByID(datasetName, rawID string) (map[string]any, error) {
	ds, ok := s.getDataset(datasetName)
	if !ok {
		return nil, NewNotFoundError("dataset", datasetName)
	}
	ds.dataMu.RLock()
	loc, found := resolveVectorLocationLocked(ds, rawID)
	ds.dataMu.RUnlock()
	if !found {
		return nil, NewNotFoundError("vector", rawID)
	}
	ds.TombstoneMu.RLock()
	tombstoned := false
	if ts := ds.Tombstones[loc.BatchIdx]; ts != nil && ts.Contains(loc.RowIdx) {
		tombstoned = true
	}
	ds.TombstoneMu.RUnlock()
	if tombstoned {
		return nil, NewNotFoundError("vector", rawID)
	}
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()
	batches := ds.Records.Read()
	if loc.BatchIdx < 0 || loc.BatchIdx >= len(batches) {
		return nil, NewNotFoundError("vector", rawID)
	}
	rec := batches[loc.BatchIdx]
	if loc.RowIdx < 0 || loc.RowIdx >= int(rec.NumRows()) {
		return nil, NewNotFoundError("vector", rawID)
	}

	out := map[string]any{
		"id":      rawID,
		"dataset": datasetName,
	}
	if vec, err := extractVectorFromCol(rec, loc.RowIdx); err == nil {
		out["vector"] = vec
	}
	schema := rec.Schema()
	for i, f := range schema.Fields() {
		if f.Name == "vector" || f.Name == "embedding" {
			continue
		}
		if col, ok := rec.Column(i).(*array.String); ok && !col.IsNull(loc.RowIdx) {
			val := col.Value(loc.RowIdx)
			if f.Name == "metadata" {
				var meta map[string]any
				if err := json.Unmarshal([]byte(val), &meta); err == nil {
					out["metadata"] = meta
					continue
				}
			}
			out[f.Name] = val
		}
	}
	return out, nil
}

func resolveVectorLocationLocked(ds *Dataset, rawID string) (RowLocation, bool) {
	if loc, ok := ds.PrimaryIndex[rawID]; ok {
		return loc, true
	}
	if nid, err := strconv.ParseInt(rawID, 10, 64); err == nil {
		if loc, ok := ds.NumericPrimaryIndex[nid]; ok {
			return loc, true
		}
	}
	if uid, err := strconv.ParseUint(rawID, 10, 64); err == nil {
		if loc, ok := ds.Uint64PrimaryIndex[uid]; ok {
			return loc, true
		}
	}
	if idxID, err := strconv.ParseUint(rawID, 10, 32); err == nil && ds.Index != nil {
		if rawLoc, ok := ds.Index.GetLocation(uint32(idxID)); ok {
			if l, ok := rawLoc.(Location); ok {
				return RowLocation(l), true
			}
		}
	}
	return RowLocation{}, false
}

// DatasetInfo summarizes dataset dimensions, row counts, and fields for REST/GraphQL inspection.
type DatasetInfo struct {
	Name       string   `json:"name"`
	RowCount   int64    `json:"row_count"`
	IndexCount int      `json:"index_count"`
	VectorDim  int      `json:"vector_dim"`
	Tombstones int      `json:"tombstones"`
	Fields     []string `json:"fields,omitempty"`
}

func (s *VectorStore) inspectDataset(name string) (*DatasetInfo, error) {
	ds, ok := s.getDataset(name)
	if !ok {
		return nil, NewNotFoundError("dataset", name)
	}
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	var rows int64
	var vecDim int
	var fields []string
	for _, b := range ds.Records.Read() {
		rows += b.NumRows()
		if len(fields) == 0 && b.Schema() != nil {
			for _, f := range b.Schema().Fields() {
				fields = append(fields, f.Name)
			}
		}
		if vecDim == 0 {
			if vec, err := extractVectorFromCol(b, 0); err == nil {
				vecDim = len(vec)
			}
		}
	}
	tombCount := 0
	for _, ts := range ds.GetTombstones() {
		if ts != nil {
			tombCount += int(ts.Count()) // #nosec G115
		}
	}
	idxCount := 0
	if ds.Index != nil {
		idxCount = ds.Index.Len()
	}
	return &DatasetInfo{
		Name:       name,
		RowCount:   rows,
		IndexCount: idxCount,
		VectorDim:  vecDim,
		Tombstones: tombCount,
		Fields:     fields,
	}, nil
}

func (s *VectorStore) listDatasetInfos(ctx context.Context) []DatasetInfo {
	names := s.GetActiveDatasets()
	sort.Strings(names)
	out := make([]DatasetInfo, 0, len(names))
	for _, n := range names {
		if !s.CanTenantListDataset(ctx, n) {
			continue
		}
		if info, err := s.inspectDataset(n); err == nil {
			out = append(out, *info)
		}
	}
	return out
}

// setTombstoneIfAbsent marks a row as deleted under the tombstone lock and
// reports whether the row was newly tombstoned.
func setTombstoneIfAbsent(ds *Dataset, batchIdx, rowIdx int) bool {
	ds.TombstoneMu.Lock()
	defer ds.TombstoneMu.Unlock()
	ts := ds.Tombstones[batchIdx]
	if ts != nil && ts.Contains(rowIdx) {
		return false
	}
	if ts == nil {
		ts = types.NewBitset()
		ds.Tombstones[batchIdx] = ts
	}
	ts.Set(rowIdx)
	return true
}

// setTombstone marks a row as deleted under the tombstone lock.
func setTombstone(ds *Dataset, batchIdx, rowIdx int) {
	_ = setTombstoneIfAbsent(ds, batchIdx, rowIdx)
}
