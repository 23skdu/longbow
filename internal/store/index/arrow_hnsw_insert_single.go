package index

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"

	"github.com/23skdu/longbow/internal/store/types"
)

const commitWaitTimeout = 2 * time.Minute

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(goCtx context.Context, ctx *ArrowSearchContext, query any, entryPoint uint32, ef, layer int, data *types.GraphData) ([]types.Candidate, error) {
	computer := h.resolveHNSWComputer(data, ctx, query, true, types.SearchOptions{ForceQuantized: layer > 0})
	var res []types.Candidate
	var err error
	if compF32, ok := computer.(*float32ToFloat32Computer); ok {
		res, err = h.searchLayerFloat32(goCtx, compF32, entryPoint, ef, layer, ctx, data)
	} else if compF64, ok := computer.(*float64Computer); ok {
		res, err = h.searchLayerFloat64(goCtx, compF64, entryPoint, ef, layer, ctx, data)
	} else {
		res, err = h.searchLayer(goCtx, computer, entryPoint, ef, layer, ctx, data, query)
	}
	if err != nil {
		return nil, err
	}

	cloned := h.getCandidateSlice(len(res))
	cloned = append(cloned, res...)
	return cloned, nil
}

// selectNeighbors selects the best M neighbors using the RobustPrune heuristic.
func (h *ArrowHNSW) commitID(id uint32) {
	h.commitMu.Lock()
	defer h.commitMu.Unlock()

	for h.GetMetadataSnapshot().NodeCount < int64(id) {
		h.commitCond.Wait()
	}

	h.updateMetadata(func(meta *HNSWMetadata) {
		if meta.NodeCount == int64(id) {
			meta.NodeCount++

			// Entry Point Promotion: Only promote if this node reached a higher level than current EP
			data := h.data.Load()
			if data != nil {
				cID := int(id) / types.ChunkSize
				cOff := int(id) % types.ChunkSize
				levels := data.GetLevelsChunk(cID)
				nodeLevel := 0
				if levels != nil {
					nodeLevel = int(atomic.LoadUint32(&levels[cOff]))
				}

				epLevel := -1
				ep := meta.EntryPoint
				if ep != math.MaxUint32 {
					epCID := int(ep) / types.ChunkSize
					epCOff := int(ep) % types.ChunkSize
					epLevels := data.GetLevelsChunk(epCID)
					if epLevels != nil {
						epLevel = int(atomic.LoadUint32(&epLevels[epCOff]))
					}
				}

				if ep == math.MaxUint32 || nodeLevel > epLevel {
					meta.EntryPoint = id
					meta.MaxLevel = int32(nodeLevel)
				}
			}
			meta.Generation++
		}
	})

	h.commitCond.Broadcast()

	// Sync atomics with the newly committed metadata
	meta := h.GetMetadataSnapshot()
	h.nodeCount.Store(meta.NodeCount)
	h.entryPoint.Store(meta.EntryPoint)
	h.maxLevel.Store(meta.MaxLevel)
}

func (h *ArrowHNSW) updateMetadata(update func(*HNSWMetadata)) {
	for {
		oldMeta := h.metadataRegistry.Load()
		newMeta := &HNSWMetadata{}
		if oldMeta != nil {
			*newMeta = *oldMeta
		}
		update(newMeta)
		newMeta.Version++
		if h.metadataRegistry.CompareAndSwap(oldMeta, newMeta) {
			// Backwards compatibility: keep legacy atomics in sync
			h.entryPoint.Store(newMeta.EntryPoint)
			h.maxLevel.Store(newMeta.MaxLevel)
			h.nodeCount.Store(newMeta.NodeCount)
			break
		}
	}
}

func (h *ArrowHNSW) updateMetadataIfHigher(id uint32, level int32) {
	h.updateMetadata(func(meta *HNSWMetadata) {
		if meta.EntryPoint == math.MaxUint32 || level > meta.MaxLevel {
			meta.MaxLevel = level
			meta.EntryPoint = id
		}
	})
}

// AddByLocation runs the add by location operation.
func (h *ArrowHNSW) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	next := h.nextID.Add(1)
	if next > math.MaxUint32 {
		return 0, fmt.Errorf("index overflow: nextID %d exceeds uint32 max", next)
	}
	id := uint32(next - 1) // #nosec G115

	var vec any
	if h.dataset != nil {
		records := h.dataset.GetRecords()
		if batchIdx < len(records) {
			record := records[batchIdx]
			// Find vector column
			vecColIdx := h.getVectorColumnIndex(record)
			if vecColIdx != -1 {
				vec = h.extractVector(record, vecColIdx, rowIdx)
			}
			// Index external ID from column 0 for O(1) reverse lookup.
			if record.NumCols() > 0 {
				if int64Col, ok := record.Column(0).(*arrowarray.Int64); ok && rowIdx < int64Col.Len() {
					if extID := int64Col.Value(rowIdx); extID >= 0 {
						h.IndexExternalID(uint64(extID), id)
					}
				}
			}
		}
	}

	h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	err := h.InsertWithVector(id, vec, h.levelFor(id))
	if err != nil {
		return 0, err
	}

	return id, nil
}

// AddByRecord runs the add by record operation.
func (h *ArrowHNSW) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	next := h.nextID.Add(1)
	if next > math.MaxUint32 {
		return 0, fmt.Errorf("index overflow: nextID %d exceeds uint32 max", next)
	}
	id := uint32(next - 1) // #nosec G115

	var vec any
	// Find vector column
	vecColIdx := h.getVectorColumnIndex(rec)
	if vecColIdx != -1 {
		vec = h.extractVector(rec, vecColIdx, rowIdx)
	}

	// Index external ID from column 0 for O(1) reverse lookup.
	if rec.NumCols() > 0 {
		if int64Col, ok := rec.Column(0).(*arrowarray.Int64); ok && rowIdx < int64Col.Len() {
			if extID := int64Col.Value(rowIdx); extID >= 0 {
				h.IndexExternalID(uint64(extID), id)
			}
		}
	}

	h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	err := h.InsertWithVector(id, vec, h.levelFor(id))
	if err != nil {
		return 0, err
	}

	return id, nil
}
