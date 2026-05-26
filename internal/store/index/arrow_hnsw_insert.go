package index

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(goCtx context.Context, ctx *ArrowSearchContext, query any, entryPoint uint32, ef, layer int, data *types.GraphData) ([]types.Candidate, error) {
	computer := h.resolveHNSWComputer(data, ctx, query, true)
	var res []types.Candidate
	var err error
	if compF32, ok := computer.(*float32ToFloat32Computer); ok {
		res, err = h.searchLayerFloat32(goCtx, compF32, entryPoint, ef, layer, ctx, data)
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
func (h *ArrowHNSW) selectNeighbors(ctx *ArrowSearchContext, candidates []types.Candidate, m int, data *types.GraphData) []types.Candidate {
	if len(candidates) <= m {
		return candidates
	}

	// Optimization: Limit the scope of the diversity check
	limit := h.config.SelectionHeuristicLimit
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}

	if data.Type == types.VectorTypeFloat32 {
		return h.selectNeighborsFloat32(ctx, candidates, m, data)
	}

	// Generic path for other types (Int8, Int16, etc.)
	var selected []types.Candidate
	if ctx != nil {
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]types.Candidate, 0, m)
	}

	if len(candidates) == 0 {
		return nil
	}

	extracted := make([]any, len(candidates))
	for i, cand := range candidates {
		vecAny, _ := data.GetVector(cand.ID)
		extracted[i] = vecAny
	}

	selectedVecs := make([]any, 0, m)

	// For non-float32, we use a cached diversity check
	// We use specialized loops for each type to avoid type assertions in the hot path
	switch data.Type {
	case types.VectorTypeInt8:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]int8)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]int8)
				d, _ := h.distFuncInt8(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeInt16:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]int16)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]int16)
				d, _ := h.distFuncInt16(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeUint16:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]uint16)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]uint16)
				d, _ := h.distFuncUint16(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeInt32:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]int32)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]int32)
				d, _ := h.distFuncInt32(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeUint32:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]uint32)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]uint32)
				d, _ := h.distFuncUint32(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeInt64:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]int64)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]int64)
				d, _ := h.distFuncInt64(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeUint64:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]uint64)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]uint64)
				d, _ := h.distFuncUint64(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeFloat64:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]float64)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]float64)
				d, _ := h.distFuncF64(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeComplex64:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]complex64)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]complex64)
				d, _ := h.distFuncC64(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	case types.VectorTypeComplex128:
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1, _ := extracted[i].([]complex128)
			if v1 == nil {
				continue
			}
			for j := range selected {
				v2 := selectedVecs[j].([]complex128)
				d, _ := h.distFuncC128(v1, v2)
				if d < cand.Dist {
					isDiverse = false
					break
				}
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	default:
		// Fallback for unknown types (slow path)
		for i, cand := range candidates {
			if len(selected) >= m {
				break
			}
			isDiverse := true
			v1 := extracted[i]
			if v1 == nil {
				continue
			}
			for j := range selected {
				_ = selectedVecs[j]
				// CORRECT RobustPrune: only reject if an existing neighbor is CLOSER to the candidate than the query is
				// We don't have a distFunc for unknown types, so we fallback to a simple reachability check
				// (in practice this shouldn't happen as all types are registered)
				// For now, just allow all up to M
			}
			if isDiverse {
				selected = append(selected, cand)
				selectedVecs = append(selectedVecs, v1)
			}
		}
	}

	if len(selected) == 0 && len(candidates) > 0 {
		selected = append(selected, candidates[0])
	}

	if ctx != nil {
		ctx.scratchSelected = selected
	}
	return selected
}

// selectNeighborsFloat32 is a specialized high-performance diversity check for float32 vectors.
func (h *ArrowHNSW) selectNeighborsFloat32(ctx *ArrowSearchContext, candidates []types.Candidate, m int, data *types.GraphData) []types.Candidate {
	var selected []types.Candidate
	if ctx != nil {
		selected = ctx.scratchSelected[:0]
	} else {
		selected = make([]types.Candidate, 0, m)
	}

	var extracted [][]float32
	if ctx != nil {
		if cap(ctx.scratchExtractedF32) >= len(candidates) {
			extracted = ctx.scratchExtractedF32[:len(candidates)]
			for i := range extracted {
				extracted[i] = nil
			}
		} else {
			extracted = make([][]float32, len(candidates))
			ctx.scratchExtractedF32 = extracted
		}
	} else {
		extracted = make([][]float32, len(candidates))
	}

	maxGen := uint64(math.MaxUint64)
	if ctx != nil {
		maxGen = ctx.MaxGeneration
	}

	pd := data.GetPaddedDimsForType(types.VectorTypeFloat32)
	for i, cand := range candidates {
		cID := int(cand.ID) / types.ChunkSize
		cOff := int(cand.ID) % types.ChunkSize
		chunk := data.GetVectorsChunkWithGen(cID, maxGen)
		if chunk != nil {
			start := cOff * pd
			if start+data.Dims <= len(chunk) {
				extracted[i] = chunk[start : start+data.Dims]
			}
		}
	}

	// Try GPU pruning first if enabled
	if h.gpuEnabled && h.gpuIndex != nil && len(candidates) > 16 {
		candIds := make([]uint32, len(candidates))
		candDists := make([]float32, len(candidates))
		for i, c := range candidates {
			candIds[i] = uint32(c.ID)
			candDists[i] = c.Dist
		}

		selectedIds, err := h.pruneNeighborsGPU(candIds, candDists, m)
		if err == nil {
			// Convert back to types.Candidate
			res := make([]types.Candidate, 0, len(selectedIds))
			// We need distances to keep the Candidate structure consistent
			// But for pruning results, we only care about IDs usually.
			// However, HNSW might need them.
			// For simplicity, we find the original candidate for each ID.
			candMap := make(map[uint32]float32)
			for i := range candidates {
				candMap[uint32(candidates[i].ID)] = candidates[i].Dist
			}

			for _, id := range selectedIds {
				res = append(res, types.Candidate{ID: id, Dist: candMap[id]})
			}

			if ctx != nil {
				ctx.scratchSelected = res
			}
			return res
		}
	}

	var selectedVecs [][]float32
	if ctx != nil {
		selectedVecs = ctx.scratchSelectedVecsF32[:0]
	} else {
		selectedVecs = make([][]float32, 0, m)
	}

	for i, cand := range candidates {
		if len(selected) >= m {
			break
		}

		isDiverse := true
		v1 := extracted[i]
		if v1 == nil {
			continue
		}

		for j := range selected {
			v2 := selectedVecs[j]

			d, err := h.distFunc(v1, v2)

			threshold := cand.Dist
			if h.config.SQ8Enabled {
				threshold *= 1.2
			}

			if err == nil && d > 0 && d < threshold {
				isDiverse = false
				break
			}
		}

		if isDiverse {
			selected = append(selected, cand)
			selectedVecs = append(selectedVecs, v1)
		}
	}

	if len(selected) == 0 && len(candidates) > 0 {
		selected = append(selected, candidates[0])
	}

	if ctx != nil {
		ctx.scratchSelected = selected
		ctx.scratchSelectedVecsF32 = selectedVecs
	}
	return selected
}

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
		} else {
			// Already committed or skipped
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
		}
	}

	h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	err := h.InsertWithVector(id, vec, h.generateLevel())
	if err != nil {
		return 0, err
	}

	return id, nil
}

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

	h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	err := h.InsertWithVector(id, vec, h.generateLevel())
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (h *ArrowHNSW) generateLevel() int {
	l := int(math.Floor(-math.Log(rand.Float64()) * h.levelMultiplier)) // #nosec G404
	if l >= types.ArrowMaxLayers {
		l = types.ArrowMaxLayers - 1
	}
	return l
}

func (h *ArrowHNSW) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	h.bulkMu.Lock()
	var startID uint32
	var startIDAssigned bool
	defer func() {
		h.bulkMu.Unlock()
		if startIDAssigned {
			n := len(rowIdxs)
			finalID := int64(startID + uint32(n)) // #nosec G115
			h.commitMu.Lock()
			for h.nodeCount.Load() < int64(startID) {
				h.commitCond.Wait()
			}
			if h.nodeCount.Load() < finalID {
				h.nodeCount.Store(finalID)
			}
			h.commitCond.Broadcast()
			h.commitMu.Unlock()
		}
	}()

	n := len(rowIdxs)
	if n == 0 {
		return nil, nil
	}

	// Discover vector column
	var schemaSource arrow.RecordBatch
	for _, r := range recs {
		if r != nil {
			schemaSource = r
			break
		}
	}

	vecColIdx := h.getVectorColumnIndex(schemaSource)

	if vecColIdx == -1 {
		var colNames []string
		if schemaSource != nil {
			for i := 0; i < int(schemaSource.NumCols()); i++ {
				colNames = append(colNames, schemaSource.ColumnName(i))
			}
		}
		return nil, fmt.Errorf("no vector column found (looked for 'vector', 'embedding', 'vec'); available columns: %v", colNames)
	}

	// Allocate local IDs for the entire batch to ensure monotonic assignment and avoid overwrites
	newNext := h.nextID.Add(int64(n))
	if newNext > math.MaxUint32+1 {
		h.nextID.Add(-int64(n)) // Roll back
		return nil, fmt.Errorf("vector ID overflow: nextID %d exceeds max uint32", newNext-1)
	}
	startID = uint32(newNext - int64(n)) // #nosec G115 - bounds checked above
	startIDAssigned = true

	// Ensure the index is grown to accommodate the new batch before parallel ingestion.
	if n > 0 && vecColIdx != -1 {
		h.growMu.Lock()
		data := h.data.Load()
		if data == nil || int(startID)+n > data.Capacity || h.dims.Load() == 0 {
			// Extract first vector to determine dimensions
			var recFirst arrow.RecordBatch
			if len(recs) == 1 {
				recFirst = recs[0]
			} else if batchIdxs[0] >= 0 && batchIdxs[0] < len(recs) {
				recFirst = recs[batchIdxs[0]]
			}

			if recFirst != nil {
				v := h.extractVector(recFirst, vecColIdx, rowIdxs[0])
				if v != nil {
					dims := 0
					switch vt := v.(type) {
					case []float32:
						dims = len(vt)
					case []float16.Num:
						dims = len(vt)
					case []float64:
						dims = len(vt)
					case []int32:
						dims = len(vt)
					case []uint32:
						dims = len(vt)
					case []int16:
						dims = len(vt)
					case []uint16:
						dims = len(vt)
					case []int8:
						dims = len(vt)
					case []uint8:
						dims = len(vt)
					case []int64:
						dims = len(vt)
					case []uint64:
						dims = len(vt)
					case []complex64:
						dims = len(vt)
					case []complex128:
						dims = len(vt)
					}

					if dims > 0 {
						newCap := int(startID) + n
						if data != nil && data.Capacity > 0 {
							newCap = int(math.Max(float64(newCap), float64(data.Capacity*2)))
						}
						newCap = (newCap + types.ChunkSize - 1) & ^(types.ChunkSize - 1)
						_ = h.growInternal(newCap, dims)
					}
				}
			}
		}
		h.growMu.Unlock()
	}

	// Bulk optimization path with parallel linkage.
	// AddBatchBulk handles its own bootstrap sequentially then links remaining in parallel.
	if n >= BulkInsertThreshold {

		if vecColIdx != -1 {
			// Extract all vectors into a typed slice for bulk processing
			var vecs any
			supported := true
			switch h.config.DataType {
			case types.VectorTypeFloat32:
				f32s := make([][]float32, n)
				// Cache raw slices per record batch to avoid expensive column calls
				valuesCache := make(map[arrow.RecordBatch][]float32)
				// Pre-populate valuesCache
				for i := range rowIdxs {
					// Robust record resolution: if we only have one record, use it regardless of batchIdx
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}

					if rec != nil {
						if _, ok := valuesCache[rec]; !ok {
							col := rec.Column(vecColIdx)
							if f32Arr, okCol := col.(*arrowarray.Float32); okCol {
								valuesCache[rec] = f32Arr.Float32Values()
							}
						}
					}
				}
				physicalDims := int(h.dims.Load())

				// Parallel extraction
				pool := GetSharedPool()
				var supportedAtomic atomic.Bool
				supportedAtomic.Store(true)

				pool.ParallelFor(n, max(256, (n+runtime.NumCPU()-1)/runtime.NumCPU()), func(start, end int) {
					if !supportedAtomic.Load() {
						return
					}

					for i := start; i < end; i++ {
						// Robust record resolution
						var rec arrow.RecordBatch
						bIdx := batchIdxs[i]
						if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
							rec = recs[bIdx]
						} else if len(recs) == 1 {
							rec = recs[0]
						}

						if rec == nil {
							supportedAtomic.Store(false)
							return
						}

						values := valuesCache[rec]

						if values != nil {
							off := rowIdxs[i] * physicalDims
							if off+physicalDims <= len(values) {
								f32s[i] = values[off : off+physicalDims]
								h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}) // #nosec G115
							} else {
								supportedAtomic.Store(false)
								return
							}
						} else {
							// Fallback to slow path if type mismatch
							if v, okC := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float32); okC {
								f32s[i] = v
								h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}) // #nosec G115
							} else {
								supportedAtomic.Store(false)
								return
							}
						}
					}
				})
				supported = supportedAtomic.Load()
				vecs = f32s

				// Zero-Copy Direct Mapping Optimization
				// If we are ingesting a full contiguous block that aligns with HNSW chunks,
				// we map the Arrow memory instead of copying into arenas.
				if len(recs) == 1 && startID%uint32(types.ChunkSize) == 0 && n >= types.ChunkSize {
					isContiguous := rowIdxs[0]%types.ChunkSize == 0
					if isContiguous {
						for j := 1; j < n; j++ {
							if rowIdxs[j] != rowIdxs[j-1]+1 {
								isContiguous = false
								break
							}
						}
					}

					if isContiguous {
						rec := recs[0]
						values := valuesCache[rec]
						if values != nil {
							data := h.data.Load()
							numFullChunks := n / types.ChunkSize
							for c := 0; c < numFullChunks; c++ {
								cID := int(startID)/types.ChunkSize + c
								rowOffset := rowIdxs[0] + (c * types.ChunkSize)

								offset := rowOffset * physicalDims
								dataSize := types.ChunkSize * physicalDims
								if offset+dataSize <= len(values) {
									chunkData := values[offset : offset+dataSize]
									col := rec.Column(vecColIdx)
									_ = data.SetZeroCopyMapping(cID, chunkData, col)
								}
							}
						}
					}
				}
			case types.VectorTypeFloat16:
				f16s := make([][]float16.Num, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float16.Num); ok {
						f16s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = f16s
			case types.VectorTypeInt8:
				i8s := make([][]int8, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int8); ok {
						i8s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i8s
			case types.VectorTypeFloat64:
				f64s := make([][]float64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float64); ok {
						f64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = f64s
			case types.VectorTypeComplex64:
				c64s := make([][]complex64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]complex64); ok {
						c64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = c64s
			case types.VectorTypeComplex128:
				c128s := make([][]complex128, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]complex128); ok {
						c128s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = c128s
			case types.VectorTypeUint32:
				u32s := make([][]uint32, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint32); ok {
						u32s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = u32s
			case types.VectorTypeInt32:
				i32s := make([][]int32, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int32); ok {
						i32s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i32s
			case types.VectorTypeInt16:
				i16s := make([][]int16, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int16); ok {
						i16s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i16s
			case types.VectorTypeUint16:
				u16s := make([][]uint16, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint16); ok {
						u16s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = u16s
			case types.VectorTypeInt64:
				i64s := make([][]int64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int64); ok {
						i64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i64s
			case types.VectorTypeUint64:
				u64s := make([][]uint64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint64); ok {
						u64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = u64s
			default:
				supported = false
			}

			if supported && vecs != nil {
				err := h.addBatchBulkInternal(ctx, startID, n, vecs)
				if err == nil {
					ids := make([]uint32, n)
					for i := 0; i < n; i++ {
						ids[i] = startID + uint32(i)
					}
					return ids, nil
				}
			}
		}
	}



	// Fallback to sequential insertion if bulk fails (rare)
	ids := make([]uint32, len(rowIdxs))
	maxID := startID + uint32(len(rowIdxs)) - 1 // #nosec G115
	data, err := h.EnsureChunks(int(types.ChunkID(startID)), int(types.ChunkID(maxID)), int(h.dims.Load()))
	if err == nil {
		data = data.Clone()
	}
	if err != nil {
		return nil, err
	}
	// h.data.Store(data)

	// Phase 1: Sequential Vector Ingestion
	// Ensures all vectors are persistent in arenas before we start linking nodes.
	for i := 0; i < len(rowIdxs); i++ {
		id := startID + uint32(i) // #nosec G115

		// Resolve record batch
		var rec arrow.RecordBatch
		bIdx := batchIdxs[i]
		if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
			rec = recs[bIdx]
		} else if len(recs) == 1 {
			rec = recs[0]
		}

		if rec == nil {
			continue
		}

		v := h.extractVector(rec, vecColIdx, rowIdxs[i])
		if v != nil {
			if err := data.SetVector(id, v); err != nil {
				return nil, err
			}
			h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
		}
	}

	// Publish the populated snapshot
	h.compareAndSwapData(h.data.Load(), data.Clone())

	// Phase 1.5: Sequential Bootstrap
	// If the index is empty or very small, we must insert some nodes sequentially
	// to establish an entry point and basic graph structure before parallel insertion.
	bootstrapEnd := 0
	nodeCount := h.GetMetadataSnapshot().NodeCount
	seedCount := 256
	if nodeCount < int64(seedCount) {
		bootstrapEnd = seedCount - int(nodeCount)
		if bootstrapEnd > len(rowIdxs) {
			bootstrapEnd = len(rowIdxs)
		}
		if bootstrapEnd < 0 {
			bootstrapEnd = 0
		}
	}

	for i := 0; i < len(rowIdxs); i++ {
		id := startID + uint32(i) // #nosec G115

		var rec arrow.RecordBatch
		bIdx := batchIdxs[i]
		if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
			rec = recs[bIdx]
		} else if len(recs) == 1 {
			rec = recs[0]
		}

		if rec == nil {
			return nil, fmt.Errorf("could not resolve record batch for index %d", i)
		}

		vec := h.extractVector(rec, vecColIdx, rowIdxs[i])
		if vec == nil {
			return nil, fmt.Errorf("vector missing for row %d", rowIdxs[i])
		}

		// Insert node-by-node. InsertWithVector uses in-place arena updates (lock-free)
		// and commitID for sequential metadata commitment (serialized).
		if err := h.InsertWithVector(id, vec, -1); err != nil {
			return nil, err
		}
		ids[i] = id
	}

	return ids, nil
}
