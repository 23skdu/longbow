package index

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/23skdu/longbow/internal/store/types"
)

// AddBatch runs the add batch operation.
func (h *ArrowHNSW) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	// Count callers for the whole call, not just the bulk link. The R8 quality
	// gate can only measure a batch's reachability when no other ingest caller is
	// mutating the graph; it reads inBulkInsert for this and misses the case
	// where the other callers are in their own bulk link or their own sequential
	// fallback at that moment.
	h.inAddBatch.Add(1)
	defer h.inAddBatch.Add(-1)

	h.bulkMu.Lock()
	var startID uint32
	var startIDAssigned bool
	var bulkMuReleased bool
	defer func() {
		if !bulkMuReleased {
			h.bulkMu.Unlock()
		}
		if startIDAssigned {
			n := len(rowIdxs)
			finalID := int64(startID + uint32(n)) // #nosec G115
			h.commitMu.Lock()
			deadline := time.Now().Add(commitWaitTimeout)
			wake := time.AfterFunc(commitWaitTimeout, func() {
				h.commitMu.Lock()
				h.commitCond.Broadcast()
				h.commitMu.Unlock()
			})
			for h.nodeCount.Load() < int64(startID) && time.Now().Before(deadline) {
				h.commitCond.Wait()
			}
			wake.Stop()
			if h.nodeCount.Load() < finalID {
				h.nodeCount.Store(finalID)
			}
			h.commitCond.Broadcast()
			h.commitMu.Unlock()
			// Ensure metadata registry stays in sync with nodeCount even if
			// addBatchBulkInternal failed or timed out. Search reads NodeCount
			// from the metadata registry, so an inconsistency produces 0 results.
			nc := h.nodeCount.Load()
			if nc > 0 {
				h.updateMetadata(func(meta *HNSWMetadata) {
					if nc > meta.NodeCount {
						meta.NodeCount = nc
					}
				})
			}
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
					dims := VectorLength(v)

					if dims > 0 {
						h.dims.Store(int32(dims)) // #nosec G115
						h.config.Dims = dims

						newCap := int(startID) + n
						if data != nil && data.Capacity > 0 {
							grown := data.Capacity * 2
							if data.Capacity >= 256*types.ChunkSize {
								grown = data.Capacity + data.Capacity/4
							}
							if grown > newCap {
								newCap = grown
							}
						}
						newCap = (newCap + types.ChunkSize - 1) & ^(types.ChunkSize - 1)
						_ = h.growInternal(newCap, dims)
					}
				}
			}
		}
		h.growMu.Unlock()
	}

	// Unlock bulkMu to allow concurrent streams to process their batches in parallel.
	// nextID has already been reserved and growInternal has already secured the capacity.
	h.bulkMu.Unlock()
	bulkMuReleased = true

	// Bulk optimization path with parallel linkage.
	// AddBatchBulk handles its own bootstrap sequentially then links remaining in parallel.
	if n >= BulkInsertThreshold {

		if vecColIdx != -1 {
			// Extract all vectors into a typed slice for bulk processing
			var vecs any
			supported := true
			switch h.config.DataType {
			case types.VectorTypeFloat32, types.VectorTypeTQ:
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
							if list, okList := col.(*arrowarray.FixedSizeList); okList {
								if f32Arr, okArr := list.ListValues().(*arrowarray.Float32); okArr {
									valuesCache[rec] = f32Arr.Float32Values()
								}
							} else if f32Arr, okCol := col.(*arrowarray.Float32); okCol {
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
							// Pin a reader so a concurrent compareAndSwapData
							// (from addBatchBulkInternal) cannot Release() the
							// published GraphData while we mutate its
							// SetZeroCopyMapping fields. Without this, a Release()
							// could race with the writes below and silently drop
							// the zero-copy mapping.
							data := h.data.Load()
							data.AcquireReader()
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
							data.ReleaseReader()
						}
					}
				}
			case types.VectorTypeUint8:
				u8s := make([][]uint8, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint8); ok {
							u8s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = u8s
			case types.VectorTypeFloat16:
				f16s := make([][]float16.Num, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float16.Num); ok {
							f16s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = f16s
			case types.VectorTypeInt8:
				i8s := make([][]int8, n)
				valuesCache := make(map[arrow.RecordBatch][]int8)
				for i := range rowIdxs {
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
							if list, okList := col.(*arrowarray.FixedSizeList); okList {
								if i8Arr, okArr := list.ListValues().(*arrowarray.Int8); okArr {
									valuesCache[rec] = i8Arr.Int8Values()
								}
							} else if i8Arr, okArr := col.(*arrowarray.Int8); okArr {
								valuesCache[rec] = i8Arr.Int8Values()
							}
						}
					}
				}
				pool := GetSharedPool()
				var supportedAtomic atomic.Bool
				supportedAtomic.Store(true)
				pool.ParallelFor(n, max(256, (n+runtime.NumCPU()-1)/runtime.NumCPU()), func(start, end int) {
					if !supportedAtomic.Load() {
						return
					}
					for i := start; i < end; i++ {
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
						physicalDims := int(h.dims.Load())
						if values != nil {
							sizePerRow := physicalDims
							if list, okList := rec.Column(vecColIdx).(*arrowarray.FixedSizeList); okList {
								sizePerRow = int(list.DataType().(*arrow.FixedSizeListType).Len())
							}
							off := rowIdxs[i] * sizePerRow
							if off+physicalDims <= len(values) && physicalDims > 0 {
								i8s[i] = values[off : off+physicalDims]
								h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}) // #nosec G115
							} else {
								supportedAtomic.Store(false)
								return
							}
						} else {
							if v, okC := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int8); okC {
								i8s[i] = v
								h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}) // #nosec G115
							} else {
								supportedAtomic.Store(false)
								return
							}
						}
					}
				})
				supported = supportedAtomic.Load()
				vecs = i8s
			case types.VectorTypeFloat64:
				f64s := make([][]float64, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float64); ok {
							f64s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = f64s
			case types.VectorTypeComplex64:
				c64s := make([][]complex64, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]complex64); ok {
							c64s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = c64s
			case types.VectorTypeComplex128:
				if isContiguousArrowBatches(recs, batchIdxs, rowIdxs) {
					// Stream chunked directly from Arrow RecordBatches inside bulk_column_reader
					// without materializing a [][]complex128 slice for all rows upfront (P0-2.4).
					vecs = recs
				} else {
					c128s := make([][]complex128, n)
					for i := range rowIdxs {
						var rec arrow.RecordBatch
						bIdx := batchIdxs[i]
						if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
							rec = recs[bIdx]
						} else if len(recs) == 1 {
							rec = recs[0]
						}
						if rec != nil {
							if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]complex128); ok {
								c128s[i] = v
								h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
								continue
							}
						}
						supported = false
						break
					}
					vecs = c128s
				}
			case types.VectorTypeUint32:
				u32s := make([][]uint32, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint32); ok {
							u32s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = u32s
			case types.VectorTypeInt32:
				i32s := make([][]int32, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int32); ok {
							i32s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = i32s
			case types.VectorTypeInt16:
				i16s := make([][]int16, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int16); ok {
							i16s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = i16s
			case types.VectorTypeUint16:
				u16s := make([][]uint16, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint16); ok {
							u16s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = u16s
			case types.VectorTypeInt64:
				i64s := make([][]int64, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int64); ok {
							i64s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = i64s
			case types.VectorTypeUint64:
				u64s := make([][]uint64, n)
				for i := range rowIdxs {
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}
					if rec != nil {
						if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint64); ok {
							u64s[i] = v
							h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
							continue
						}
					}
					supported = false
					break
				}
				vecs = u64s
			default:
				supported = false
			}

			if supported && vecs != nil {
				h.inBulkInsert.Add(1)
				err := h.addBatchBulkInternal(ctx, startID, n, vecs)
				h.inBulkInsert.Add(-1)
				if err == nil {
					ids := make([]uint32, n)
					for i := 0; i < n; i++ {
						ids[i] = startID + uint32(i)
					}
					return ids, nil
				}
				// A cancelled or expired context is terminal: falling back to
				// the sequential path would redo the whole batch and blow
				// through the deadline we were supposed to respect.
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
				fmt.Printf("[INFO][HNSW] Bulk insert quality guard or failure (%v), falling back to sequential AddBatch\n", err)
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
			h.encodeAndStoreTQ(data, id, v)
			h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
		}
	}

	// Publish the populated snapshot
	h.publishDataClone(data)

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
		// This fallback runs one InsertWithVector per row, so it must honour
		// the caller's deadline instead of running the whole batch blind.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
			// Debug: capture details about why extraction failed
			col := rec.Column(vecColIdx)
			return nil, fmt.Errorf("vector missing for row %d (rec.NumRows=%d, batchIdx=%d, colType=%T)", rowIdxs[i], rec.NumRows(), batchIdxs[i], col)
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

func isContiguousArrowBatches(recs []arrow.RecordBatch, batchIdxs, rowIdxs []int) bool {
	if len(recs) == 0 || len(rowIdxs) == 0 || len(rowIdxs) != len(batchIdxs) {
		return false
	}
	total := 0
	for _, r := range recs {
		if r == nil {
			return false
		}
		total += int(r.NumRows())
	}
	if total != len(rowIdxs) {
		return false
	}
	pos := 0
	for bIdx, r := range recs {
		rows := int(r.NumRows())
		for row := 0; row < rows; row++ {
			if batchIdxs[pos] != bIdx || rowIdxs[pos] != row {
				return false
			}
			pos++
		}
	}
	return true
}
