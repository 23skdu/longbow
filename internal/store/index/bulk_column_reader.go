package index

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	types "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// activeNode represents an in-flight vector ready for graph linkage.
type activeNode struct {
	id    uint32
	level int
	vec   any // Can be []float32, []float16.Num, etc.
}

// resolveArrowBatchLayout flattens an []arrow.RecordBatch into a per-row index
// so that the parallel preparation loop can locate the owning batch of a row
// with a binary search and reuse a pre-resolved vector column index.
//
// recRowStart[k] is the first global row index of batch k (recRowStart[0] == 0);
// batches are laid out back to back in slice order. recVecCol[k] caches the
// vector column index, or -1 when it could not be resolved and the caller
// should fall back to the metadata lookup.
//
// For any other vector representation both results are nil, which makes the
// caller skip the lookup entirely.
func resolveArrowBatchLayout(vecs any) (recRowStart []int, recVecCol []int) {
	vs, ok := vecs.([]arrow.RecordBatch)
	if !ok || len(vs) == 0 {
		return nil, nil
	}

	recRowStart = make([]int, len(vs)+1)
	recVecCol = make([]int, len(vs))
	for i, rec := range vs {
		if rec != nil {
			recRowStart[i+1] = recRowStart[i] + int(rec.NumRows())
		} else {
			recRowStart[i+1] = recRowStart[i]
		}
	}
	return recRowStart, recVecCol
}

// extractAndIngestBatchVectors unrolls, validates, quantizes, and writes batch
// vectors into the private GraphData copy in parallel across all available worker threads.
func (h *ArrowHNSW) extractAndIngestBatchVectors(
	ctx context.Context,
	startID uint32,
	n, dims int,
	vecs any,
	data *types.GraphData,
) ([]activeNode, error) {
	activeNodes := make([]activeNode, n)

	// Pre-load vectors and generate levels (Parallel)
	pool := GetSharedPool()
	var errPrep error
	var errMu sync.Mutex

	// Slice into chunks for workers to amortize goroutine overhead
	chunkSize := (n + runtime.NumCPU() - 1) / runtime.NumCPU()
	if chunkSize < 64 {
		chunkSize = 64 // Minimum chunk size to justify overhead
	}

	highPriority := types.IsHighPriority(ctx)
	parallelFor := pool.ParallelFor
	if highPriority {
		parallelFor = pool.ParallelForHighPriority
	}

	// Resolve the Arrow batch layout once, before dispatching workers.
	recRowStart, recVecCol := resolveArrowBatchLayout(vecs)
	if recVecCol != nil {
		for i, rec := range vecs.([]arrow.RecordBatch) {
			if rec != nil {
				recVecCol[i] = h.getVectorColumnIndex(rec)
			}
		}
	}

	parallelFor(n, chunkSize, func(start, end int) {
		errMu.Lock()
		if errPrep != nil || ctx.Err() != nil {
			if errPrep == nil {
				errPrep = ctx.Err()
			}
			errMu.Unlock()
			return
		}
		errMu.Unlock()

		for j := start; j < end; j++ {
			id := startID + uint32(j) // #nosec G115
			cID := types.ChunkID(id)
			cOff := types.ChunkOffset(id)

			// Level generation, derived from the node ID so the bulk linker
			// produces the same levels whatever order the parallel prep loop
			// reaches the nodes in (roadmap Item 31).
			level := h.levelFor(id)

			// Vector Ingestion (Zero-Copy from passed batch)
			var v any
			// Type switch to extract vector from generic batch
			switch vs := vecs.(type) {
			case [][]float32:
				switch h.config.DataType {
				case types.VectorTypeComplex64:
					f32s := vs[j]
					c64s := make([]complex64, len(f32s)/2)
					for k := 0; k < len(f32s)/2; k++ {
						c64s[k] = complex(f32s[2*k], f32s[2*k+1])
					}
					v = c64s
				case types.VectorTypeComplex128:
					f32s := vs[j]
					c128s := make([]complex128, len(f32s)/2)
					for k := 0; k < len(f32s)/2; k++ {
						c128s[k] = complex(float64(f32s[2*k]), float64(f32s[2*k+1]))
					}
					v = c128s
				default:
					v = vs[j]
				}
			case []arrow.RecordBatch:
				// recRowStart is the prefix sum of the batch row counts,
				// so locating the owning batch is a binary search instead
				// of a scan, and the vector column index was resolved
				// once for the whole batch instead of per row.
				recIdx := sort.Search(len(recRowStart), func(k int) bool {
					return recRowStart[k] > j
				}) - 1
				if recIdx >= 0 && recIdx < len(vs) && vs[recIdx] != nil {
					rec := vs[recIdx]
					idx := recVecCol[recIdx]
					if idx == -1 {
						idx = h.getVectorColumnIndex(rec)
					}
					if idx != -1 {
						v = h.extractVector(rec, idx, j-recRowStart[recIdx])
					}
				}
			case [][]uint32:
				v = vs[j]
			case [][]int32:
				v = vs[j]
			case [][]uint16:
				v = vs[j]
			case [][]int16:
				v = vs[j]
			case [][]uint8:
				v = vs[j]
			case [][]int8:
				v = vs[j]
			case [][]int64:
				v = vs[j]
			case [][]uint64:
				v = vs[j]
			case [][]float64:
				v = vs[j]
			case [][]complex64:
				v = vs[j]
			case [][]complex128:
				v = vs[j]
			case [][]float16.Num:
				v = vs[j]
			default:
				errMu.Lock()
				errPrep = fmt.Errorf("unsupported vector type in bulk insert: %T", vecs)
				errMu.Unlock()
				return
			}

			// Basic validation
			if v == nil {
				errMu.Lock()
				errPrep = fmt.Errorf("vector missing for bulk insert ID %d (nil slice)", id)
				errMu.Unlock()
				return
			}

			// Validate dimensions based on type
			vLen := VectorLength(v)

			if vLen != dims {
				metrics.BulkInsertDimensionErrorsTotal.Inc()
				errMu.Lock()
				errPrep = types.NewVectorDimensionMismatchError(int(id), dims, vLen)
				errMu.Unlock()
				return
			}

			// Always ingest into hot storage using method that handles all types
			// Use private data snapshot for vector storage
			if err := data.SetVector(id, v); err != nil {
				errMu.Lock()
				errPrep = err
				errMu.Unlock()
				return
			}

			// 2. SQ8 Ingestion
			// Parallelize quantization - significant speedup for large batches
			if h.config.SQ8Enabled && h.quantizer != nil && h.sq8Ready.Load() {
				sq8Chunk := data.GetVectorsSQ8Chunk(cID)
				if sq8Chunk != nil {
					if vf32, ok := v.([]float32); ok {
						sq8Stride := (dims + 63) & ^63
						startOff := int(cOff) * sq8Stride
						dest := sq8Chunk[startOff : startOff+dims]
						h.quantizer.Encode(vf32, dest)
					}
				}
			}

			// 3. BQ Ingestion
			if h.config.BQEnabled && h.bqEncoder != nil {
				bqChunk := data.GetVectorsBQChunk(cID)
				if bqChunk != nil {
					if vf32, ok := v.([]float32); ok {
						code := h.bqEncoder.Encode(vf32)
						numWords := h.bqEncoder.CodeSize()
						dest := bqChunk[int(cOff)*numWords : (int(cOff)+1)*numWords]
						copy(dest, code)
					}
				}
			}

			// 4. PQ Ingestion
			if h.config.PQEnabled && h.oopqEncoder != nil {
				pqChunk := data.GetVectorsPQChunk(cID)
				if pqChunk != nil {
					if vf32, ok := v.([]float32); ok {
						switch enc := h.oopqEncoder.(type) {
						case *pq.PQEncoder:
							code, err := enc.Encode(vf32)
							if err == nil {
								pqM := h.config.PQM
								dest := pqChunk[int(cOff)*pqM : (int(cOff)+1)*pqM]
								copy(dest, code)
							}
						case *pq.OPQEncoder:
							code, err := enc.Encode(vf32)
							if err == nil {
								pqM := h.config.PQM
								dest := pqChunk[int(cOff)*pqM : (int(cOff)+1)*pqM]
								copy(dest, code)
							}
						}
					}
				}
			}

			// 5. TQ Ingestion
			tqEnc := tqEncoderOf(h)
			if tqEnc != nil {
				tqChunk := data.GetVectorsTQChunk(cID)
				if tqChunk != nil {
					if vf32, ok := v.([]float32); ok {
						code, err := tqEnc.Encode(vf32)
						if err == nil {
							stride := data.PackedSize()
							dest := tqChunk[int(cOff)*stride : (int(cOff)+1)*stride]
							copy(dest, code)
						}
					}
				}
			}

			activeNodes[j] = activeNode{
				id:    id,
				level: level,
				vec:   v,
			}

			// Mandatory location registration for HNSW navigator
			if _, ok := h.GetLocation(id); !ok {
				h.SetLocation(types.VectorID(id), types.Location{BatchIdx: 0, RowIdx: int(id)})
			}

			// Init levels chunk if needed
			levelsChunk := data.GetLevelsChunk(cID)
			if levelsChunk != nil {
				atomic.StoreUint32(&levelsChunk[cOff], uint32(level)) // #nosec G115
			}
		}
	})

	if errPrep != nil {
		return nil, errPrep
	}
	return activeNodes, nil
}
