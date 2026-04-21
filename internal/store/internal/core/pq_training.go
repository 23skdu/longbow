package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"fmt"

	"github.com/23skdu/longbow/internal/pq"
)

// TrainPQ trains the PQ encoder on the provided sample vectors and enables PQ.
func (h *ArrowHNSW) TrainPQ(vectors [][]float32) error {
	h.growMu.Lock()
	defer h.growMu.Unlock()

	if len(vectors) == 0 {
		return fmt.Errorf("no vectors for training")
	}

	dims := len(vectors[0])
	if dims == 0 {
		return fmt.Errorf("vector dimension is 0")
	}

	m := h.config.PQM
	if m == 0 {
		// Heuristic: M = dims / 4 or dims / 8
		switch {
		case dims%8 == 0:
			m = dims / 8
		case dims%4 == 0:
			m = dims / 4
		default:
			m = 1 // No split
		}
	}

	k := h.config.PQK
	if k == 0 {
		k = 256
	}

	encoder, err := pq.NewPQEncoder(dims, m, k)
	if err != nil {
		return err
	}

	useGPU := h.IsGPUEnabled()
	if useGPU {
		// Flatten vectors for GPU training
		flatVectors := make([]float32, len(vectors)*dims)
		for i, v := range vectors {
			copy(flatVectors[i*dims:(i+1)*dims], v)
		}
		if err := h.TrainPQOnGPU(flatVectors, m, k); err == nil {
			h.gpuTrained.Store(true)
		} else {
			// Fallback to CPU training
			if err := encoder.Train(vectors); err != nil {
				return err
			}
		}
	} else {
		if err := encoder.Train(vectors); err != nil {
			return err
		}
	}

	h.pqEncoder = encoder
	h.config.PQEnabled = true
	h.config.PQM = m
	h.config.PQK = k

	// Atomic COW update for feature enablement
	for {
		oldData := h.data.Load()
		if oldData == nil {
			break
		}
		newData := oldData.Clone()
		newData.PQEnabled = true
		newData.PQM = m
		if h.data.CompareAndSwap(oldData, newData) {
			break
		}
	}

	data := h.data.Load()
	if h.config.AdaptiveMEnabled && !h.adaptiveMTriggered.Load() {
		count := int(h.nodeCount.Load())
		threshold := h.config.AdaptiveMThreshold
		if threshold <= 0 {
			threshold = 1024
		}

		if count >= threshold {
			h.adjustMParameter(data, threshold)
		}
	}
	if data != nil {
		targetCap := int(h.nodeCount.Load())
		if targetCap < data.Capacity {
			targetCap = data.Capacity
		}
		if err := h.growNoLock(targetCap, data.Dims); err != nil {
			return err
		}
		// Refresh pointer after growth
		data = h.data.Load()

		if data.VectorsPQ != nil {
			nodeCount := int(h.nodeCount.Load())
			m := h.config.PQM
			dims := int(h.dims.Load())

			if h.gpuTrained.Load() {
				// Batch encode on GPU
				batchSize := 1024
				for i := 0; i < nodeCount; i += batchSize {
					end := i + batchSize
					if end > nodeCount {
						end = nodeCount
					}

					// Collect vectors for batch
					batchVectors := make([]float32, 0, (end-i)*dims)
					validIDs := make([]uint32, 0, end-i)

					for j := i; j < end; j++ {
						v := h.mustGetVectorFromData(data, uint32(j))
						if vf32, ok := v.([]float32); ok {
							batchVectors = append(batchVectors, vf32...)
							validIDs = append(validIDs, uint32(j))
						}
					}

					if len(batchVectors) > 0 {
						codes, err := h.EncodePQOnGPU(batchVectors)
						if err == nil {
							// Copy codes to PQ chunks
							for idx, vid := range validIDs {
								cID := types.ChunkID(vid)
								cOff := types.ChunkOffset(vid)
								if chunk := data.GetVectorsPQChunk(cID); chunk != nil {
									copy(chunk[int(cOff)*m:(int(cOff)+1)*m], codes[idx*m:(idx+1)*m])
								}
							}
						}
					}
				}
			} else {
				// Sequential CPU encoding
				for i := uint32(0); i < uint32(nodeCount); i++ { // #nosec G115
					v := h.mustGetVectorFromData(data, i)
					if vf32, ok := v.([]float32); ok {
						code, err := encoder.Encode(vf32)
						if err == nil {
							cID := types.ChunkID(i)
							cOff := types.ChunkOffset(i)
							if chunk := data.GetVectorsPQChunk(cID); chunk != nil {
								copy(chunk[int(cOff)*m:(int(cOff)+1)*m], code)
							}
						}
					}
				}
			}
		}
	}

	return nil
}
