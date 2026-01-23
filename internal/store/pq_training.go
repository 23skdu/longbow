package store

import (
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

	if err := encoder.Train(vectors); err != nil {
		return err
	}

	h.pqEncoder = encoder
	h.config.PQEnabled = true
	h.config.PQM = m
	h.config.PQK = k

	data := h.data.Load()

	if h.config.AdaptiveMEnabled && !h.adaptiveMTriggered.Load() {
		count := int(h.nodeCount.Load())
		threshold := h.config.AdaptiveMThreshold
		if threshold <= 0 {
			threshold = 100
		}

		if count == threshold {
			h.adjustMParameter(data, threshold)
		}
	}
	if data != nil {
		h.growNoLock(data.Capacity, data.Dims)
		data = h.data.Load()

		if data.VectorsPQ != nil {
			nodeCount := int(h.nodeCount.Load())
			for i := uint32(0); i < uint32(nodeCount); i++ {
				v := h.mustGetVectorFromData(data, i)
				if v == nil {
					continue
				}
				vf32, ok := v.([]float32)
				if !ok {
					continue
				}
				code, err := encoder.Encode(vf32)
				if err != nil {
					continue
				}
				cID := chunkID(i)
				cOff := chunkOffset(i)

				data = h.ensureChunk(data, cID, cOff, data.Dims)

				if chunk := data.GetVectorsPQChunk(cID); chunk != nil {
					copy(chunk[int(cOff)*m:(int(cOff)+1)*m], code)
				}
			}
		}
	}

	return nil
}
