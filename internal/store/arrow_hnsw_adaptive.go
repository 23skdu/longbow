package store

import (
	"math"

	"github.com/23skdu/longbow/internal/metrics"
)

// adjustMParameter calculates intrinsic dimensionality and adjusts M/MMax
func (h *ArrowHNSW) adjustMParameter(data *GraphData, sampleSize int) {
	// 1. Mark triggered
	if !h.adaptiveMTriggered.CompareAndSwap(false, true) {
		return
	}

	// 2. Sample vectors
	// We need 'sampleSize' vectors. We just pick 0..sampleSize-1
	var sampleVecs [][]float32
	dims := int(h.dims.Load())

	// Safety check
	if dims == 0 {
		return
	}

	for i := uint32(0); i < uint32(sampleSize); i++ {
		v := h.mustGetVectorFromData(data, i)
		if v != nil {
			if vf32, ok := v.([]float32); ok {
				sampleVecs = append(sampleVecs, vf32)
			}
		}
	}

	if len(sampleVecs) < 20 { // Not enough data
		return
	}

	// 3. Estimate Intrinsic Dimensionality (ID)
	// Heuristic: Ratio of distances?
	// Simplified heuristic: use standard deviation of distances from a pivot?
	// Let's use specific "concentration of measure" idea:
	// ID ~ mean(dist) / std_dev(dist). Higher dimension -> tighter concentration relative to mean.
	// Or simpler: just use random pairs.

	var dists []float32
	numPairs := 500
	if numPairs > len(sampleVecs)*len(sampleVecs) {
		numPairs = len(sampleVecs) * len(sampleVecs)
	}

	// Deterministic sampling for stability or random?
	// Random is better for diversity.
	for k := 0; k < numPairs; k++ {
		// Pick 2 random
		i := k % len(sampleVecs)
		j := (k + 13) % len(sampleVecs) // simple shift
		if i == j {
			continue
		}

		d, err := h.distFunc(sampleVecs[i], sampleVecs[j])
		if err != nil {
			continue
		}
		dists = append(dists, d)
	}

	// Calc stats
	var sum, sumSq float64
	for _, d := range dists {
		sum += float64(d)
		sumSq += float64(d * d)
	}
	mean := sum / float64(len(dists))
	variance := (sumSq / float64(len(dists))) - (mean * mean)
	stdDev := math.Sqrt(variance)

	// Intrinsic Dim Heuristic: mean^2 / variance (roughly)
	// For high dims, variance is low relative to mean.
	// ID = 2 * (mean / stdDev)^2 approximately for certain distributions.
	idEst := 0.0
	if stdDev > 0.000001 {
		idEst = (mean * mean) / (variance)
	}

	metrics.HNSWIntrinsicDimensionality.WithLabelValues("default").Set(idEst)

	// 4. Adjust M
	// Default M=16.
	// If ID > 20, boost M.
	// Logic: NewM = BaseM * (1 + log(ID/10)) ?
	// Simple logic:
	// ID < 10: M = 16 (or lower?) - keep default
	// ID 10-50: M = 24
	// ID > 50: M = 32
	// ID > 100: M = 48

	newM := h.m
	newMMax := h.mMax

	switch {
	case idEst > 100:
		newM = 48
		newMMax = 96
	case idEst > 50:
		newM = 32
		newMMax = 64
	case idEst > 20:
		newM = 24
		newMMax = 48
	}

	// Only adjust if increasing (usually safer for recall)
	if newM > h.m {
		h.m = newM
		h.mMax = newMMax
		h.mMax0 = newMMax * 2
		// Update config for visibility (though config is struct copy usually, h.config might be used elsewhere)
		h.config.M = newM
		h.config.MMax = newMMax
		h.config.MMax0 = newMMax * 2

		metrics.HNSWAdaptiveMValue.WithLabelValues("default").Set(float64(newM))
		metrics.HNSWAdaptiveAdjustments.WithLabelValues("default").Inc()
	}
}
