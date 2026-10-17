package simd

import (
	"errors"
	"math"
)

// SQ8ChannelCalibration contains per-channel calibration statistics
// for non-uniform scalar quantization of skewed high-dimensional embeddings.
type SQ8ChannelCalibration struct {
	Dims   int
	Mins   []float32
	Maxs   []float32
	Scales []float32 // 255.0 / (Max - Min)
	InvSq  []float32 // 1.0 / (Scale * Scale) for fast weighted distance evaluation
}

// NewSQ8ChannelCalibration creates an empty calibration for dims dimensions.
func NewSQ8ChannelCalibration(dims int) *SQ8ChannelCalibration {
	return &SQ8ChannelCalibration{
		Dims:   dims,
		Mins:   make([]float32, dims),
		Maxs:   make([]float32, dims),
		Scales: make([]float32, dims),
		InvSq:  make([]float32, dims),
	}
}

// CalibrateSQ8Channels calculates per-channel empirical bounds with 3-sigma outlier clipping.
func CalibrateSQ8Channels(vectors [][]float32, dims int) (*SQ8ChannelCalibration, error) {
	if len(vectors) == 0 || dims <= 0 {
		return nil, errors.New("simd: empty vectors or invalid dims for calibration")
	}

	calib := NewSQ8ChannelCalibration(dims)
	n := float64(len(vectors))

	for j := 0; j < dims; j++ {
		var sum, sumSq float64
		minObs := float32(math.MaxFloat32)
		maxObs := float32(-math.MaxFloat32)

		for _, vec := range vectors {
			if j < len(vec) {
				v := vec[j]
				sum += float64(v)
				sumSq += float64(v) * float64(v)
				if v < minObs {
					minObs = v
				}
				if v > maxObs {
					maxObs = v
				}
			}
		}

		mean := sum / n
		variance := (sumSq / n) - (mean * mean)
		if variance < 1e-12 {
			variance = 1e-12
		}
		stdDev := math.Sqrt(variance)

		// 3-sigma empirical bounds with clipping to observed bounds
		cMin := float32(mean - 3.0*stdDev)
		cMax := float32(mean + 3.0*stdDev)

		if cMin < minObs {
			cMin = minObs
		}
		if cMax > maxObs {
			cMax = maxObs
		}

		span := cMax - cMin
		var scale float32
		if span > 1e-8 {
			scale = 255.0 / span
		} else {
			scale = 0
		}

		calib.Mins[j] = cMin
		calib.Maxs[j] = cMax
		calib.Scales[j] = scale
		if scale > 0 {
			calib.InvSq[j] = 1.0 / (scale * scale)
		} else {
			calib.InvSq[j] = 0
		}
	}

	return calib, nil
}

// QuantizeSQ8Adaptive quantizes a float32 vector to uint8 using per-channel calibration.
func QuantizeSQ8Adaptive(src []float32, dst []byte, calib *SQ8ChannelCalibration) {
	dims := calib.Dims
	if len(src) < dims {
		dims = len(src)
	}

	for j := 0; j < dims; j++ {
		minVal := calib.Mins[j]
		scale := calib.Scales[j]

		val := (src[j] - minVal) * scale
		if val < 0 {
			val = 0
		} else if val > 255.0 {
			val = 255.0
		}
		dst[j] = byte(val + 0.5) // rounded
	}
}

// DequantizeSQ8Adaptive reconstructs a float32 vector from uint8 using per-channel calibration.
func DequantizeSQ8Adaptive(src []byte, dst []float32, calib *SQ8ChannelCalibration) {
	dims := calib.Dims
	if len(src) < dims {
		dims = len(src)
	}

	for j := 0; j < dims; j++ {
		minVal := calib.Mins[j]
		scale := calib.Scales[j]
		if scale > 0 {
			dst[j] = minVal + float32(src[j])/scale
		} else {
			dst[j] = minVal
		}
	}
}

// EuclideanDistanceSQ8Adaptive computes channel-scale normalized squared distance.
func EuclideanDistanceSQ8Adaptive(a, b []byte, calib *SQ8ChannelCalibration) float32 {
	dims := calib.Dims
	if len(a) < dims {
		dims = len(a)
	}
	if len(b) < dims {
		dims = len(b)
	}

	var totalDist float32
	for j := 0; j < dims; j++ {
		diff := float32(int32(a[j]) - int32(b[j]))
		totalDist += diff * diff * calib.InvSq[j]
	}
	return totalDist
}
