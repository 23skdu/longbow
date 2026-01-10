package pq

import (
	"testing"
)

func FuzzPQEncoder_TrainAndEncode(f *testing.F) {
	// Add some seeds
	f.Add(32, 4, 16, 100) // dims, M, K, numSamples
	f.Add(64, 8, 256, 50)
	f.Add(128, 16, 256, 200)

	f.Fuzz(func(t *testing.T, dims, M, K, numSamples int) {
		// Validations to keep it within reasonable bounds for fuzzing
		if dims <= 0 || M <= 0 || dims%M != 0 || K <= 0 || K > 256 || numSamples < K*2 {
			return
		}
		if dims > 1024 || M > 128 || numSamples > 2000 {
			return
		}

		// 1. Generate Training Data
		data := make([][]float32, numSamples)
		for i := range data {
			vec := make([]float32, dims)
			for j := range vec {
				vec[j] = float32(i+j) / float32(numSamples) // Deterministic-ish
			}
			data[i] = vec
		}

		// 2. Train Encoder
		encoder, err := NewPQEncoder(dims, M, K)
		if err != nil {
			return
		}

		err = encoder.Train(data)
		if err != nil {
			t.Errorf("Train failed: %v", err)
			return
		}

		// 3. Encode & Decode
		for i := 0; i < 5 && i < numSamples; i++ {
			original := data[i]
			code, err := encoder.Encode(original)
			if err != nil {
				t.Errorf("Encode failed: %v", err)
				return
			}
			if len(code) != M {
				t.Errorf("Code length mismatch: expected %d, got %d", M, len(code))
			}

			reconstructed, err := encoder.Decode(code)
			if err != nil {
				t.Errorf("Decode failed: %v", err)
				return
			}
			if len(reconstructed) != dims {
				t.Errorf("Reconstructed length mismatch: expected %d, got %d", dims, len(reconstructed))
			}
		}

		// 4. ADC Verification
		query := make([]float32, dims)
		for j := range query {
			query[j] = 0.5
		}

		table, err := encoder.BuildADCTable(query)
		if err != nil {
			t.Errorf("BuildADCTable failed: %v", err)
			return
		}

		targetCode, _ := encoder.Encode(data[0])
		adcDist, err := encoder.ADCDistance(table, targetCode)
		if err != nil {
			t.Errorf("ADCDistance failed: %v", err)
			return
		}

		// Decoded L2 check
		decoded, _ := encoder.Decode(targetCode)
		var l2 float32
		for j := range query {
			d := query[j] - decoded[j]
			l2 += d * d
		}

		if mathAbs(adcDist-l2) > 0.1 {
			t.Errorf("ADC distance mismatch: expected %f, got %f", l2, adcDist)
		}
	})
}

func mathAbs(f float32) float32 {
	if f < 0 {
		return -f
	}
	return f
}
