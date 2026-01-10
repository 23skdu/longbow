package pq

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestADC_Distance(t *testing.T) {
	dims := 32
	M := 4
	K := 256
	numSamples := 1000

	// 1. Generate Training Data
	data := make([][]float32, numSamples)
	for i := range data {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		data[i] = vec
	}

	// 2. Train Encoder
	encoder, err := NewPQEncoder(dims, M, K)
	require.NoError(t, err)
	err = encoder.Train(data)
	require.NoError(t, err)

	// 3. Encode a vector
	targetVec := data[0]
	code, err := encoder.Encode(targetVec)
	require.NoError(t, err)

	// 4. Create a query vector (can be same or different)
	queryVec := make([]float32, dims)
	for j := range queryVec {
		queryVec[j] = rand.Float32()
	}

	// 5. Build ADC Table
	table, err := encoder.BuildADCTable(queryVec)
	require.NoError(t, err)
	assert.Equal(t, M*K, len(table))

	// 6. Compute ADC Distance
	adcDist, err := encoder.ADCDistance(table, code)
	require.NoError(t, err)

	// 7. Verify against decoded distance
	// ADC approximates L2(query, decoded(code))
	decodedVec, _ := encoder.Decode(code)

	// Manual L2
	var manualDist float32
	for i := 0; i < dims; i++ {
		d := queryVec[i] - decodedVec[i]
		manualDist += d * d
	}

	// They should be exactly equal (ignoring float precision noise)
	assert.InDelta(t, manualDist, adcDist, 0.0001, "ADC distance should match L2(query, decoded)")
}
