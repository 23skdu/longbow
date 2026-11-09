//go:build !gpu || !linux || !cuda

package cuvs

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// decodeFuzzFloat32s turns a fuzz byte blob into float32 query values.
func decodeFuzzFloat32s(b []byte) []float32 {
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return out
}

func encodeFuzzFloat32s(vals []float32) []byte {
	b := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	return b
}

const (
	stubNewErr    = "cuVS index not supported on this platform: build with -tags gpu,linux,cuda"
	stubMethodErr = "cuVS index not supported on this platform"
)

func FuzzCUVSStubContract(f *testing.F) {
	f.Add("bert-base", 128, encodeFuzzFloat32s([]float32{0.1, 0.2, 0.3}), 5)
	f.Add("", -1, []byte{}, 0)                              // invalid dim and empty query
	f.Add("model", 0, encodeFuzzFloat32s([]float32{1}), -3) // invalid k
	f.Add("path/to/model with spaces", 4096, encodeFuzzFloat32s([]float32{1, 2, 3, 4}), 1<<40)
	f.Add("x", 7, encodeFuzzFloat32s([]float32{math.MaxFloat32, math.SmallestNonzeroFloat32}), 1)
	f.Add("utf8-\u00e9\u4e2d", 1, encodeFuzzFloat32s([]float32{}), 2)

	f.Fuzz(func(t *testing.T, dataset string, dim int, queryRaw []byte, k int) {
		// Bound inputs; larger queries are out of range and dropped.
		if len(queryRaw) > 4096*4 || len(dataset) > 1024 {
			return
		}
		query := decodeFuzzFloat32s(queryRaw)
		ctx := context.Background()

		// The stub constructor must deterministically refuse to build an
		// index on this platform, no matter the arguments.
		idx1, err1 := NewCUVSIndex(dataset, dim)
		idx2, err2 := NewCUVSIndex(dataset, dim)
		require.Nil(t, idx1)
		require.Nil(t, idx2)
		require.EqualError(t, err1, stubNewErr)
		require.Equal(t, err1.Error(), err2.Error(), "constructor error must be deterministic")

		// A directly constructed stub index must reject every operation
		// with a deterministic error and never touch the inputs.
		c := &CUVSIndex{dataset: dataset, dim: dim}
		queryCopy := make([]float32, len(query))
		copy(queryCopy, query)

		ids1, dists1, err1 := c.Search(ctx, query, k)
		ids2, dists2, err2 := c.Search(ctx, query, k)
		require.Nil(t, ids1)
		require.Nil(t, dists1)
		require.Nil(t, ids2)
		require.Nil(t, dists2)
		require.EqualError(t, err1, stubMethodErr)
		require.EqualError(t, err2, stubMethodErr)

		require.EqualError(t, c.AddBatch(ctx, []int64{1, 2}, query), stubMethodErr)

		// Close must stay a nil-op, even repeated.
		require.NoError(t, c.Close())
		require.NoError(t, c.Close())

		// The stub must never mutate caller buffers.
		require.Equal(t, queryCopy, query)
	})
}
