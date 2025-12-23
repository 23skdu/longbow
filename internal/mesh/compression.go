package mesh

import (
	"github.com/golang/snappy"
)

// CompressPayload compresses bytes using Snappy.
// Returns the original slice if compression doesn't save space or fails.
func CompressPayload(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	// Encode returns a new slice
	compressed := snappy.Encode(nil, data)
	if len(compressed) >= len(data) {
		return data
	}
	return compressed
}

// DecompressPayload decompresses Snappy compressed bytes.
func DecompressPayload(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	return snappy.Decode(nil, data)
}
