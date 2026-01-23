package simd

import (
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// euclideanF16BatchGeneric is the baseline implementation for Float16 batch distance.
func euclideanF16BatchGeneric(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	for i, v := range vectors {
		if v == nil {
			continue
		}
		d, err := euclideanF16Unrolled4x(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

// Note: euclideanSQ8BatchGeneric is already in simd.go but we could move it here later for consistency.
