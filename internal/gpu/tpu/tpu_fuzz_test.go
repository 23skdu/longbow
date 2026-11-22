package tpu

import (
	"testing"
)

// FuzzTPUEnqueue runs the fuzz tpu enqueue operation.
func FuzzTPUEnqueue(f *testing.F) {
	f.Add(10)
	f.Fuzz(func(t *testing.T, size int) {
		if size < 0 || size > 1000000 {
			return
		}
		data := make([]float32, size)
		for i := range data {
			data[i] = 1.0
		}
		_ = tpuEnqueueBatch(0, data)
	})
}
