package query

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkStringFilterOp runs the benchmark string filter op operation.
func BenchmarkStringFilterOp(b *testing.B) {
	bldr := array.NewStringBuilder(memory.NewGoAllocator())
	defer bldr.Release()
	vals := []string{"alpha", "beta", "gamma", "delta"}
	for i := 0; i < 4096; i++ {
		bldr.Append(vals[i%len(vals)])
	}
	col := bldr.NewStringArray()
	defer col.Release()

	op := &stringFilterOp{
		operator: "eq",
		val:      "gamma",
	}
	if err := op.Bind(col); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		matches := 0
		for row := 0; row < col.Len(); row++ {
			if op.Match(row) {
				matches++
			}
		}
		if matches != 1024 {
			b.Fatalf("unexpected matches %d", matches)
		}
	}
}
