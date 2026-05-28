//go:build ignore

package main
import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow"
)

func main() {
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "f1", Type: arrow.PrimitiveTypes.Int32},
	}, nil))
	defer b.Release()
	rec := b.NewRecord()
	defer rec.Release()

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Panicked:", r)
		}
	}()

	fmt.Println("Getting col -1")
	col := rec.Column(-1)
	fmt.Println("Got col:", col)
}
