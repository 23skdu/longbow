//go:build ignore

package main
import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow"
)

func main() {
	b := array.NewFixedSizeListBuilder(memory.DefaultAllocator, 2, arrow.PrimitiveTypes.Int8)
	vb := b.ValueBuilder().(*array.Int8Builder)
	b.Append(true)
	vb.AppendValues([]int8{1, 2}, nil)
	
	arr := b.NewArray().(*array.FixedSizeList)
	values := arr.ListValues()
	
	fmt.Printf("Type: %T\n", values)
}
