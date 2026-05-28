//go:build ignore

package main
import "fmt"

func main() {
	var a []int8 = []int8{1,2,3}
	var b []int8 = a[3:3] // empty slice
	var c any = b
	fmt.Printf("b == nil: %v, c == nil: %v, len: %d\n", b == nil, c == nil, len(b))
	
	var x []int8
	var y any = x
	fmt.Printf("x == nil: %v, y == nil: %v\n", x == nil, y == nil)
}
