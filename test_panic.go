package main

import (
	"fmt"
	"github.com/23skdu/longbow/internal/store/types"
)

func main() {
	gd := types.NewGraphData(100, 128, true, true, 16, true, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)
	err := gd.EnsureChunks(10, 128)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Uint64Arena:", gd.Uint64Arena)
}
