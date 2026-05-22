package main

import (
	"fmt"
	"github.com/23skdu/longbow/internal/store/types"
)

func main() {
	gd := types.NewGraphData(100, 128, false, false, 0, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)
	gd.SQ8Enabled = true
	gd.PQEnabled = true
	gd.PQM = 16
	gd.BQEnabled = true
	gd.TurboQuantEnabled = true
	gd.TurboQuantBits = 8
	
    fmt.Println("SharedVectorSpace:", gd.SharedVectorSpace)
    fmt.Println("PQEnabled:", gd.PQEnabled)
    fmt.Println("len before:", len(gd.VectorsPQ))
	err := gd.EnsureChunks(10, 128)
	fmt.Println("err:", err)
    fmt.Println("len after:", len(gd.VectorsPQ))
}
