package main

import (
	"fmt"
	"github.com/23skdu/longbow/internal/store/types"
	"sync/atomic"
)

func main() {
	gd := types.NewGraphData(100, 128, false, false, 0, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)
	gd.SQ8Enabled = true
	gd.PQEnabled = true
	gd.PQM = 16
	gd.BQEnabled = true
	gd.TurboQuantEnabled = true
	gd.TurboQuantBits = 8
	
	err := gd.EnsureChunks(10, 128)
	fmt.Println("err:", err)
	fmt.Println("Uint64Arena:", gd.Uint64Arena)
	
	if gd.Uint64Arena == nil {
	    // manually simulate EnsureChunk
	    cID := 0
	    dims := 128
        numWordsPerNode := (gd.PQM + 7) / 8
        fmt.Println("len(VectorsPQ):", len(gd.VectorsPQ), "val:", atomic.LoadUint64(&gd.VectorsPQ[cID]))
        cond1 := cID < len(gd.VectorsPQ)
        cond2 := atomic.LoadUint64(&gd.VectorsPQ[cID]) == 0
        cond3 := dims > 0
        cond4 := numWordsPerNode > 0
        fmt.Println("cond1:", cond1, "cond2:", cond2, "cond3:", cond3, "cond4:", cond4)
	}
}
