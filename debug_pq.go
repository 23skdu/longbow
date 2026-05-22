package main

import (
	"fmt"
	"github.com/23skdu/longbow/internal/store/types"
	"sync/atomic"
)

func main() {
	g := types.NewGraphData(100, 128, true, true, 16, true, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)
	g.GrowMetadataSlices(10)
	cID := 0
	dims := 128
	fmt.Println("PQEnabled:", g.PQEnabled, "PQM:", g.PQM)
	numWordsPerNode := (g.PQM + 7) / 8
	fmt.Println("numWords:", numWordsPerNode, "len(VectorsPQ):", len(g.VectorsPQ))
	if cID < len(g.VectorsPQ) && atomic.LoadUint64(&g.VectorsPQ[cID]) == 0 && dims > 0 && numWordsPerNode > 0 {
		fmt.Println("Inside if!")
	} else {
		fmt.Println("Did not enter if! cID<len:", cID < len(g.VectorsPQ), "atomic==0:", atomic.LoadUint64(&g.VectorsPQ[cID]) == 0)
	}

    // Now for BQ:
    fmt.Println("BQEnabled:", g.BQEnabled)
    paddedDims := (dims + 63) & ^63
    numWords := paddedDims / 64
    fmt.Println("numWords BQ:", numWords, "len(VectorsBQ):", len(g.VectorsBQ))
	if cID < len(g.VectorsBQ) && atomic.LoadUint64(&g.VectorsBQ[cID]) == 0 && numWords > 0 {
		fmt.Println("Inside BQ if!")
	}
}
