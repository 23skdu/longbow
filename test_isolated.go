package main

import (
	"context"
	"fmt"

	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
)

func main() {
	config := index.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeInt8
	config.Dims = 16
	idx := index.NewArrowHNSW(nil, &config, nil)
	
	batch := types.NewVectorBatch(1100, types.VectorTypeInt8, 16)
	for i := 0; i < 1100; i++ {
		v := make([]int8, 16)
		v[0] = int8(i)
		v[1] = int8(i >> 8)
		batch.AddInt8(uint32(i), v)
	}
	idx.AddBatchBulk(context.Background(), batch)
	
	v, _ := idx.GetVector(500)
	opts := types.SearchOptions{Ef: 1100}
	res, _ := idx.SearchVectors(context.Background(), v, 5, nil, opts)
	for _, r := range res {
		fmt.Printf("ID=%d Dist=%f\n", r.ID, r.Distance)
	}
}
