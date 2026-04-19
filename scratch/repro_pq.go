package main

import (
	"fmt"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/internal/store/internal/core"
	"github.com/23skdu/longbow/internal/pq"
)

func main() {
	dim := 128
	count := 10
	
	// Create PQ encoder
	encoder, err := pq.NewPQEncoder(dim, 16, 256)
	if err != nil {
		panic(err)
	}
	
	// Training data
	data := make([][]float32, 100)
	for i := 0; i < 100; i++ {
		data[i] = make([]float32, dim)
	}
	_ = encoder.Train(data)
	
	config := types.DefaultArrowHNSWConfig()
	config.PQEnabled = true
	config.InitialCapacity = 50000
	
	index := core.NewArrowHNSW(nil, &config)
	index.SetPQEncoder(encoder)
	
	// Try setting vector for ID 0
	code := make([]byte, 16)
	// AddByLocation triggers internal logic
	// But we can test GraphData directly
	gd := index.GetData()
	err = gd.EnsureChunk(0, 0, dim)
	if err != nil {
		fmt.Printf("EnsureChunk failed: %v\n", err)
	}
	
	err = gd.SetVectorPQ(0, code)
	if err != nil {
		fmt.Printf("SetVectorPQ 0 failed: %v\n", err)
	} else {
		fmt.Println("SetVectorPQ 0 succeeded")
	}
	
	// Try a different chunk
	err = gd.EnsureChunk(1, 0, dim)
	err = gd.SetVectorPQ(1024, code)
	if err != nil {
		fmt.Printf("SetVectorPQ 1024 failed: %v\n", err)
	} else {
		fmt.Println("SetVectorPQ 1024 succeeded")
	}
}
