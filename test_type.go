package main

import (
	"fmt"
	"github.com/23skdu/longbow/internal/store/index"
)

type VectorIndex interface {
	Len() int
}

type ShardedHNSW = index.ShardedHNSW

func main() {
	var idx VectorIndex = &index.ShardedHNSW{}
	
	if _, ok := idx.(*ShardedHNSW); ok {
		fmt.Println("OK ShardedHNSW")
	} else {
		fmt.Println("NOT OK")
	}
}
