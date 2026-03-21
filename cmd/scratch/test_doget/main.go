package main

import (
	"fmt"
	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func main() {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := store.NewVectorStore(mem, logger, 1<<30, 0, 0)
	fmt.Printf("doGetPipelinePool: %v\n", vs.GetDoGetPipelinePool())
}
