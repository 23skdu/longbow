package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	filePath := "internal/store/internal/core/neighbor_ops.go"
	b, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	s := string(b)

	// Fix AddConnection
	old1 := "if countsChunk == nil || neighborsChunk == nil {\n\t\t// Reload data and try again\n\t\tdata = h.data.Load()\n\t\tdata = h.promoteNode(data, source)\n\t\tcountsChunk = data.GetCountsChunk(layer, cID)\n\t\tneighborsChunk = data.GetNeighborsChunk(layer, cID)\n\t\tif countsChunk == nil || neighborsChunk == nil {\n\t\t\treturn\n\t\t}\n\t}"
	new1 := "if countsChunk == nil || neighborsChunk == nil {\n\t\t// Reload data and try again\n\t\tdata = h.data.Load()\n\t\tdata = h.promoteNode(data, source)\n\t\tcountsChunk = data.GetCountsChunk(layer, cID)\n\t\tneighborsChunk = data.GetNeighborsChunk(layer, cID)\n\t\tif countsChunk == nil || neighborsChunk == nil {\n\t\t\tfmt.Printf(\"Warning: AddConnection failed - chunk for %d at layer %d not initialized\\n\", source, layer)\n\t\t\treturn\n\t\t}\n\t}"
	
	if strings.Contains(s, old1) {
		s = strings.Replace(s, old1, new1, 1)
	} else {
		fmt.Println("Warning: Could not find AddConnection block (maybe already updated or whitespace mismatch)")
	}

	// Fix AddConnectionsBatch
	old2 := "if countsChunk == nil || neighborsChunk == nil {\n\t\tdata = h.data.Load()\n\t\tcountsChunk = data.GetCountsChunk(layer, cID)\n\t\tneighborsChunk = data.GetNeighborsChunk(layer, cID)\n\t\tif countsChunk == nil || neighborsChunk == nil {\n\t\t\treturn\n\t\t}\n\t}"
	new2 := "if countsChunk == nil || neighborsChunk == nil {\n\t\tdata = h.data.Load()\n\t\tcountsChunk = data.GetCountsChunk(layer, cID)\n\t\tneighborsChunk = data.GetNeighborsChunk(layer, cID)\n\t\tif countsChunk == nil || neighborsChunk == nil {\n\t\t\tfmt.Printf(\"Warning: AddConnectionsBatch failed - chunk for target %d at layer %d not initialized\\n\", target, layer)\n\t\t\treturn\n\t\t}\n\t}"

	if strings.Contains(s, old2) {
		s = strings.Replace(s, old2, new2, 1)
	} else {
		fmt.Println("Warning: Could not find AddConnectionsBatch block (maybe already updated or whitespace mismatch)")
	}

	err = os.WriteFile(filePath, []byte(s), 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Successfully updated neighbor_ops.go")
}
