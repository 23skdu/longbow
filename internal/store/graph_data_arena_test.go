package store

import (
	"testing"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

func TestGraphData_ArenaNeighbors(t *testing.T) {
	// Initialize GraphData with Arena enabled
	// Since we are refactoring, we might need a flag or just defaults.
	// NewGraphData signature: func NewGraphData(capacity, dims int, sq8, pq, bq bool, false, false, VectorTypeFloat32, false, false, false, 8) *GraphData
	// The new signature is likely: func NewGraphData(capacity, dims int, sq8, pq bool, pqDims int, bq bool, false, false, VectorTypeFloat32, false, false, false, 8) *GraphData
	initialCapacity := 100
	dims := 128
	data := lbtypes.NewGraphData(initialCapacity, dims, false, false, 0, false, false, false, lbtypes.VectorTypeFloat32, false, false, false, 8)

	// Simulate node allocation
	// We need to ensure chunks are allocated for ID 0
	_ = data.EnsureChunk(0, 0, 128)

	// Simulate adding neighbors for Node 0 at Layer 0
	// We expect a new method or modified usage.
	// Let's assume we add a helper 'AllocNeighbors' or 'SetNeighbors' that uses arena internally.
	// For this test to compile before refactoring, we might need to cast or use what's available.
	// BUT, as per TDD, we write the test for the NEW API we want.

	// Proposed API:
	// gd.SetNeighbors(layer, nodeID, neighbors []uint32)
	// neighbors := gd.GetNeighbors(layer, nodeID)

	layer := 0
	nodeID := uint32(0)
	targets := []uint32{1, 2, 3, 5, 8}

	// This method doesn't exist yet, it will fail compilation if run.
	// That's fine for TDD step 1.
	_ = data.SetNeighbors(nodeID, targets)

	readBack := data.GetNeighbors(layer, nodeID, nil)
	if len(readBack) != len(targets) {
		t.Fatalf("Expected len %d, got %d", len(targets), len(readBack))
	}

	for i, v := range readBack {
		if v != targets[i] {
			t.Errorf("Idx %d: expected %d, got %d", i, targets[i], v)
		}
	}
}

func TestGraphData_ArenaGrowth(t *testing.T) {
	// Verify that we can store widespread IDs (triggering multiple chunks/slabs)
	gd := lbtypes.NewGraphData(10000, 16, false, false, 0, false, false, false, lbtypes.VectorTypeFloat32, false, false, false, 8)

	// Add neighbors for node 5000
	id := uint32(5000)
	_ = gd.EnsureChunk(int(id/1024), int(id%1024), 16)

	data := []uint32{100, 200, 300}
	_ = gd.SetNeighbors(id, data)

	res := gd.GetNeighbors(0, id, nil)
	if len(res) != 3 || res[1] != 200 {
		t.Error("Failed to retrieve neighbors for high ID")
	}
}

func TestGraphData_PreAllocate(t *testing.T) {
	// Test PreAllocate method for different data types
	testCases := []struct {
		name     string
		capacity int
		dims     int
		dataType lbtypes.VectorDataType
	}{
		{"Float32_1M", 1000000, 128, lbtypes.VectorTypeFloat32},
		{"Float32_100K", 100000, 256, lbtypes.VectorTypeFloat32},
		{"Float64_10K", 10000, 64, lbtypes.VectorTypeFloat64},
		{"Int8_50K", 50000, 384, lbtypes.VectorTypeInt8},
		{"Int16_25K", 25000, 192, lbtypes.VectorTypeInt16},
		{"Int32_20K", 20000, 96, lbtypes.VectorTypeInt32},
		{"Int64_15K", 15000, 48, lbtypes.VectorTypeInt64},
		{"Uint8_40K", 40000, 256, lbtypes.VectorTypeUint8},
		{"Uint16_30K", 30000, 128, lbtypes.VectorTypeUint16},
		{"Uint32_20K", 20000, 64, lbtypes.VectorTypeUint32},
		{"Uint64_10K", 10000, 32, lbtypes.VectorTypeUint64},
		{"Float16_50K", 50000, 128, lbtypes.VectorTypeFloat16},
		{"Complex64_10K", 10000, 32, lbtypes.VectorTypeComplex64},
		{"Complex128_5K", 5000, 16, lbtypes.VectorTypeComplex128},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gd := lbtypes.NewGraphData(0, tc.dims, false, false, 0, false, false, false, tc.dataType, false, false, false, 8)

			err := gd.PreAllocate(tc.capacity)
			if err != nil {
				t.Fatalf("PreAllocate failed: %v", err)
			}

			if gd.Capacity != tc.capacity {
				t.Errorf("Expected capacity %d, got %d", tc.capacity, gd.Capacity)
			}

			// Verify chunks were pre-allocated based on type
			numChunks := (tc.capacity + 1024 - 1) / 1024

			switch tc.dataType {
			case lbtypes.VectorTypeFloat32:
				if len(gd.VectorsF32) != numChunks {
					t.Errorf("Expected %d Float32 chunks, got %d", numChunks, len(gd.VectorsF32))
				}
			case lbtypes.VectorTypeFloat64:
				if len(gd.VectorsFloat64Offsets) != numChunks {
					t.Errorf("Expected %d Float64 chunks, got %d", numChunks, len(gd.VectorsFloat64Offsets))
				}
			case lbtypes.VectorTypeInt8, lbtypes.VectorTypeUint8:
				if len(gd.VectorsInt8) != numChunks {
					t.Errorf("Expected %d Int8 chunks, got %d", numChunks, len(gd.VectorsInt8))
				}
			case lbtypes.VectorTypeInt16:
				if len(gd.VectorsInt16) != numChunks {
					t.Errorf("Expected %d Int16 chunks, got %d", numChunks, len(gd.VectorsInt16))
				}
			case lbtypes.VectorTypeUint16:
				if len(gd.VectorsUint16) != numChunks {
					t.Errorf("Expected %d Uint16 chunks, got %d", numChunks, len(gd.VectorsUint16))
				}
			case lbtypes.VectorTypeInt32:
				if len(gd.VectorsInt32) != numChunks {
					t.Errorf("Expected %d Int32 chunks, got %d", numChunks, len(gd.VectorsInt32))
				}
			case lbtypes.VectorTypeUint32:
				if len(gd.VectorsUint32) != numChunks {
					t.Errorf("Expected %d Uint32 chunks, got %d", numChunks, len(gd.VectorsUint32))
				}
			case lbtypes.VectorTypeInt64:
				if len(gd.VectorsInt64) != numChunks {
					t.Errorf("Expected %d Int64 chunks, got %d", numChunks, len(gd.VectorsInt64))
				}
			case lbtypes.VectorTypeUint64:
				if len(gd.VectorsUint64) != numChunks {
					t.Errorf("Expected %d Uint64 chunks, got %d", numChunks, len(gd.VectorsUint64))
				}
			case lbtypes.VectorTypeFloat16:
				if len(gd.VectorsF16) != numChunks {
					t.Errorf("Expected %d Float16 chunks, got %d", numChunks, len(gd.VectorsF16))
				}
			case lbtypes.VectorTypeComplex64:
				if len(gd.VectorsComplex64Offsets) != numChunks {
					t.Errorf("Expected %d Complex64 chunks, got %d", numChunks, len(gd.VectorsComplex64Offsets))
				}
			case lbtypes.VectorTypeComplex128:
				if len(gd.VectorsComplex128Offsets) != numChunks {
					t.Errorf("Expected %d Complex128 chunks, got %d", numChunks, len(gd.VectorsComplex128Offsets))
				}
			}

			// Verify Levels were pre-allocated
			if len(gd.Levels) != numChunks {
				t.Errorf("Expected %d Level chunks, got %d", numChunks, len(gd.Levels))
			}

			// Verify Neighbors/Counts/Versions for all layers
			for layer := 0; layer < lbtypes.ArrowMaxLayers; layer++ {
				if len(gd.Neighbors) <= layer || len(gd.Neighbors[layer]) != numChunks {
					t.Errorf("Layer %d: expected %d neighbor chunks, got %d", layer, numChunks, len(gd.Neighbors[layer]))
				}
				if len(gd.Counts) <= layer || len(gd.Counts[layer]) != numChunks {
					t.Errorf("Layer %d: expected %d count chunks, got %d", layer, numChunks, len(gd.Counts[layer]))
				}
				if len(gd.Versions) <= layer || len(gd.Versions[layer]) != numChunks {
					t.Errorf("Layer %d: expected %d version chunks, got %d", layer, numChunks, len(gd.Versions[layer]))
				}
			}
		})
	}
}

func TestGraphData_PreAllocate_SQ8(t *testing.T) {
	gd := lbtypes.NewGraphData(0, 128, false, false, 0, false, true, false, lbtypes.VectorTypeFloat32, false, false, false, 8)
	gd.SQ8Enabled = true

	err := gd.PreAllocate(10000)
	if err != nil {
		t.Fatalf("PreAllocate failed: %v", err)
	}

	numChunks := (10000 + 1024 - 1) / 1024
	if len(gd.VectorsSQ8) != numChunks {
		t.Errorf("Expected %d SQ8 chunks, got %d", numChunks, len(gd.VectorsSQ8))
	}
}

func TestGraphData_PreAllocate_PQ(t *testing.T) {
	gd := lbtypes.NewGraphData(0, 128, false, false, 0, true, false, false, lbtypes.VectorTypeFloat32, false, true, false, 8)
	gd.PQM = 64

	err := gd.PreAllocate(10000)
	if err != nil {
		t.Fatalf("PreAllocate failed: %v", err)
	}

	numChunks := (10000 + 1024 - 1) / 1024
	if len(gd.VectorsPQ) != numChunks {
		t.Errorf("Expected %d PQ chunks, got %d", numChunks, len(gd.VectorsPQ))
	}
}

func TestGraphData_PreAllocate_BQ(t *testing.T) {
	gd := lbtypes.NewGraphData(0, 128, false, false, 0, false, false, false, lbtypes.VectorTypeFloat32, true, false, false, 8)

	err := gd.PreAllocate(10000)
	if err != nil {
		t.Fatalf("PreAllocate failed: %v", err)
	}

	numChunks := (10000 + 1024 - 1) / 1024
	if len(gd.VectorsBQ) != numChunks {
		t.Errorf("Expected %d BQ chunks, got %d", numChunks, len(gd.VectorsBQ))
	}
}

func TestGraphData_PreAllocate_ZeroCapacity(t *testing.T) {
	gd := lbtypes.NewGraphData(0, 128, false, false, 0, false, false, false, lbtypes.VectorTypeFloat32, false, false, false, 8)

	err := gd.PreAllocate(0)
	if err != nil {
		t.Fatalf("PreAllocate(0) failed: %v", err)
	}

	err = gd.PreAllocate(-1)
	if err != nil {
		t.Fatalf("PreAllocate(-1) failed: %v", err)
	}
}

func TestGraphData_NewGraphData_AutoPreAllocate(t *testing.T) {
	// Verify NewGraphData automatically pre-allocates when capacity > 0
	gd := lbtypes.NewGraphData(5000, 64, false, false, 0, false, false, false, lbtypes.VectorTypeFloat32, false, false, false, 8)

	// NewGraphData creates initial chunks + calls PreAllocate
	// So we should have at least numChunks pre-allocated
	numChunks := (5000 + 1024 - 1) / 1024

	// Should have pre-allocated chunks (at least numChunks, possibly more due to initialization)
	if len(gd.VectorsF32) < numChunks {
		t.Errorf("Expected at least %d pre-allocated chunks, got %d", numChunks, len(gd.VectorsF32))
	}

	if len(gd.Levels) < numChunks {
		t.Errorf("Expected at least %d pre-allocated level chunks, got %d", numChunks, len(gd.Levels))
	}

	// Verify data can be written to pre-allocated chunks
	id := uint32(100)
	chunkID := int(id) / 1024
	offset := int(id) % 1024

	chunk := gd.GetVectorsChunk(chunkID)
	if chunk == nil {
		t.Fatal("Failed to get pre-allocated chunk")
	}

	// Write test data
	testVec := make([]float32, 64)
	for i := range testVec {
		testVec[i] = float32(i)
	}

	start := offset * 64
	copy(chunk[start:start+64], testVec)

	// Verify data was written
	for i := 0; i < 64; i++ {
		if chunk[start+i] != float32(i) {
			t.Errorf("Data mismatch at index %d: expected %v, got %v", i, float32(i), chunk[start+i])
		}
	}
}
