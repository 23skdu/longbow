package store

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphPersistence_SnapshotRecovery(t *testing.T) {
	// 1. Setup
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()
	logger := mockLogger()
	store := NewVectorStore(mem, logger, 1024, 0, time.Hour)

	// Init Persistence
	err := store.InitPersistence(storage.StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 1 * time.Hour,
	})
	require.NoError(t, err)

	// 2. Create Dataset and Add Graph Edge
	datasetName := "test_graph"

	ds := NewDataset(datasetName, nil) // Schema nil for now, pure graph?
	store.updateDatasets(func(m map[string]*Dataset) {
		m[datasetName] = ds
	})

	// Add Edge to Graph
	edge := Edge{
		Subject:   VectorID(1),
		Predicate: "connects",
		Object:    VectorID(2),
		Weight:    1.0,
	}
	err = ds.Graph.AddEdge(edge)
	require.NoError(t, err)

	assert.Equal(t, 1, ds.Graph.EdgeCount())

	// 3. Snapshot
	err = store.Snapshot()
	require.NoError(t, err)

	// 4. Close
	err = store.Close()
	require.NoError(t, err)

	// 5. Reopen
	store2 := NewVectorStore(mem, logger, 1024, 0, time.Hour)
	err = store2.InitPersistence(storage.StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 1 * time.Hour,
	})
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()

	// 6. Verify
	ds2, ok := store2.getDataset(datasetName)

	require.True(t, ok, "Dataset should exist after recovery")
	require.NotNil(t, ds2.Graph, "GraphStore should be initialized")
	assert.Equal(t, 1, ds2.Graph.EdgeCount(), "Graph should have 1 edge recovered")

	edges := ds2.Graph.GetEdgesBySubject(VectorID(1))
	require.Len(t, edges, 1)
	assert.Equal(t, VectorID(2), edges[0].Object)
}
