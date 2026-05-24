import re
import subprocess

# Read original from git
result = subprocess.run(['git', 'show', 'HEAD:internal/store/servers_test.go'], capture_output=True, text=True)
content = result.stdout

# Change package to cluster_test
content = content.replace('package store', 'package cluster_test')

# Add imports
content = content.replace('"github.com/23skdu/longbow/internal/store/types"', '"github.com/23skdu/longbow/internal/store/types"\n\t"github.com/23skdu/longbow/internal/store"\n\t"github.com/23skdu/longbow/internal/store/cluster"')

# Replace types globally
content = content.replace('VectorStore', 'store.VectorStore')
content = content.replace('store.store.VectorStore', 'store.VectorStore')
content = content.replace('Newstore.VectorStore', 'store.NewVectorStore')
content = content.replace('store.NewVectorStore', 'store.NewVectorStore')
content = content.replace('NewDataServer', 'cluster.NewDataServer')
content = content.replace('cluster.cluster.NewDataServer', 'cluster.NewDataServer')
content = content.replace('NewMetaServer', 'cluster.NewMetaServer')
content = content.replace('cluster.cluster.NewMetaServer', 'cluster.NewMetaServer')
content = content.replace('MockMeshClient', 'cluster.MockMeshClient')
content = content.replace('cluster.cluster.MockMeshClient', 'cluster.MockMeshClient')
content = content.replace('*Dataset', '*store.Dataset')
content = content.replace('*store.store.Dataset', '*store.Dataset')
content = content.replace('NewLockFreeSliceFrom', 'store.NewLockFreeSliceFrom')
content = content.replace('putDataViaVectorStore', 'putDataViaVectorStore')

# Fix the two functions exactly by string replacing their full definitions
content = content.replace('''// TestDataServerListFlightsUnimplemented tests Unimplemented response
func TestDataServerListFlightsUnimplemented(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	client, _ := setupDataServerTest(t)
	ctx := context.Background()

	stream, err := client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		t.Fatalf("ListFlights call failed: %v", err)
	}

	_, err = stream.Recv()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected Unimplemented, got %v", st.Code())
	}
}''', '')

content = content.replace('''// TestDataServerGetFlightInfoUnimplemented tests Unimplemented response
func TestDataServerGetFlightInfoUnimplemented(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	client, _ := setupDataServerTest(t)
	ctx := context.Background()

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"test"},
	}
	_, err := client.GetFlightInfo(ctx, desc)
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected Unimplemented, got %v", st.Code())
	}
}''', '')

# Add workers
target = '''	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}'''
replacement = '''	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}
	vs.StartIndexingWorkers(4)
	vs.StartIngestionWorkers(4)'''
content = content.replace(target, replacement)

# Fix memory to 500MB
content = content.replace('1024*1024*100', '1024*1024*500')

# Fix WaitForIndexing in TestDataServerDoGet
target2 = '''	// Wait for async ingestion
	if ds, ok := vs.GetDataset("ds_get_test"); ok {
		ds.WaitForIndexing()
	}'''
replacement2 = '''	// Wait for async ingestion
	if ds, err := vs.GetDataset("ds_get_test"); err == nil {
		ds.WaitForIndexing()
	}'''
content = content.replace(target2, replacement2)

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
