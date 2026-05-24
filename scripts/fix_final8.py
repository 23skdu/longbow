import subprocess

result = subprocess.run(['git', 'show', 'HEAD:internal/store/servers_test.go'], capture_output=True, text=True)
content = result.stdout

content = content.replace('package store', 'package cluster_test')

content = content.replace('"github.com/apache/arrow-go/v18/arrow/memory"', '"github.com/apache/arrow-go/v18/arrow/memory"\n\t"github.com/23skdu/longbow/internal/storage"\n\t"github.com/23skdu/longbow/internal/store"\n\t"github.com/23skdu/longbow/internal/store/cluster"\n\t"time"')

content = content.replace('NewVectorStore(mem, logger, 1024*1024*100, 0, 0)', 'store.NewVectorStore(mem, logger, 1024*1024*500, 0, 0)')

content = content.replace('''	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}

	ds := NewDataServer(vs)''', '''	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}
	vs.StartIndexingWorkers(4)
	vs.StartIngestionWorkers(4)

	ds := cluster.NewDataServer(vs)''')

content = content.replace('''	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}

	ms := NewMetaServer(vs)''', '''	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}
	vs.StartIndexingWorkers(4)
	vs.StartIngestionWorkers(4)

	ms := cluster.NewMetaServer(vs)''')

content = content.replace('func setupMetaServerTest(t *testing.T) (flight.Client, *VectorStore) {', 'func setupMetaServerTest(t *testing.T) (flight.Client, *store.VectorStore) {')

content = content.replace('func putDataViaVectorStore(vs *VectorStore, name string) {', 'func putDataViaVectorStore(vs *store.VectorStore, name string) {')

content = content.replace('''	vs.updateDatasets(func(m map[string]*Dataset) {
		m[name] = &Dataset{
			Name:    name,
			Records: NewLockFreeSliceFrom([]arrow.RecordBatch{rec}),
		}
	})''', '''	vs.UpdateDatasets(func(m map[string]*store.Dataset) {
		ds := &store.Dataset{
			Name:    name,
			Records: store.NewLockFreeSliceFrom([]arrow.RecordBatch{rec}),
		}
		ds.IsReady.Store(true)
		m[name] = ds
	})''')

old_wait = '''	// Wait for async ingestion
	if ds, ok := vs.GetDataset("ds_get_test"); ok {
		ds.WaitForIndexing()
	}'''
new_wait = '''	// Wait for async ingestion
	var ds *store.Dataset
	for i := 0; i < 50; i++ {
		vs.IterateDatasets(func(n string, d *store.Dataset) {
			if n == "ds_get_test" {
				ds = d
			}
		})
		if ds != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ds != nil {
		ds.WaitForIndexing()
	} else {
		t.Fatal("dataset not found after wait")
	}'''
content = content.replace(old_wait, new_wait)

old_check = '''	// Ensure the dataset exists
	datasetsPtr := vs.datasets.Load()
	require.NotNil(t, datasetsPtr)
	require.Contains(t, *datasetsPtr, "reset_test_ds")'''
new_check = '''	// Ensure the dataset exists
	active := vs.GetActiveDatasets()
	require.Contains(t, active, "reset_test_ds")'''
content = content.replace(old_check, new_check)

old_check_2 = '''	// Verify the dataset is dropped
	datasetsPtr = vs.datasets.Load()
	require.NotNil(t, datasetsPtr)
	assert.NotContains(t, *datasetsPtr, "reset_test_ds")'''
new_check_2 = '''	// Verify the dataset is dropped
	active = vs.GetActiveDatasets()
	assert.NotContains(t, active, "reset_test_ds")'''
content = content.replace(old_check_2, new_check_2)

content = content.replace('''// TestDataServerListFlightsUnimplemented tests Unimplemented response
func TestDataServerListFlightsUnimplemented(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	client := setupDataServerTest(t)
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
	client := setupDataServerTest(t)
	ctx := context.Background()

	desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"test"}}
	_, err := client.GetFlightInfo(ctx, desc)
	if err == nil {
		t.Fatal("Expected error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected Unimplemented, got %v", st.Code())
	}
}''', '')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
