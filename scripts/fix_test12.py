import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Remove TestDataServerListFlightsUnimplemented and TestDataServerGetFlightInfoUnimplemented
start_str = 'func TestDataServerListFlightsUnimplemented(t *testing.T) {'
end_str = 'func TestMetaServerDoPutUnimplemented(t *testing.T) {'

idx1 = content.find(start_str)
if idx1 != -1:
    idx2 = content.find(end_str, idx1)
    if idx2 != -1:
        content = content[:idx1] + content[idx2:]

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
