import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Change package to cluster_test
content = content.replace('package store', 'package cluster_test')

# Add imports
if '"github.com/23skdu/longbow/internal/store/cluster"' not in content:
    content = content.replace('"github.com/23skdu/longbow/internal/store/types"', '"github.com/23skdu/longbow/internal/store/types"\n\t"github.com/23skdu/longbow/internal/store"\n\t"github.com/23skdu/longbow/internal/store/cluster"')

# Replace types
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

# Find exactly where TestMetaServerDoPutUnimplemented starts, and where TestDataServerListFlightsUnimplemented starts
idx1 = content.find('// TestDataServerListFlightsUnimplemented')
idx2 = content.find('// TestMetaServerDoPutUnimplemented')

if idx1 != -1 and idx2 != -1:
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
