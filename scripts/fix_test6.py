import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

target = '''	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}
	vs.StartIndexingWorkers(4)'''

replacement = '''	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}
	vs.StartIndexingWorkers(4)
	vs.StartIngestionWorkers(4)'''

content = content.replace(target, replacement)

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
