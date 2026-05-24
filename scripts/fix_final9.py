import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Fix duplicated storage import
content = content.replace('"github.com/23skdu/longbow/internal/storage"\n\t"github.com/23skdu/longbow/internal/storage"', '"github.com/23skdu/longbow/internal/storage"')

# Fix time not used (by adding it ONLY once and verifying we replaced time.Sleep)
# Wait, why was time.Sleep not replacing?
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

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
