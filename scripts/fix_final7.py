import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Fix putDataViaVectorStore
content = content.replace('vs.updateDatasets(func(m map[string]*store.Dataset)', 'vs.UpdateDatasets(func(m map[string]*store.Dataset)')

# Fix TestResetDatasetAction checks
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

# Fix WaitForIndexing in TestDataServerDoGet
old_wait = '''	// Wait for async ingestion
	if ds, err := vs.GetDataset("ds_get_test"); err == nil {
		ds.WaitForIndexing()
	}'''
new_wait = '''	// Wait for async ingestion
	var ds *store.Dataset
	vs.IterateDatasets(func(n string, d *store.Dataset) {
		if n == "ds_get_test" {
			ds = d
		}
	})
	if ds != nil {
		ds.WaitForIndexing()
	}'''
content = content.replace(old_wait, new_wait)

# Fix Dataset reference in putDataViaVectorStore
content = content.replace('m[name] = &Dataset{', 'm[name] = &store.Dataset{')

# Write back
with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
