with open('internal/store/store.go', 'r') as f:
    content = f.read()

content = content.replace('func (vs *VectorStore) updateDatasets', 'func (vs *VectorStore) UpdateDatasets')
content = content.replace('vs.updateDatasets', 'vs.UpdateDatasets')

with open('internal/store/store.go', 'w') as f:
    f.write(content)

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

content = content.replace('datasetsPtr = vs.datasets.Load()', 'ds, _ = vs.GetDataset("reset_test_ds")')
content = content.replace('require.NotNil(t, ds)\n\tassert.NotContains(t, *datasetsPtr, "reset_test_ds")', 'assert.Nil(t, ds)')
# Also check if datasetsPtr was used around line 611 and 613
# It was "undefined: datasetsPtr" because it was using "=" instead of ":=" if the previous := was replaced
# Let's just make sure "datasetsPtr" is completely gone.

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
