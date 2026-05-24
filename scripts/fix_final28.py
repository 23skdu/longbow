with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

content = content.replace('Newstore.VectorStore(', 'store.NewVectorStore(')
content = content.replace('datasetsPtr := vs.datasets.Load()', 'ds, _ := vs.GetDataset("reset_test_ds")')
content = content.replace('require.NotNil(t, datasetsPtr)', 'require.NotNil(t, ds)')
content = content.replace('require.Contains(t, *datasetsPtr, "reset_test_ds")', 'require.Equal(t, "reset_test_ds", ds.Name)')
content = content.replace('vs.updatestore.Datasets', 'vs.UpdateDatasets')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)

with open('internal/store/store.go', 'r') as f:
    content = f.read()
content = content.replace('func (s *VectorStore) updateDatasets', 'func (s *VectorStore) UpdateDatasets')
with open('internal/store/store.go', 'w') as f:
    f.write(content)

