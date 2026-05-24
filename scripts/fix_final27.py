with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

content = content.replace('Newstore(', 'store.NewVectorStore(')
content = content.replace('vs.updatestore.Datasets(', 'vs.UpdateDatasets(')
content = content.replace('Resetstore.Dataset', 'ResetDataset')
# For vs.datasets check
content = content.replace('datasetsPtr := vs.datasets.Load()', 'ds, _ := vs.GetDataset("reset_test_ds")')
content = content.replace('require.NotNil(t, datasetsPtr)', 'require.NotNil(t, ds)')
content = content.replace('require.Contains(t, *datasetsPtr, "reset_test_ds")', 'require.Equal(t, "reset_test_ds", ds.Name)')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
