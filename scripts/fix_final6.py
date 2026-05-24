import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Fix putDataViastore
content = content.replace('putDataViastore.VectorStore', 'putDataViaVectorStore')

# Fix unexported access in TestResetDatasetAction
content = content.replace('''	// Ensure the dataset exists
	datasetsPtr := vs.datasets.Load()
	require.NotNil(t, datasetsPtr)
	require.Contains(t, *datasetsPtr, "reset_test_ds")''', '''	// Ensure the dataset exists
	_, err := vs.GetDataset("reset_test_ds")
	require.NoError(t, err)''')

content = content.replace('''	// Verify the dataset is dropped
	datasetsPtr = vs.datasets.Load()
	require.NotNil(t, datasetsPtr)
	assert.NotContains(t, *datasetsPtr, "reset_test_ds")''', '''	// Verify the dataset is dropped
	_, err = vs.GetDataset("reset_test_ds")
	require.Error(t, err)''')

# Also fix the unexported store.NewLockFreeSliceFrom
content = content.replace('store.NewLockFreeSliceFrom', 'store.NewLockFreeSliceFrom') # Oh wait, NewLockFreeSliceFrom is exported, but wait, `Dataset` struct creation
# Wait! In putDataViaVectorStore:
# 	vs.updateDatasets(func(m map[string]*store.Dataset) {
# vs.updateDatasets is unexported!
# So we can't use putDataViaVectorStore!
