import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Add WaitForIndexing in TestDataServerDoGet
target = '''	_ = w.Close()
	_ = stream.CloseSend()
	_, _ = stream.Recv()

	// Now get data - ticket uses JSON format'''

replacement = '''	_ = w.Close()
	_ = stream.CloseSend()
	_, _ = stream.Recv()

	// Wait for async ingestion
	if ds, ok := vs.GetDataset("ds_get_test"); ok {
		ds.WaitForIndexing()
	}

	// Now get data - ticket uses JSON format'''

content = content.replace(target, replacement)

# Fix syntax errors with _, err = stream.Recv() from earlier sed
content = content.replace('_, err = stream.Recv(); if err != nil && err.Error() != "EOF" { t.Fatalf("Recv failed: %v", err) }', '_, _ = stream.Recv()')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
