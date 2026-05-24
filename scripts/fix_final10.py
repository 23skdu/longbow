with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Fix setupDataServerTest return
content = content.replace('func setupDataServerTest(t *testing.T) flight.Client {', 'func setupDataServerTest(t *testing.T) (flight.Client, *store.VectorStore) {')
content = content.replace('	return client\n}', '	return client, vs\n}')

# Fix callers of setupDataServerTest
content = content.replace('client := setupDataServerTest(t)', 'client, vs := setupDataServerTest(t)')

# Fix TestDataServerDoGet wait loop (which requires time package!)
old_wait = '''	_ = stream.CloseSend()
	_, _ = stream.Recv()

	// Now get data - ticket uses JSON format'''
new_wait = '''	_ = stream.CloseSend()
	_, _ = stream.Recv()

	// Wait for async ingestion
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
	}

	// Now get data - ticket uses JSON format'''
content = content.replace(old_wait, new_wait)

# Add "time" import
content = content.replace('"testing"', '"testing"\n\t"time"')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
