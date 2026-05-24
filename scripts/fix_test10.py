import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# We need to find the `func TestMetaServerDoPutUnimplemented(t *testing.T) {`
# And we need to find `	if st.Code() != codes.NotFound {\n		t.Errorf("Expected NotFound, got %v", st.Code())\n	}\n}` which is the end of TestDataServerDoGetNotFound.

start_str = '''	if st.Code() != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", st.Code())
	}
}
'''

end_str = 'func TestMetaServerDoPutUnimplemented(t *testing.T) {'

idx1 = content.find(start_str)
if idx1 != -1:
    idx1 += len(start_str)
    idx2 = content.find(end_str, idx1)
    if idx2 != -1:
        content = content[:idx1] + '\n' + content[idx2:]

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
