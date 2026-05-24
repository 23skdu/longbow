import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Replace client, vs := setupDataServerTest(t) with client, _ := setupDataServerTest(t)
# globally, then revert ONLY TestDataServerDoGet
content = content.replace('client, vs := setupDataServerTest(t)', 'client, _ := setupDataServerTest(t)')

# Now restore it in TestDataServerDoGet
content = content.replace('''func TestDataServerDoGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	client, _ := setupDataServerTest(t)''', '''func TestDataServerDoGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	client, vs := setupDataServerTest(t)''')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
