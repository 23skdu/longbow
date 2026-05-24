import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Just replace all 'client, vs :=' with 'client, _ :='
content = content.replace('client, vs := setupDataServerTest(t)', 'client, _ := setupDataServerTest(t)')

# Then restore only the one in TestDataServerDoGet
target = '''func TestDataServerDoGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	client, _ := setupDataServerTest(t)'''

replacement = '''func TestDataServerDoGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	client, vs := setupDataServerTest(t)'''

content = content.replace(target, replacement)

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
