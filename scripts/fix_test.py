import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# 1. Revert the broken sed change
content = content.replace('_, _ = stream.Recv(); time.Sleep(500 * time.Millisecond)', '_, _ = stream.Recv()')

# 2. Change setupDataServerTest signature
content = re.sub(
    r'func setupDataServerTest\(t \*testing\.T\) flight\.Client \{',
    'func setupDataServerTest(t *testing.T) (flight.Client, *store.VectorStore) {',
    content
)

# 3. Change return client to return client, vs
content = re.sub(
    r'return client\n\}',
    'return client, vs\n}',
    content
)

# 4. Update TestDataServerDoGet
content = re.sub(
    r'client := setupDataServerTest\(t\)',
    'client, vs := setupDataServerTest(t)',
    content
)

# 5. Update TestDataServerListFlightsUnimplemented and TestDataServerGetFlightInfoUnimplemented
content = re.sub(
    r'client := setupDataServerTest\(t\)',
    'client, _ := setupDataServerTest(t)',
    content
)

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
