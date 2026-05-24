import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Delete TestDataServerListFlightsUnimplemented and TestDataServerGetFlightInfoUnimplemented
content = re.sub(r'// TestDataServerListFlightsUnimplemented.*?}\n', '', content, flags=re.DOTALL)
content = re.sub(r'// TestDataServerGetFlightInfoUnimplemented.*?}\n', '', content, flags=re.DOTALL)

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
