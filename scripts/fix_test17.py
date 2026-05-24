import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Let's see what is around line 230
lines = content.split('\n')
for i, line in enumerate(lines[220:240]):
    print(f"{i+220}: {line}")
