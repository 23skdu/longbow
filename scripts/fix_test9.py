import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# We want to remove TestDataServerListFlightsUnimplemented and TestDataServerGetFlightInfoUnimplemented
# Let's find the start of TestDataServerListFlightsUnimplemented
idx1 = content.find('func TestDataServerListFlightsUnimplemented(t *testing.T) {')
idx2 = content.find('func TestDataServerGetFlightInfoUnimplemented(t *testing.T) {')
idx3 = content.find('func TestMetaServerDoPutUnimplemented(t *testing.T) {')

# The file originally had TestDataServerListFlightsUnimplemented, but because of my bad regex,
# I might have removed some comments but left the function body.
# Let's just find the end of TestDataServerDoGetNotFound and delete everything after it until TestMetaServerDoPutUnimplemented.
