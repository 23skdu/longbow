import re

with open('internal/store/cluster/cdc_test.go', 'r') as f:
    content = f.read()

# Replace cdc.Unsubscribe(sub.ID) with sub.Close() ONLY in those functions
content = content.replace('''func TestCDCSubscription_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	sub := &CDCSubscription{
		ID:     "test-sub",
		Ch:     make(chan arrow.RecordBatch, 10),
		closed: false,
	}

	assert.False(t, sub.IsClosed())
	cdc.Unsubscribe(sub.ID)
	assert.True(t, sub.IsClosed())''', '''func TestCDCSubscription_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	sub := &CDCSubscription{
		ID:     "test-sub",
		Ch:     make(chan arrow.RecordBatch, 10),
		closed: false,
	}

	assert.False(t, sub.IsClosed())
	sub.Close()
	assert.True(t, sub.IsClosed())''')

content = content.replace('''func TestCDCSubscription_Close_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	sub := &CDCSubscription{
		ID:     "test-sub",
		Ch:     make(chan arrow.RecordBatch, 10),
		closed: false,
	}

	cdc.Unsubscribe(sub.ID)
	assert.True(t, sub.IsClosed())

	cdc.Unsubscribe(sub.ID)
	assert.True(t, sub.IsClosed())''', '''func TestCDCSubscription_Close_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	sub := &CDCSubscription{
		ID:     "test-sub",
		Ch:     make(chan arrow.RecordBatch, 10),
		closed: false,
	}

	sub.Close()
	assert.True(t, sub.IsClosed())

	sub.Close()
	assert.True(t, sub.IsClosed())''')

with open('internal/store/cluster/cdc_test.go', 'w') as f:
    f.write(content)
