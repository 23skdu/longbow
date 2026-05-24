#!/bin/bash
cat << 'MOCK' >> internal/store/cluster/cdc_test.go

type MockCDCStore struct {
	cdcSubscribers map[string][]chan arrow.RecordBatch
}

func (m *MockCDCStore) RegisterCDCSubscriber(dataset string, ch chan arrow.RecordBatch) {
	if m.cdcSubscribers == nil {
		m.cdcSubscribers = make(map[string][]chan arrow.RecordBatch)
	}
	m.cdcSubscribers[dataset] = append(m.cdcSubscribers[dataset], ch)
}

func (m *MockCDCStore) UnregisterCDCSubscriber(dataset string, ch chan arrow.RecordBatch) {
	if m.cdcSubscribers == nil {
		return
	}
	subs := m.cdcSubscribers[dataset]
	for i, sub := range subs {
		if sub == ch {
			m.cdcSubscribers[dataset] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}
MOCK
