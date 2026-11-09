package main

import "testing"

func FuzzRingOperations(f *testing.F) {
	f.Add(10, "node1", "key")
	f.Add(1, "", "")
	f.Fuzz(func(t *testing.T, vnodes int, node, key string) {
		if vnodes < 1 {
			vnodes = 1
		}
		if vnodes > 64 {
			vnodes = 64
		}
		r := NewRing(vnodes)
		if node != "" {
			r.AddNode(node)
		}
		_ = r.GetNode(key)
		_ = r.GetPreferenceList(key, 3)
		r.RemoveNode(node)
	})
}
