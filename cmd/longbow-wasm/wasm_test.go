package main

import (
	"testing"

	"github.com/23skdu/longbow/pkg/wasmclient"
)

func TestWASMEntryHelper(t *testing.T) {
	c, err := wasmclient.NewEdgeClient(wasmclient.ClientConfig{Dataset: "edge", OfflineFirst: true})
	if err != nil {
		t.Fatal(err)
	}
	if c.LocalIndex().Len() != 0 {
		t.Fatalf("expected empty index")
	}
}

func FuzzWASMEntryConfig(f *testing.F) {
	f.Add("edge-ds", true)
	f.Fuzz(func(t *testing.T, dataset string, offline bool) {
		_, _ = wasmclient.NewEdgeClient(wasmclient.ClientConfig{
			Dataset:      dataset,
			OfflineFirst: offline,
		})
	})
}
