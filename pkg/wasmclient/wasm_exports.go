//go:build js && wasm

package wasmclient

import (
	"context"
	"encoding/json"
	"syscall/js"
)

var globalWASMClient *EdgeClient

func wasmErrorJSON(msg string) string {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return string(b)
}

// RegisterJSGlobals binds Longbow Edge/WASM functions onto the browser/worker global object.
func RegisterJSGlobals() {
	js.Global().Set("longbowEdgeInit", js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) < 1 {
			return wasmErrorJSON("config JSON required")
		}
		var cfg ClientConfig
		if err := json.Unmarshal([]byte(args[0].String()), &cfg); err != nil {
			return wasmErrorJSON(err.Error())
		}
		c, err := NewEdgeClient(cfg)
		if err != nil {
			return wasmErrorJSON(err.Error())
		}
		globalWASMClient = c
		return `{"status":"ok"}`
	}))

	js.Global().Set("longbowEdgeUpsert", js.FuncOf(func(this js.Value, args []js.Value) any {
		if globalWASMClient == nil || len(args) < 1 {
			return wasmErrorJSON("not initialized")
		}
		var doc EdgeDocument
		if err := json.Unmarshal([]byte(args[0].String()), &doc); err != nil {
			return wasmErrorJSON(err.Error())
		}
		if err := globalWASMClient.Upsert(context.Background(), doc); err != nil {
			return wasmErrorJSON(err.Error())
		}
		return `{"status":"ok"}`
	}))

	js.Global().Set("longbowEdgeSearch", js.FuncOf(func(this js.Value, args []js.Value) any {
		if globalWASMClient == nil || len(args) < 2 {
			return wasmErrorJSON("not initialized")
		}
		var vec []float32
		if err := json.Unmarshal([]byte(args[0].String()), &vec); err != nil {
			return wasmErrorJSON(err.Error())
		}
		k := args[1].Int()
		res, err := globalWASMClient.Search(context.Background(), vec, k)
		if err != nil {
			return wasmErrorJSON(err.Error())
		}
		b, _ := json.Marshal(res)
		return string(b)
	}))
}
